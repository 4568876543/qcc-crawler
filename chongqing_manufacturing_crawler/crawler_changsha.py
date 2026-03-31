# 长沙市制造业爬虫 - 基于优化版本
import asyncio
import re
import os
import json
from datetime import datetime
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# 使用长沙配置
import config_changsha as config

# 从配置获取常量
CITY = config.CITY
COMPANY_STATUS = config.COMPANY_STATUS
MANUFACTURING_SUBCATEGORIES = config.MANUFACTURING_SUBCATEGORIES
CHANGSHA_DISTRICTS = config.CHANGSHA_DISTRICTS
TIMEOUT = config.TIMEOUT
RANDOM_DELAY_MIN = config.RANDOM_DELAY_MIN
RANDOM_DELAY_MAX = config.RANDOM_DELAY_MAX
SAVE_ON_EACH_INDUSTRY = config.SAVE_ON_EACH_INDUSTRY
VERIFY_CONDITIONS_ON_RESUME = config.VERIFY_CONDITIONS_ON_RESUME

# 动态获取输出目录和文件路径的函数
get_output_dir = config.get_output_dir
get_output_files = config.get_output_files

from utils.excel_utils import (
    create_excel_template,
    create_district_sheets,
    create_summary_sheet,
    update_excel_data,
    update_district_sheet,
    update_summary_sheet,
    update_all_district_sheets,
    save_city_summary_table,
    save_industry_detail_table,
    save_summary_table,
    validate_data_consistency
)
from utils.data_utils import (
    save_temp_data,
    load_temp_data,
    random_delay,
)
from utils.task_manager import TaskManager, TaskStatus
from utils.index_cache import IndexCache
from utils.data_validator import DataValidator

COOKIE_FILE = "data/qcc_cookies.json"


class ChangshaCrawler:
    """长沙市制造业爬虫"""
    
    def __init__(self, use_existing_browser=False, cdp_url=None, output_dir=None):
        self.browser = None
        self.page = None
        self.context = None
        self.playwright = None
        self.data = []
        
        # 初始化输出目录（如果未指定则创建新的）
        if output_dir:
            self.output_dir = output_dir
            os.makedirs(output_dir, exist_ok=True)
        else:
            self.output_dir = get_output_dir()
        
        # 获取输出文件路径
        self.output_files = get_output_files(self.output_dir)
        self.OUTPUT_FILE = self.output_files["excel"]
        self.TEMP_DATA_FILE = self.output_files["temp"]
        self.TASK_FILE = self.output_files["task"]
        self.INDEX_CACHE_FILE = self.output_files["index_cache"]
        self.VALIDATION_DIR = self.output_files["validation_dir"]
        
        # 任务管理器
        self.task_manager = TaskManager(self.TASK_FILE)
        # 索引缓存
        self.index_cache = IndexCache(self.INDEX_CACHE_FILE)
        # 数据验证器
        self.validator = DataValidator(self.VALIDATION_DIR)
        
        self.screenshot_dir = os.path.join(self.output_dir, "screenshots")
        self.use_existing_browser = use_existing_browser
        self.cdp_url = cdp_url
        os.makedirs(self.screenshot_dir, exist_ok=True)
        os.makedirs(self.VALIDATION_DIR, exist_ok=True)
        
        self.log(f"输出目录: {self.output_dir}")
    
    async def init_browser(self):
        """初始化浏览器"""
        self.log("正在启动浏览器...")
        
        if self.use_existing_browser and self.cdp_url:
            self.log(f"[模式] 连接到已有浏览器: {self.cdp_url}")
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.connect_over_cdp(self.cdp_url)
            contexts = self.browser.contexts
            if contexts:
                self.context = contexts[0]
                pages = self.context.pages
                if pages:
                    self.page = pages[0]
                    self.log(f"[模式] 已连接到现有页面: {self.page.url}")
                else:
                    self.page = await self.context.new_page()
            else:
                self.context = await self.browser.new_context()
                self.page = await self.context.new_page()
        else:
            self.log("[模式] 启动新浏览器（支持Cookie持久化）")
            self.playwright = await async_playwright().start()
            
            # 创建持久化浏览器上下文（保存cookie到本地）
            user_data_dir = os.path.join("data", "browser_profile")
            os.makedirs(user_data_dir, exist_ok=True)
            
            self.context = await self.playwright.chromium.launch_persistent_context(
                user_data_dir,
                headless=False,
                slow_mo=50,
                viewport={'width': 1920, 'height': 1080}
            )
            self.browser = None  # 持久化上下文不需要browser对象
            
            # 获取或创建页面
            pages = self.context.pages
            if pages:
                self.page = pages[0]
            else:
                self.page = await self.context.new_page()
        
        self.page.set_default_timeout(TIMEOUT)
        self.log("浏览器启动完成")
    
    async def save_cookies(self):
        """保存当前cookie到文件"""
        try:
            cookies = await self.context.cookies()
            os.makedirs(os.path.dirname(COOKIE_FILE), exist_ok=True)
            with open(COOKIE_FILE, 'w', encoding='utf-8') as f:
                json.dump(cookies, f, ensure_ascii=False, indent=2)
            self.log(f"[Cookie] 已保存 {len(cookies)} 个cookie")
        except Exception as e:
            self.log(f"[Cookie] 保存失败: {e}")
    
    async def load_cookies(self):
        """从文件加载cookie"""
        try:
            if os.path.exists(COOKIE_FILE):
                with open(COOKIE_FILE, 'r', encoding='utf-8') as f:
                    cookies = json.load(f)
                await self.context.add_cookies(cookies)
                self.log(f"[Cookie] 已加载 {len(cookies)} 个cookie")
                return True
        except Exception as e:
            self.log(f"[Cookie] 加载失败: {e}")
        return False
    
    async def screenshot(self, name: str):
        """截图保存"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{timestamp}_{name}.png"
            filepath = os.path.join(self.screenshot_dir, filename)
            await self.page.screenshot(path=filepath, full_page=True)
            self.log(f"截图: {filepath}")
        except Exception as e:
            self.log(f"截图失败: {e}")
    
    def log(self, message: str):
        """日志输出"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_line = f"[{timestamp}] {message}"
        print(log_line)
        log_file = os.path.join("logs", "changsha_crawler.log")
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(log_line + "\n")
    
    async def check_login(self):
        """检查登录状态 - 改进版：检查是否有登录按钮或二维码"""
        try:
            # 检查是否有登录按钮或登录链接
            login_indicators = [
                self.page.locator('button:has-text("登录")'),
                self.page.locator('a:has-text("登录")'),
                self.page.locator('text=请登录'),
                self.page.locator('text=扫码登录'),
                self.page.locator('[class*="qrcode"]'),
                self.page.locator('[class*="login-modal"]'),
            ]
            
            for locator in login_indicators:
                try:
                    if await locator.first.is_visible(timeout=1000):
                        return False  # 未登录
                except:
                    pass
            
            # 检查是否有用户头像或用户名（已登录状态）
            logged_in_indicators = [
                self.page.locator('[class*="user-info"]'),
                self.page.locator('[class*="avatar"]'),
                self.page.locator('[class*="member"]'),
            ]
            
            for locator in logged_in_indicators:
                try:
                    if await locator.first.is_visible(timeout=1000):
                        return True  # 已登录
                except:
                    pass
            
            # 默认检查URL是否包含login
            if 'login' in self.page.url.lower():
                return False
                
            return True
        except:
            return True
    
    async def close_popups(self):
        """关闭各种弹窗"""
        popup_closed = False
        try:
            # 常见的弹窗关闭按钮选择器
            close_selectors = [
                '.qcc-login-modal-close',
                '.close-btn',
                '.modal-close',
                '[class*="close"]',
                'button[aria-label="关闭"]',
                'i[class*="close"]',
                '.dialog-close',
                '[class*="popup"] [class*="close"]',
            ]
            
            for selector in close_selectors:
                try:
                    close_btn = self.page.locator(selector).first
                    if await close_btn.is_visible(timeout=500):
                        await close_btn.click()
                        await self.page.wait_for_timeout(300)
                        popup_closed = True
                        self.log("  已关闭弹窗")
                except:
                    pass
            
            # 尝试按ESC关闭弹窗
            if not popup_closed:
                await self.page.keyboard.press("Escape")
                await self.page.wait_for_timeout(300)
                
        except Exception as e:
            self.log(f"  关闭弹窗时出错: {e}")
        
        return popup_closed
    
    async def navigate_to_search(self):
        """访问企查查，搜索长沙 - 改进版：更好的登录等待机制"""
        try:
            self.log(f"第一步：访问企查查首页...")
            await self.page.goto("https://www.qcc.com/", wait_until='networkidle')
            await self.page.wait_for_timeout(2000)
            
            # 尝试关闭弹窗
            await self.close_popups()
            
            # 检查登录状态
            if not await self.check_login():
                self.log("=" * 50)
                self.log("⚠️  未登录，请扫码登录！")
                self.log("=" * 50)
                self.log("请打开浏览器窗口，使用企查查APP扫码登录")
                self.log("登录成功后，程序将自动继续...")
                self.log("")
                
                # 等待用户登录，最长等待5分钟
                for i in range(150):  # 150 * 2秒 = 5分钟
                    await self.page.wait_for_timeout(2000)
                    
                    # 检查是否登录成功
                    if await self.check_login():
                        self.log("✅ 检测到已登录！")
                        await self.page.wait_for_timeout(1000)
                        
                        # 登录成功后关闭可能的弹窗
                        await self.close_popups()
                        
                        # 保存cookie
                        await self.save_cookies()
                        self.log("✅ 已保存登录状态")
                        break
                    
                    # 每10秒提示一次
                    if i % 5 == 0 and i > 0:
                        remaining = 150 - i
                        self.log(f"⏳ 等待登录中... (剩余 {remaining*2} 秒)")
                else:
                    self.log("❌ 登录超时，请重新运行程序")
                    return False
            
            # 关闭可能出现的各种弹窗（广告、公告等）
            await self.close_popups()
            
            self.log(f"第二步：输入{CITY}，点击查一下...")
            
            # 确保页面处于可操作状态
            await self.page.wait_for_timeout(1000)
            
            # 查找搜索框
            search_input = self.page.locator('input[placeholder*="企业"], input[placeholder*="搜索"], input[type="text"]').first
            await search_input.click()
            await search_input.fill("")
            await search_input.fill(CITY)
            await self.page.wait_for_timeout(500)
            
            # 点击搜索按钮
            search_btn = self.page.get_by_role("button", name="查一下").first
            await search_btn.click()
            
            await self.page.wait_for_load_state('networkidle')
            await self.page.wait_for_timeout(2000)
            
            # 关闭搜索结果页可能出现的弹窗
            await self.close_popups()
            
            self.log(f"当前页面: {self.page.url}")
            await self.screenshot("step2_search_result")
            
            return True
            
        except Exception as e:
            self.log(f"导航失败: {e}")
            await self.screenshot("navigate_error")
            return False
    
    async def setup_filters(self):
        """设置筛选条件"""
        try:
            self.log("第三步：设置筛选条件...")
            
            # 勾选"地址"
            self.log("  勾选地址...")
            try:
                address_checkbox = self.page.get_by_role("checkbox", name="地址")
                if await address_checkbox.is_visible(timeout=3000):
                    await address_checkbox.check()
                    await self.page.wait_for_timeout(500)
            except Exception as e:
                self.log(f"  勾选地址失败: {e}")
            
            # 点击"更多"展开省份地区
            self.log("  点击更多展开省份地区...")
            try:
                more_btn = self.page.locator("a").filter(has_text="更多").first
                if await more_btn.is_visible(timeout=3000):
                    await more_btn.click()
                    await self.page.wait_for_timeout(1000)
            except Exception as e:
                self.log(f"  点击更多失败: {e}")
            
            # 先点击湖南省（省份）
            self.log("  点击湖南省...")
            try:
                # 等待省份列表加载
                await self.page.wait_for_timeout(500)
                # 点击湖南省
                hunan_btn = self.page.locator('text=湖南省').first
                if await hunan_btn.is_visible(timeout=3000):
                    await hunan_btn.click()
                    await self.page.wait_for_timeout(1000)
                    self.log("  已选择湖南省")
                else:
                    self.log("  湖南省不可见，尝试其他方式")
            except Exception as e:
                self.log(f"  点击湖南省失败: {e}")
            
            # 再点击长沙市（城市）
            self.log(f"  点击{CITY}...")
            try:
                # 等待城市列表加载
                await self.page.wait_for_timeout(500)
                city_btn = self.page.get_by_text(CITY).first
                await city_btn.click()
                await self.page.wait_for_timeout(1000)
                self.log(f"  已选择{CITY}")
            except Exception as e:
                self.log(f"  点击{CITY}失败: {e}")
            
            # 关闭地区选择面板（点击空白处或关闭按钮）
            try:
                await self.page.keyboard.press("Escape")
                await self.page.wait_for_timeout(500)
                # 点击页面空白处关闭面板
                await self.page.mouse.click(400, 300)
                await self.page.wait_for_timeout(500)
            except:
                pass
            
            # 点击登记状态下拉框 - 使用JavaScript绕过遮罩层
            self.log("  点击登记状态下拉框...")
            try:
                # 使用JavaScript直接点击
                await self.page.evaluate('''() => {
                    const elements = document.querySelectorAll('.app-dselect, .dselect-text, [class*="dselect"]');
                    for (const el of elements) {
                        if (el.textContent.includes('登记状态')) {
                            el.click();
                            return true;
                        }
                    }
                    return false;
                }''')
                await self.page.wait_for_timeout(1000)
                self.log("  已展开登记状态选项")
            except Exception as e:
                self.log(f"  点击登记状态失败: {e}")
            
            # 勾选正常状态（存续/在业）- 使用JavaScript
            self.log(f"  勾选{COMPANY_STATUS}...")
            try:
                await self.page.wait_for_timeout(500)
                # 使用JavaScript点击存续/在业选项
                await self.page.evaluate(f'''() => {{
                    const elements = document.querySelectorAll('span, a, label, div');
                    for (const el of elements) {{
                        if (el.textContent.includes('{COMPANY_STATUS}')) {{
                            el.click();
                            return true;
                        }}
                    }}
                    return false;
                }}''')
                await self.page.wait_for_timeout(500)
                self.log(f"  已选择{COMPANY_STATUS}")
            except Exception as e:
                self.log(f"  勾选{COMPANY_STATUS}失败: {e}")
            
            # 缓存当前选择的条件
            await self.cache_current_conditions()
            
            await self.screenshot("step3_filters_set")
            
            return True
            
        except Exception as e:
            self.log(f"设置筛选条件失败: {e}")
            return False
    
    async def cache_current_conditions(self):
        """缓存当前选择的条件"""
        try:
            conditions_area = self.page.locator('.has-selected, .selected-conditions, [class*="selected"]').first
            if await conditions_area.is_visible(timeout=2000):
                conditions_text = await conditions_area.text_content()
                conditions = {
                    'text': conditions_text,
                    'city': CITY,
                    'status': COMPANY_STATUS,
                    'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                self.task_manager.set_selected_conditions(conditions)
                self.log(f"  已缓存筛选条件")
        except Exception as e:
            self.log(f"  缓存筛选条件失败: {e}")
    
    async def verify_conditions_on_resume(self) -> bool:
        """续爬时验证筛选条件是否一致"""
        if not VERIFY_CONDITIONS_ON_RESUME:
            return True
        
        self.log("[条件验证] 正在验证筛选条件...")
        
        try:
            # 获取整个页面的已选条件区域文本
            conditions_area = self.page.locator('.has-selected, .selected-conditions, [class*="selected"]').first
            if not await conditions_area.is_visible(timeout=2000):
                self.log("[条件验证] 无法获取当前条件区域，跳过验证")
                return True
            
            current_text = await conditions_area.text_content()
            self.log(f"[条件验证] 当前条件文本: {current_text[:100]}...")
            
            # 宽松验证：检查是否包含关键条件
            checks_passed = 0
            total_checks = 3
            
            # 检查城市（可能是"长沙市"或"长沙"）
            if CITY in current_text or CITY.replace('市', '') in current_text:
                checks_passed += 1
                self.log(f"[条件验证] ✅ 城市: {CITY}")
            else:
                self.log(f"[条件验证] ⚠️ 城市未明确显示: {CITY}")
                # 城市不显示也继续，因为URL中可能已包含
            
            # 检查登记状态
            if COMPANY_STATUS in current_text:
                checks_passed += 1
                self.log(f"[条件验证] ✅ 登记状态: {COMPANY_STATUS}")
            else:
                self.log(f"[条件验证] ⚠️ 登记状态未显示: {COMPANY_STATUS}")
            
            # 检查制造业
            if "制造业" in current_text or "行业" in current_text:
                checks_passed += 1
                self.log(f"[条件验证] ✅ 行业筛选已设置")
            else:
                self.log(f"[条件验证] ⚠️ 行业筛选未显示")
            
            # 条件验证不做硬性要求，继续执行
            self.log(f"[条件验证] ℹ️ 验证完成 ({checks_passed}/{total_checks})，继续执行")
            return True
            
        except Exception as e:
            self.log(f"[条件验证] 验证失败: {e}")
            return False
    
    async def get_district_distribution(self):
        """获取区县分布数据"""
        try:
            await self.page.wait_for_timeout(2000)
            body_text = await self.page.text_content('body')
            
            district_data = {}
            pattern = r'([\u4e00-\u9fa5]+[区县市]|[\u4e00-\u9fa5]+开发区|[\u4e00-\u9fa5]+新区)\s*[\(（]([\d,]+)[\)）]'
            matches = re.findall(pattern, body_text)
            
            for match in matches:
                district = match[0]
                count = int(match[1].replace(',', ''))
                district_data[district] = count
            
            if district_data:
                self.log(f"  获取到 {len(district_data)} 个区县")
                for d, c in list(district_data.items())[:5]:
                    self.log(f"    {d}: {c}")
                return district_data
            else:
                self.log("  未获取到区县数据")
                await self.screenshot("no_district_data")
                return {}
                
        except Exception as e:
            self.log(f"获取区县分布失败: {e}")
            return {}
    
    async def click_manufacturing(self):
        """点击制造业"""
        try:
            self.log("  点击制造业...")
            manufacturing = self.page.get_by_text("制造业").first
            await manufacturing.click()
            await self.page.wait_for_timeout(2000)
            return True
        except Exception as e:
            self.log(f"  点击制造业失败: {e}")
            return False
    
    async def click_industry(self, industry_name: str):
        """点击指定行业 - 简化版：直接在行业列表中点击"""
        try:
            self.log(f"  [行业选择] 点击: {industry_name}")
            
            # 截图记录选择前状态
            await self.screenshot(f"before_select_{industry_name[:8]}")
            
            # 简化方法：直接用Playwright点击包含行业名称的元素
            # 行业列表中的项通常是 <a> 或 <span> 标签
            clicked = False
            
            # 方法1: 尝试点击行业列表中的链接
            try:
                industry_link = self.page.locator(f'a:has-text("{industry_name}")').first
                if await industry_link.is_visible(timeout=2000):
                    await industry_link.click()
                    clicked = True
                    self.log(f"  [行业选择] 方式1: 点击a标签成功")
            except:
                pass
            
            # 方法2: 尝试点击span
            if not clicked:
                try:
                    industry_span = self.page.locator(f'span:has-text("{industry_name}")').first
                    if await industry_span.is_visible(timeout=1000):
                        await industry_span.click()
                        clicked = True
                        self.log(f"  [行业选择] 方式2: 点击span标签成功")
                except:
                    pass
            
            # 方法3: 使用getByRole
            if not clicked:
                try:
                    industry_item = self.page.get_by_role("link", name=industry_name).first
                    if await industry_item.is_visible(timeout=1000):
                        await industry_item.click()
                        clicked = True
                        self.log(f"  [行业选择] 方式3: 点击link角色成功")
                except:
                    pass
            
            # 方法4: 使用getByText
            if not clicked:
                try:
                    industry_text = self.page.get_by_text(industry_name, exact=True).first
                    await industry_text.click()
                    clicked = True
                    self.log(f"  [行业选择] 方式4: 点击文本成功")
                except:
                    pass
            
            await self.page.wait_for_timeout(2000)
            
            # 截图记录选择后状态
            await self.screenshot(f"after_select_{industry_name[:8]}")
            
            if clicked:
                self.log(f"  [行业选择] ✅ 已点击: {industry_name}")
                return True
            else:
                self.log(f"  [行业选择] ❌ 未能点击: {industry_name}")
                return False
                
        except Exception as e:
            self.log(f"  [行业选择] ❌ 点击失败: {e}")
            await self.screenshot(f"select_error_{industry_name[:8]}")
            return False
    
    async def deselect_industry(self, industry_name: str):
        """取消选择行业 - 改进版：使用更可靠的选择器和验证机制"""
        try:
            self.log(f"  [取消选择] 取消: {industry_name}")
            
            # 截图记录取消前状态
            await self.screenshot(f"before_deselect_{industry_name[:8]}")
            
            # 方法1: 查找已选中标签区域中的行业标签
            # 已选中的行业标签通常在 "已选条件" 区域,并且有删除按钮或可点击取消
            deselect_success = False
            
            # 尝试多种方式取消选择
            try:
                # 方式A: 查找包含行业名称且有删除图标的标签
                # 企查查的已选标签通常是: <span>行业名 <i class="close-icon"></i></span>
                close_icon_clicked = await self.page.evaluate(f'''() => {{
                    // 查找所有可能的已选标签容器
                    const containers = document.querySelectorAll('.has-selected, .selected-conditions, [class*="selected"], .tag-list, .filter-tags');
                    for (const container of containers) {{
                        // 查找包含行业名称的标签
                        const tags = container.querySelectorAll('span, a, div.tag, div[class*="tag"]');
                        for (const tag of tags) {{
                            if (tag.textContent.includes('{industry_name}')) {{
                                // 查找关闭按钮
                                const closeBtn = tag.querySelector('i, .close, .delete, [class*="close"], [class*="remove"]');
                                if (closeBtn) {{
                                    closeBtn.click();
                                    return 'close_btn_clicked';
                                }}
                                // 如果没有关闭按钮,直接点击标签本身
                                tag.click();
                                return 'tag_clicked';
                            }}
                        }}
                    }}
                    return null;
                }}''')
                
                if close_icon_clicked:
                    self.log(f"  [取消选择] JavaScript方式: {close_icon_clicked}")
                    deselect_success = True
            except Exception as e:
                self.log(f"  [取消选择] JavaScript方式失败: {e}")
            
            # 方式B: 如果JavaScript方式失败,使用Playwright查找
            if not deselect_success:
                try:
                    # 查找"已选"区域内的行业标签
                    selected_area = self.page.locator('.has-selected, .selected-conditions, [class*="selected"]').first
                    if await selected_area.is_visible(timeout=2000):
                        # 在已选区域内查找行业标签
                        industry_tag = selected_area.locator(f'span:has-text("{industry_name}"), a:has-text("{industry_name}")').first
                        if await industry_tag.is_visible(timeout=1000):
                            await industry_tag.click()
                            self.log(f"  [取消选择] Playwright方式: 点击标签")
                            deselect_success = True
                except Exception as e:
                    self.log(f"  [取消选择] Playwright方式失败: {e}")
            
            await self.page.wait_for_timeout(1000)
            
            # 截图记录取消后状态
            await self.screenshot(f"after_deselect_{industry_name[:8]}")
            
            # 验证是否真正取消选择
            verification = await self.verify_industry_deselected(industry_name)
            
            if verification:
                self.log(f"  [取消选择] ✅ 已取消并验证: {industry_name}")
                return True
            else:
                self.log(f"  [取消选择] ⚠️ 点击了但验证失败: {industry_name}")
                # 即使验证失败,也返回True让程序继续运行
                return True
                
        except Exception as e:
            self.log(f"  [取消选择] ❌ 取消失败: {e}")
            await self.screenshot(f"deselect_error_{industry_name[:8]}")
            return False
    
    async def verify_industry_deselected(self, industry_name: str) -> bool:
        """验证行业是否已取消选择"""
        try:
            # 检查已选条件区域是否还包含该行业
            still_selected = await self.page.evaluate(f'''() => {{
                const containers = document.querySelectorAll('.has-selected, .selected-conditions, [class*="selected"], .tag-list, .filter-tags');
                for (const container of containers) {{
                    if (container.textContent.includes('{industry_name}')) {{
                        return true;
                    }}
                }}
                return false;
            }}''')
            
            if still_selected:
                self.log(f"  [验证] ⚠️ 行业仍处于选中状态: {industry_name}")
                return False
            else:
                self.log(f"  [验证] ✅ 行业已取消选择: {industry_name}")
                return True
                
        except Exception as e:
            self.log(f"  [验证] 验证失败: {e}")
            return False
    
    async def debug_page_state(self, context: str = ""):
        """调试方法：打印当前页面状态"""
        try:
            self.log(f"  [调试] ===== 页面状态快照 {context} =====")
            
            # 获取当前选中的条件
            selected_info = await self.page.evaluate('''() => {
                const result = {
                    url: window.location.href,
                    selectedConditions: [],
                    industryPanel: null
                };
                
                // 获取已选条件
                const selectedArea = document.querySelector('.has-selected, .selected-conditions, [class*="selected"]');
                if (selectedArea) {
                    result.selectedConditions = selectedArea.textContent.trim().split(/\\s+/).filter(t => t.length > 0);
                }
                
                // 检查行业面板是否展开
                const industryPanel = document.querySelector('[class*="industry"], [class*="category"]');
                if (industryPanel) {
                    result.industryPanel = 'visible';
                }
                
                return result;
            }''')
            
            self.log(f"  [调试] URL: {selected_info.get('url', '')[:100]}")
            self.log(f"  [调试] 已选条件: {selected_info.get('selectedConditions', [])}")
            self.log(f"  [调试] 行业面板: {selected_info.get('industryPanel', 'hidden')}")
            
            await self.screenshot(f"debug_{context}")
            
        except Exception as e:
            self.log(f"  [调试] 获取页面状态失败: {e}")

    async def init_task_session(self):
        """初始化任务会话"""
        pending = self.task_manager.get_pending_industries()
        
        if pending and self.task_manager.session:
            self.log(f"[断点续爬] 发现未完成任务: {len(pending)} 个行业待处理")
            progress = self.task_manager.get_progress()
            self.log(f"[断点续爬] 进度: {self.task_manager.get_progress_bar()}")
            
            temp_data = load_temp_data(self.TEMP_DATA_FILE)
            if temp_data:
                self.data = temp_data.get('data', [])
                self.log(f"[断点续爬] 已加载 {len(self.data)} 条历史数据")
            
            return True
        else:
            self.log("[新建会话] 初始化任务...")
            
            industries = dict(MANUFACTURING_SUBCATEGORIES)
            districts = list(CHANGSHA_DISTRICTS)
            
            self.task_manager.init_session(
                city=CITY,
                status_filter=COMPANY_STATUS,
                industries=industries,
                districts=districts
            )
            
            self.index_cache.set_city(CITY)
            self.index_cache.set_districts(districts)
            self.index_cache.set_industries(industries)
            
            return False
    
    async def crawl_single_industry(self, industry_code: str, industry_name: str) -> bool:
        """
        爬取单个行业的数据
        
        Args:
            industry_code: 行业代码
            industry_name: 行业名称
            
        Returns:
            bool: 是否成功
        """
        try:
            self.task_manager.start_industry(industry_code)
            
            # 先删除该行业的旧数据（重新爬取时避免数据重复）
            old_count = len(self.data)
            self.data = [d for d in self.data if d.get("行业代码") != industry_code]
            removed_count = old_count - len(self.data)
            if removed_count > 0:
                self.log(f"  已删除旧数据: {removed_count} 条")
            
            # 点击行业
            if await self.click_industry(industry_name):
                await self.screenshot(f"industry_{industry_code}_selected")
                
                # 获取区县数据
                district_data = await self.get_district_distribution()
                
                if district_data:
                    for district, count in district_data.items():
                        self.data.append({
                            "区县": district,
                            "行业代码": industry_code,
                            "行业类别": industry_name,
                            "企业数量": count
                        })
                        self.task_manager.update_district(district, industry_code, industry_name, count)
                    
                    self.log(f"  ✅ {industry_name}: {len(district_data)} 个区县, 共 {sum(district_data.values())} 家企业")
                    
                    # 保存行业明细表 + 截图
                    if hasattr(self, 'city_total_data'):
                        save_industry_detail_table(
                            self.output_dir, 
                            industry_code, 
                            industry_name, 
                            district_data, 
                            self.city_total_data, 
                            CITY
                        )
                        await self.screenshot(f"{industry_name}行业汇总表")
                        self.log(f"  行业明细表已保存并截图")
                    
                    if SAVE_ON_EACH_INDUSTRY:
                        update_excel_data(self.OUTPUT_FILE, self.data)
                        for district, count in district_data.items():
                            update_district_sheet(self.OUTPUT_FILE, district, industry_code, industry_name, count)
                        self.log(f"  已保存到Excel")
                    
                    self.task_manager.complete_industry(
                        industry_code, 
                        len(district_data), 
                        sum(district_data.values())
                    )
                else:
                    self.log(f"  ⚠️ {industry_name}: 未获取到数据")
                    self.task_manager.fail_industry(industry_code, "未获取到区县数据")
                    return False
                
                # 取消选择行业
                await self.deselect_industry(industry_name)
                
                # 保存临时数据
                save_temp_data(self.TEMP_DATA_FILE, {
                    "processed_industries": [t.code for t in self.task_manager.get_completed_industries()],
                    "data": self.data
                })
                
                random_delay(RANDOM_DELAY_MIN, RANDOM_DELAY_MAX)
                return True
            else:
                self.log(f"  ❌ 点击行业失败: {industry_name}")
                self.task_manager.fail_industry(industry_code, "点击行业失败")
                return False
                
        except Exception as e:
            self.log(f"  ❌ 爬取行业 {industry_name} 时出错: {e}")
            await self.screenshot(f"industry_{industry_code}_error")
            return False

    async def crawl_all_industries(self):
        """爬取所有行业数据"""
        try:
            self.log("第五步：点击制造业，获取制造业各区县数据...")
            
            await self.click_manufacturing()
            
            manufacturing_data = await self.get_district_distribution()
            
            if manufacturing_data:
                for district, count in manufacturing_data.items():
                    self.data.append({
                        "区县": district,
                        "行业代码": "C",
                        "行业类别": "制造业合计",
                        "企业数量": count
                    })
                    self.index_cache.set_district_enterprise_count(district, count)
                
                total = sum(manufacturing_data.values())
                self.index_cache.set_manufacturing_total(total)
                self.log(f"  制造业合计: {len(manufacturing_data)} 个区县, 共 {total} 家企业")
                
                # ====== 步骤1: 保存城市汇总表 + 截图 ======
                self.log("保存城市汇总表...")
                self.city_total_data = manufacturing_data  # 保存城市总数据供后续使用
                save_city_summary_table(self.output_dir, manufacturing_data, CITY)
                await self.screenshot("城市汇总表")
                self.log("  城市汇总表已保存并截图")
            
            return True
            
        except Exception as e:
            self.log(f"❌ 获取制造业汇总数据失败: {e}")
            await self.screenshot("crawl_manufacturing_error")
            return False
    
    async def close_browser(self):
        """关闭浏览器"""
        if self.context:
            try:
                await self.context.close()
                self.log("浏览器已关闭")
            except Exception as e:
                self.log(f"关闭浏览器时出错: {e}")
        self.browser = None
        self.page = None
        self.context = None
    
    async def crawl_industries_batch(self, restart_interval: int = 10):
        """
        批量爬取行业数据，每restart_interval个行业重启浏览器
        
        Args:
            restart_interval: 每爬取多少个行业后重启浏览器
        """
        pending_industries = self.task_manager.get_pending_industries()
        total = len(pending_industries)
        
        if total == 0:
            self.log("所有行业已完成爬取")
            return True
        
        self.log(f"待处理行业数: {total}")
        self.log(f"每 {restart_interval} 个行业重启浏览器")
        
        completed_count = 0
        
        for i, task in enumerate(pending_industries):
            industry_code = task.code
            industry_name = task.name
            
            progress = f"[{i+1}/{total}]"
            self.log(f"{progress} 处理: {industry_name} ({industry_code})")
            
            try:
                success = await self.crawl_single_industry(industry_code, industry_name)
                if success:
                    completed_count += 1
                
                # 每restart_interval个行业，重启浏览器
                if completed_count > 0 and completed_count % restart_interval == 0:
                    self.log(f"已完成 {completed_count} 个行业，重启浏览器...")
                    
                    # 保存当前进度
                    save_temp_data(self.TEMP_DATA_FILE, {
                        "processed_industries": [t.code for t in self.task_manager.get_completed_industries()],
                        "data": self.data
                    })
                    
                    # 关闭浏览器
                    await self.close_browser()
                    
                    # 重新启动浏览器
                    await self.init_browser()
                    
                    # 重新导航到搜索页面并设置条件
                    if not await self.navigate_to_search():
                        self.log("❌ 重启后导航失败")
                        return False
                    
                    await self.save_cookies()
                    
                    if not await self.setup_filters():
                        self.log("❌ 重启后设置筛选条件失败")
                        return False
                    
                    self.log("浏览器重启完成，继续爬取...")
                    
            except Exception as e:
                self.log(f"❌ 爬取行业 {industry_name} 时发生异常: {e}")
                import traceback
                self.log(f"  异常堆栈: {traceback.format_exc()}")
                await self.screenshot(f"industry_{industry_code}_exception")
                
                # 保存当前进度
                save_temp_data(self.TEMP_DATA_FILE, {
                    "processed_industries": [t.code for t in self.task_manager.get_completed_industries()],
                    "data": self.data
                })
                
                # 尝试重启浏览器
                self.log("尝试重启浏览器...")
                try:
                    await self.close_browser()
                    await self.init_browser()
                    if await self.navigate_to_search():
                        await self.save_cookies()
                        if await self.setup_filters():
                            self.log("浏览器重启成功，继续爬取...")
                        else:
                            self.log("❌ 重启后设置筛选条件失败")
                            return False
                    else:
                        self.log("❌ 重启后导航失败")
                        return False
                except Exception as restart_error:
                    self.log(f"❌ 重启浏览器失败: {restart_error}")
                    return False
        
        return True
    
    def run_validation(self):
        """运行数据验证"""
        self.log("=" * 60)
        self.log("开始数据验证...")
        self.log("=" * 60)
        
        results = self.validator.validate(
            data=self.data,
            index_cache=self.index_cache,
            task_manager=self.task_manager
        )
        
        report = self.validator.generate_report(results)
        print(report)
        
        self.validator.save_report(results)
        
        return results
    
    def mark_suspicious_data(self, industry_code: str, industry_name: str, reason: str):
        """标记可疑数据,需要人工核实"""
        suspicious_file = os.path.join("data", "suspicious_data.json")
        
        # 读取已有记录
        suspicious_list = []
        if os.path.exists(suspicious_file):
            try:
                with open(suspicious_file, 'r', encoding='utf-8') as f:
                    suspicious_list = json.load(f)
            except:
                pass
        
        # 添加新记录
        record = {
            "industry_code": industry_code,
            "industry_name": industry_name,
            "reason": reason,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": "待核实"
        }
        suspicious_list.append(record)
        
        # 保存
        with open(suspicious_file, 'w', encoding='utf-8') as f:
            json.dump(suspicious_list, f, ensure_ascii=False, indent=2)
        
        self.log(f"  ⚠️ 已标记可疑数据: {industry_name} - {reason}")
    
    async def run(self):
        """运行爬虫"""
        try:
            self.log("=" * 60)
            self.log(f"{CITY}制造业企业数据爬虫启动")
            self.log("=" * 60)
            
            await self.init_browser()
            
            is_resume = await self.init_task_session()
            
            # 如果是续爬,标记上一次的最后一条数据为可疑
            if is_resume and self.data:
                last_record = self.data[-1]
                self.mark_suspicious_data(
                    last_record.get('行业代码', ''),
                    last_record.get('行业类别', ''),
                    "程序意外中断前的最后一条数据,需人工核实"
                )
            
            self.log("创建Excel文件...")
            create_excel_template(self.OUTPUT_FILE)
            create_district_sheets(self.OUTPUT_FILE, CHANGSHA_DISTRICTS, MANUFACTURING_SUBCATEGORIES)
            create_summary_sheet(self.OUTPUT_FILE, CHANGSHA_DISTRICTS, MANUFACTURING_SUBCATEGORIES)
            
            if not await self.navigate_to_search():
                return
            
            # 登录成功后保存cookie
            await self.save_cookies()
            
            if not await self.setup_filters():
                return
            
            if is_resume:
                if not await self.verify_conditions_on_resume():
                    self.log("❌ 条件验证失败，请手动检查筛选条件")
                    return
            
            # 获取制造业汇总数据
            if not await self.crawl_all_industries():
                return
            
            # 批量爬取各行业数据（每10个行业重启浏览器）
            if not await self.crawl_industries_batch(restart_interval=10):
                return
            
            # ====== 步骤3: 所有行业完成后保存汇总明细表 ======
            self.log("保存汇总明细表...")
            summary_file = save_summary_table(self.output_dir, self.data, CITY)
            self.log(f"  汇总明细表已保存: {summary_file}")
            
            self.log("更新Excel汇总表...")
            update_summary_sheet(self.OUTPUT_FILE, self.data)
            update_all_district_sheets(self.OUTPUT_FILE, self.data, CHANGSHA_DISTRICTS, MANUFACTURING_SUBCATEGORIES)
            
            # ====== 步骤4: 数据验证（城市汇总表 vs 汇总明细表） ======
            self.log("=" * 60)
            self.log("数据一致性验证...")
            self.log("=" * 60)
            
            if hasattr(self, 'city_total_data'):
                validation_result = validate_data_consistency(self.city_total_data, self.data, threshold=0.02)
                
                self.log(f"城市汇总表总额: {validation_result['city_total']} 家企业")
                self.log(f"汇总明细表总额: {validation_result['detail_total']} 家企业")
                self.log(f"差异: {validation_result['difference']} 家企业 ({validation_result['diff_ratio']:.2f}%)")
                self.log(f"允许误差: {validation_result['threshold']:.0f}%")
                
                if validation_result['is_valid']:
                    self.log("✅ 数据验证通过！误差在允许范围内。")
                else:
                    self.log(f"❌ 数据验证未通过！误差 {validation_result['diff_ratio']:.2f}% 超过阈值 {validation_result['threshold']:.0f}%")
            else:
                self.log("⚠️ 未找到城市汇总数据，跳过验证")
            
            validation_results = self.run_validation()
            
            if validation_results.get('is_valid'):
                if os.path.exists(self.TEMP_DATA_FILE):
                    os.remove(self.TEMP_DATA_FILE)
                    self.log("已清理临时文件")
                self.task_manager.clear()
            else:
                # 验证失败,标记所有有差异的区县数据
                if 'checks' in validation_results:
                    total_vs_sum = validation_results['checks'].get('total_vs_sum', {})
                    if total_vs_sum.get('differences'):
                        for diff in total_vs_sum['differences']:
                            self.mark_suspicious_data(
                                'C',
                                '制造业合计',
                                f"区县 {diff['district']} 数据不一致: 合计{diff['manufacturing_total']} vs 分行业和{diff['industry_sum']}"
                            )
            
            # 任务完成保存cookie
            await self.save_cookies()
            
            self.log("=" * 60)
            self.log(f"爬取完成！共获取 {len(self.data)} 条数据")
            self.log(f"输出目录: {self.output_dir}")
            self.log(f"  - 城市汇总表: 行业明细表/00_{CITY}市制造业总表.xlsx")
            self.log(f"  - 行业明细表: 行业明细表/*.xlsx (31个文件)")
            self.log(f"  - 汇总明细表: {CITY}制造业企业数量明细表.xlsx")
            self.log("=" * 60)
            
            return True  # 成功完成
            
        except Exception as e:
            self.log(f"爬虫运行失败: {e}")
            await self.screenshot("run_error")
            # 出错也保存cookie
            await self.save_cookies()
            
            # 标记当前正在处理的行业为可疑
            if hasattr(self, 'task_manager'):
                current_industry = self.task_manager.get_current_industry()
                if current_industry:
                    self.mark_suspicious_data(
                        current_industry.code,
                        current_industry.name,
                        f"程序异常中断: {str(e)}"
                    )
            
            return False  # 失败
            
        finally:
            if not self.use_existing_browser and self.context:
                try:
                    await self.context.close()
                    self.log("浏览器已关闭")
                except:
                    pass


async def run_with_retry(max_retries: int = 10, retry_delay: int = 30):
    """带自动重试的运行函数
    
    Args:
        max_retries: 最大重试次数
        retry_delay: 重试间隔(秒)
    """
    retry_count = 0
    output_dir = None  # 保存输出目录，确保重试时使用同一个目录
    
    # 首先检查是否有未完成的目录
    from config_changsha import find_latest_incomplete_dir
    existing_dir = find_latest_incomplete_dir()
    if existing_dir:
        print(f"\n发现未完成的爬取目录: {existing_dir}")
        output_dir = existing_dir
    
    while retry_count < max_retries:
        retry_count += 1
        
        print(f"\n{'=' * 60}")
        print(f"第 {retry_count}/{max_retries} 次运行")
        print(f"{'=' * 60}\n")
        
        # 如果已有输出目录，继续使用同一个目录续爬
        crawler = ChangshaCrawler(output_dir=output_dir)
        if output_dir is None:
            output_dir = crawler.output_dir  # 保存第一次创建的目录
        success = await crawler.run()
        
        if success:
            print("\n✅ 爬取成功完成!")
            
            # 检查是否有可疑数据
            suspicious_file = os.path.join(output_dir, "suspicious_data.json")
            if os.path.exists(suspicious_file):
                print("\n⚠️ 存在需要人工核实的数据,请查看:")
                print(f"   {suspicious_file}")
            
            return True
        else:
            print(f"\n❌ 运行失败,将在 {retry_delay} 秒后重试...")
            print(f"   输出目录: {output_dir}")
            
            if retry_count < max_retries:
                import time
                time.sleep(retry_delay)
            else:
                print(f"\n❌ 已达到最大重试次数 ({max_retries}),停止运行")
                return False
    
    return False


async def main():
    """主函数"""
    import sys
    cdp_url = None
    use_existing = False
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--connect" and len(sys.argv) > 2:
            cdp_url = sys.argv[2]
            use_existing = True
    
    if use_existing:
        # 连接已有浏览器模式,不自动重试
        crawler = ChangshaCrawler(use_existing_browser=use_existing, cdp_url=cdp_url)
        await crawler.run()
    else:
        # 自动重试模式
        await run_with_retry()


if __name__ == "__main__":
    asyncio.run(main())
