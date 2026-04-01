# 长沙市制造业爬虫 - 基于优化版本
import asyncio
import re
import os
import json
from datetime import datetime
from dataclasses import asdict
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# 使用长沙配置
import config_changsha as config

# 从配置获取常量
SEARCH_LOCATION = config.SEARCH_LOCATION
DISTRICT_LEVEL = config.DISTRICT_LEVEL
COMPANY_STATUS = config.COMPANY_STATUS
MANUFACTURING_SUBCATEGORIES = config.MANUFACTURING_SUBCATEGORIES
DISTRICTS = config.DISTRICTS
TIMEOUT = config.TIMEOUT
RANDOM_DELAY_MIN = config.RANDOM_DELAY_MIN
RANDOM_DELAY_MAX = config.RANDOM_DELAY_MAX
SAVE_ON_EACH_INDUSTRY = config.SAVE_ON_EACH_INDUSTRY
VERIFY_CONDITIONS_ON_RESUME = config.VERIFY_CONDITIONS_ON_RESUME

# 动态获取输出目录和文件路径的函数
get_output_dir = config.get_output_dir
get_output_files = config.get_output_files
get_user_input = config.get_user_input

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
    validate_data_consistency,
    update_execution_progress
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
    """企查查行业搜索爬虫"""

    def __init__(self, use_existing_browser=False, cdp_url=None, output_dir=None, user_config=None):
        self.browser = None
        self.page = None
        self.context = None
        self.playwright = None
        self.data = []

        # 用户配置（如果未提供则使用默认配置）
        if user_config is None:
            self.user_config = {
                "keyword": config.CRAWL_KEYWORD,
                "search_location": config.SEARCH_LOCATION,
                "district_level": config.DISTRICT_LEVEL,
                "company_status": config.COMPANY_STATUS,
                "industry": getattr(config, 'INDUSTRY', '制造业')  # 国标行业
            }
        else:
            self.user_config = user_config
            # 更新全局配置
            config.CRAWL_KEYWORD = user_config["keyword"]
            config.SEARCH_LOCATION = user_config["search_location"]
            config.DISTRICT_LEVEL = user_config["district_level"]
            config.COMPANY_STATUS = user_config["company_status"]
            if "industry" in user_config:
                config.INDUSTRY = user_config["industry"]

        # 获取配置
        self.search_location = self.user_config["search_location"]  # 搜索地区（如"湖南省"或"长沙市"）
        self.district_level = self.user_config["district_level"]    # "province" 或 "city"
        self.keyword = self.user_config["keyword"]
        self.company_status = self.user_config["company_status"]
        self.industry = self.user_config.get("industry", "制造业")  # 国标行业

        # 初始化下级地区列表（从页面动态获取）
        self.districts = []
        
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

        # 浏览器重启次数统计
        self.browser_restart_count = 0

        # 进度回调函数（用于CLI显示）
        self.progress_callback = None

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

    def get_current_filters(self):
        """获取当前筛选条件"""
        filters = {
            '地区': self.search_location,
            '登记状态': self.company_status,
            '行业': '制造业',
        }
        return filters

    async def verify_filters_are_set(self) -> bool:
        """
        验证筛选条件是否已正确设置
        通过检查筛选标签是否显示来验证，而不是检查页面正文文本

        Returns:
            bool: 验证是否通过
        """
        try:
            await self.page.wait_for_timeout(1000)

            # 检查是否有已选中的筛选条件标签
            # 这些标签通常包含筛选信息
            filter_elements = await self.page.query_selector_all('.selected-tag, .filter-tag, [class*="selected"], [class*="tag"]')

            selected_texts = []
            for elem in filter_elements:
                try:
                    text = await elem.text_content()
                    if text:
                        selected_texts.append(text.strip())
                except:
                    pass

            selected_all = ' '.join(selected_texts)

            # 检查地区筛选标签
            location_ok = (self.search_location in selected_all or
                         self.search_location.replace('市', '') in selected_all)

            # 检查登记状态筛选标签
            status_ok = self.company_status in selected_all

            # 检查行业筛选标签
            industry_ok = ("制造业" in selected_all or
                          self.industry in selected_all)

            # 统计验证通过数
            passed = sum([location_ok, status_ok, industry_ok])
            self.log(f"[条件验证] 已选标签: {selected_texts[:5]}...")

            # 只要地区和行业有一个通过就继续（因为登记状态可能不显示在标签里）
            if location_ok and industry_ok:
                self.log(f"[条件验证] ✅ 筛选条件已设置 (地区:{location_ok}, 行业:{industry_ok})")
                return True

            # 如果标签检查失败，检查是否有搜索结果（有结果说明筛选生效）
            try:
                # 等待搜索结果加载
                await self.page.wait_for_timeout(2000)
                result_count = await self.page.locator('.search-result, .result-list, [class*="result"]').count()
                if result_count > 0:
                    self.log(f"[条件验证] ✅ 检测到搜索结果，筛选已生效")
                    return True
            except:
                pass

            # 最后尝试：检查URL参数是否包含筛选信息
            url = self.page.url
            if 'industry' in url or 'status' in url or 'province' in url:
                self.log(f"[条件验证] ✅ URL包含筛选参数，筛选已生效")
                return True

            self.log(f"[条件验证] ⚠️ 筛选标签未明确显示，但继续执行")
            return True

        except Exception as e:
            self.log(f"⚠️ [验证异常] 筛选条件验证出错: {e}")
            return True  # 出错时也继续，避免阻断流程

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
            
            self.log(f"第二步：输入{self.search_location}，点击查一下...")
            
            # 确保页面处于可操作状态
            await self.page.wait_for_timeout(1000)
            
            # 查找搜索框
            search_input = self.page.locator('input[placeholder*="企业"], input[placeholder*="搜索"], input[type="text"]').first
            await search_input.click()
            await search_input.fill("")
            await search_input.fill(self.search_location)
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

            # 使用改进的方法选择地区
            success = await self.select_location()

            if not success:
                self.log("  ⚠️ 省份/城市选择可能失败，继续执行...")

            # 关闭地区选择面板
            try:
                # 优先尝试点击关闭按钮
                close_btn = self.page.locator('.close-btn, .icon-close, [class*="close"], [class*="cancel"]').first
                if await close_btn.is_visible(timeout=1000):
                    await close_btn.click()
                    await self.page.wait_for_timeout(500)
                    self.log("  已关闭地区选择面板")
                else:
                    # 否则按Escape关闭
                    await self.page.keyboard.press("Escape")
                    await self.page.wait_for_timeout(500)
            except:
                # 最后手段：点击页面上方空白区域
                try:
                    await self.page.mouse.click(50, 50)
                    await self.page.wait_for_timeout(300)
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
            self.log(f"  勾选{self.company_status}...")
            try:
                await self.page.wait_for_timeout(500)
                # 使用JavaScript点击存续/在业选项
                await self.page.evaluate(f'''() => {{
                    const elements = document.querySelectorAll('span, a, label, div');
                    for (const el of elements) {{
                        if (el.textContent.includes('{self.company_status}')) {{
                            el.click();
                            return true;
                        }}
                    }}
                    return false;
                }}''')
                await self.page.wait_for_timeout(500)
                self.log(f"  已选择{self.company_status}")
            except Exception as e:
                self.log(f"  勾选{self.company_status}失败: {e}")

            # 点击"国标行业"筛选，勾选制造业
            self.log("  点击国标行业筛选...")
            try:
                await self.page.wait_for_timeout(500)
                # 使用JavaScript点击国标行业
                await self.page.evaluate('''() => {{
                    const elements = document.querySelectorAll('.app-dselect, .dselect-text, [class*="dselect"]');
                    for (const el of elements) {{
                        if (el.textContent.includes('国标行业')) {{
                            el.click();
                            return true;
                        }}
                    }}
                    return false;
                }}''')
                await self.page.wait_for_timeout(1000)
                self.log("  已展开国标行业选项")
            except Exception as e:
                self.log(f"  点击国标行业失败: {e}")

            # 勾选制造业
            industry_keyword = getattr(self, 'industry', '制造业')
            self.log(f"  勾选{industry_keyword}...")
            try:
                await self.page.wait_for_timeout(500)
                # 使用JavaScript点击制造业选项
                await self.page.evaluate(f'''() => {{
                    const elements = document.querySelectorAll('span, a, label, div');
                    for (const el of elements) {{
                        if (el.textContent.includes('{industry_keyword}')) {{
                            el.click();
                            return true;
                        }}
                    }}
                    return false;
                }}''')
                await self.page.wait_for_timeout(500)
                self.log(f"  已选择{industry_keyword}")
            except Exception as e:
                self.log(f"  勾选{industry_keyword}失败: {e}")

            # 缓存当前选择的条件
            await self.cache_current_conditions()
            
            await self.screenshot("step3_filters_set")
            
            return True

        except Exception as e:
            self.log(f"设置筛选条件失败: {e}")
            return False

    # 城市到省份的映射表（直辖市和特别行政区不需要上级省份）
    CITY_TO_PROVINCE = {
        # 湖南省
        "长沙市": "湖南省", "株洲市": "湖南省", "湘潭市": "湖南省",
        "衡阳市": "湖南省", "邵阳市": "湖南省", "岳阳市": "湖南省",
        "常德市": "湖南省", "张家界市": "湖南省", "益阳市": "湖南省",
        "郴州市": "湖南省", "永州市": "湖南省", "怀化市": "湖南省",
        "娄底市": "湖南省", "湘西土家族苗族自治州": "湖南省",
        # 广东省
        "广州市": "广东省", "韶关市": "广东省", "深圳市": "广东省",
        "珠海市": "广东省", "汕头市": "广东省", "佛山市": "广东省",
        "江门市": "广东省", "湛江市": "广东省", "茂名市": "广东省",
        "肇庆市": "广东省", "惠州市": "广东省", "梅州市": "广东省",
        "汕尾市": "广东省", "河源市": "广东省", "阳江市": "广东省",
        "清远市": "广东省", "东莞市": "广东省", "中山市": "广东省",
        "潮州市": "广东省", "揭阳市": "广东省", "云浮市": "广东省",
        # 浙江省
        "杭州市": "浙江省", "宁波市": "浙江省", "温州市": "浙江省",
        "嘉兴市": "浙江省", "湖州市": "浙江省", "绍兴市": "浙江省",
        "金华市": "浙江省", "衢州市": "浙江省", "舟山市": "浙江省",
        "台州市": "浙江省", "丽水市": "浙江省",
        # 江苏省
        "南京市": "江苏省", "无锡市": "江苏省", "徐州市": "江苏省",
        "常州市": "江苏省", "苏州市": "江苏省", "南通市": "江苏省",
        "连云港市": "江苏省", "淮安市": "江苏省", "盐城市": "江苏省",
        "扬州市": "江苏省", "镇江市": "江苏省", "泰州市": "江苏省", "宿迁市": "江苏省",
        # 山东省
        "济南市": "山东省", "青岛市": "山东省", "淄博市": "山东省",
        "枣庄市": "山东省", "东营市": "山东省", "烟台市": "山东省",
        "潍坊市": "山东省", "济宁市": "山东省", "泰安市": "山东省",
        "威海市": "山东省", "日照市": "山东省", "临沂市": "山东省",
        "德州市": "山东省", "聊城市": "山东省", "滨州市": "山东省", "菏泽市": "山东省",
        # 河南省
        "郑州市": "河南省", "开封市": "河南省", "洛阳市": "河南省",
        "平顶山市": "河南省", "安阳市": "河南省", "鹤壁市": "河南省",
        "新乡市": "河南省", "焦作市": "河南省", "濮阳市": "河南省",
        "许昌市": "河南省", "漯河市": "河南省", "三门峡市": "河南省",
        "南阳市": "河南省", "商丘市": "河南省", "信阳市": "河南省",
        "周口市": "河南省", "驻马店市": "河南省",
        # 湖北省
        "武汉市": "湖北省", "黄石市": "湖北省", "十堰市": "湖北省",
        "宜昌市": "湖北省", "襄阳市": "湖北省", "鄂州市": "湖北省",
        "荆门市": "湖北省", "孝感市": "湖北省", "荆州市": "湖北省",
        "黄冈市": "湖北省", "咸宁市": "湖北省", "随州市": "湖北省",
        "恩施土家族苗族自治州": "湖北省",
        # 四川省
        "成都市": "四川省", "自贡市": "四川省", "攀枝花市": "四川省",
        "泸州市": "四川省", "德阳市": "四川省", "绵阳市": "四川省",
        "广元市": "四川省", "遂宁市": "四川省", "内江市": "四川省",
        "乐山市": "四川省", "南充市": "四川省", "眉山市": "四川省",
        "宜宾市": "四川省", "广安市": "四川省", "达州市": "四川省",
        "雅安市": "四川省", "巴中市": "四川省", "资阳市": "四川省",
        "阿坝藏族羌族自治州": "四川省", "甘孜藏族自治州": "四川省", "凉山彝族自治州": "四川省",
        # 贵州省
        "贵阳市": "贵州省", "六盘水市": "贵州省", "遵义市": "贵州省",
        "安顺市": "贵州省", "毕节市": "贵州省", "铜仁市": "贵州省",
        "黔西南布依族苗族自治州": "贵州省", "黔东南苗族侗族自治州": "贵州省", "黔南布依族苗族自治州": "贵州省",
        # 云南省
        "昆明市": "云南省", "曲靖市": "云南省", "玉溪市": "云南省",
        "保山市": "云南省", "昭通市": "云南省", "丽江市": "云南省",
        "普洱市": "云南省", "临沧市": "云南省",
        "楚雄彝族自治州": "云南省", "红河哈尼族彝族自治州": "云南省", "文山壮族苗族自治州": "云南省",
        "西双版纳傣族自治州": "云南省", "大理白族自治州": "云南省", "德宏傣族景颇族自治州": "云南省", "怒江傈僳族自治州": "云南省", "迪庆藏族自治州": "云南省",
        # 陕西省
        "西安市": "陕西省", "铜川市": "陕西省", "宝鸡市": "陕西省",
        "咸阳市": "陕西省", "渭南市": "陕西省", "延安市": "陕西省",
        "汉中市": "陕西省", "榆林市": "陕西省", "安康市": "陕西省", "商洛市": "陕西省",
        # 安徽省
        "合肥市": "安徽省", "芜湖市": "安徽省", "蚌埠市": "安徽省",
        "淮南市": "安徽省", "马鞍山市": "安徽省", "淮北市": "安徽省",
        "铜陵市": "安徽省", "安庆市": "安徽省", "黄山市": "安徽省",
        "滁州市": "安徽省", "阜阳市": "安徽省", "宿州市": "安徽省",
        "六安市": "安徽省", "亳州市": "安徽省", "池州市": "安徽省", "宣城市": "安徽省",
        # 江西省
        "南昌市": "江西省", "景德镇市": "江西省", "萍乡市": "江西省",
        "九江市": "江西省", "新余市": "江西省", "鹰潭市": "江西省",
        "赣州市": "江西省", "吉安市": "江西省", "宜春市": "江西省",
        "抚州市": "江西省", "上饶市": "江西省",
        # 福建省
        "福州市": "福建省", "厦门市": "福建省", "莆田市": "福建省",
        "三明市": "福建省", "泉州市": "福建省", "漳州市": "福建省",
        "南平市": "福建省", "龙岩市": "福建省", "宁德市": "福建省",
        # 北京市
        "北京市": None,  # 直辖市
        # 上海市
        "上海市": None,  # 直辖市
        # 天津市
        "天津市": None,  # 直辖市
        # 重庆市
        "重庆市": None,  # 直辖市
        # 内蒙古
        "呼和浩特市": "内蒙古自治区", "包头市": "内蒙古自治区", "乌海市": "内蒙古自治区",
        "赤峰市": "内蒙古自治区", "通辽市": "内蒙古自治区", "鄂尔多斯市": "内蒙古自治区",
        "呼伦贝尔市": "内蒙古自治区", "巴彦淖尔市": "内蒙古自治区", "乌兰察布市": "内蒙古自治区",
        "兴安盟": "内蒙古自治区", "锡林郭勒盟": "内蒙古自治区", "阿拉善盟": "内蒙古自治区",
        # 广西
        "南宁市": "广西壮族自治区", "柳州市": "广西壮族自治区", "桂林市": "广西壮族自治区",
        "梧州市": "广西壮族自治区", "北海市": "广西壮族自治区", "防城港市": "广西壮族自治区",
        "钦州市": "广西壮族自治区", "贵港市": "广西壮族自治区", "玉林市": "广西壮族自治区",
        "百色市": "广西壮族自治区", "贺州市": "广西壮族自治区", "河池市": "广西壮族自治区",
        "来宾市": "广西壮族自治区", "崇左市": "广西壮族自治区",
        # 海南省
        "海口市": "海南省", "三亚市": "海南省", "三沙市": "海南省", "儋州市": "海南省",
        # 黑龙江省
        "哈尔滨市": "黑龙江省", "齐齐哈尔市": "黑龙江省", "鸡西市": "黑龙江省",
        "鹤岗市": "黑龙江省", "双鸭山市": "黑龙江省", "大庆市": "黑龙江省",
        "伊春市": "黑龙江省", "佳木斯市": "黑龙江省", "七台河市": "黑龙江省",
        "牡丹江市": "黑龙江省", "黑河市": "黑龙江省", "绥化市": "黑龙江省",
        # 吉林省
        "长春市": "吉林省", "吉林市": "吉林省", "四平市": "吉林省",
        "辽源市": "吉林省", "通化市": "吉林省", "白山市": "吉林省",
        "松原市": "吉林省", "白城市": "吉林省", "延边朝鲜族自治州": "吉林省",
        # 辽宁省
        "沈阳市": "辽宁省", "大连市": "辽宁省", "鞍山市": "辽宁省",
        "抚顺市": "辽宁省", "本溪市": "辽宁省", "丹东市": "辽宁省",
        "锦州市": "辽宁省", "营口市": "辽宁省", "阜新市": "辽宁省",
        "辽阳市": "辽宁省", "盘锦市": "辽宁省", "铁岭市": "辽宁省",
        "朝阳市": "辽宁省", "葫芦岛市": "辽宁省",
        # 河北省
        "石家庄市": "河北省", "唐山市": "河北省", "秦皇岛市": "河北省",
        "邯郸市": "河北省", "邢台市": "河北省", "保定市": "河北省",
        "张家口市": "河北省", "承德市": "河北省", "沧州市": "河北省",
        "廊坊市": "河北省", "衡水市": "河北省",
        # 山西省
        "太原市": "山西省", "大同市": "山西省", "阳泉市": "山西省",
        "长治市": "山西省", "晋城市": "山西省", "朔州市": "山西省",
        "晋中市": "山西省", "运城市": "山西省", "忻州市": "山西省",
        "临汾市": "山西省", "吕梁市": "山西省",
        # 甘肃省
        "兰州市": "甘肃省", "嘉峪关市": "甘肃省", "金昌市": "甘肃省",
        "白银市": "甘肃省", "天水市": "甘肃省", "武威市": "甘肃省",
        "张掖市": "甘肃省", "平凉市": "甘肃省", "酒泉市": "甘肃省",
        "庆阳市": "甘肃省", "定西市": "甘肃省", "陇南市": "甘肃省",
        "临夏回族自治州": "甘肃省", "甘南藏族自治州": "甘肃省",
        # 青海省
        "西宁市": "青海省", "海东市": "青海省",
        "海北藏族自治州": "青海省", "黄南藏族自治州": "青海省", "海南藏族自治州": "青海省",
        "果洛藏族自治州": "青海省", "玉树藏族自治州": "青海省", "海西蒙古族藏族自治州": "青海省",
        # 宁夏
        "银川市": "宁夏回族自治区", "石嘴山市": "宁夏回族自治区", "吴忠市": "宁夏回族自治区",
        "固原市": "宁夏回族自治区", "中卫市": "宁夏回族自治区",
        # 新疆
        "乌鲁木齐市": "新疆维吾尔自治区", "克拉玛依市": "新疆维吾尔自治区",
        "吐鲁番市": "新疆维吾尔自治区", "哈密市": "新疆维吾尔自治区",
        "昌吉回族自治州": "新疆维吾尔自治区", "博尔塔拉蒙古自治州": "新疆维吾尔自治区",
        "巴音郭楞蒙古自治州": "新疆维吾尔自治区", "阿克苏地区": "新疆维吾尔自治区",
        "克孜勒苏柯尔克孜自治州": "新疆维吾尔自治区", "喀什地区": "新疆维吾尔自治区",
        "和田地区": "新疆维吾尔自治区", "伊犁哈萨克自治州": "新疆维吾尔自治区", "塔城地区": "新疆维吾尔自治区", "阿勒泰地区": "新疆维吾尔自治区",
        # 西藏
        "拉萨市": "西藏自治区", "日喀则市": "西藏自治区", "昌都市": "西藏自治区",
        "林芝市": "西藏自治区", "山南市": "西藏自治区", "那曲市": "西藏自治区", "阿里地区": "西藏自治区",
    }

    async def select_location(self):
        """
        选择搜索地区

        根据搜索层级：
        - 省级（如湖南省）：直接选择省份
        - 市级（如长沙市）：先选择上级省份，再选择具体城市
        """
        self.log(f"  搜索地区: {self.search_location}")
        self.log(f"  搜索层级: {'省级' if self.district_level == 'province' else '市级'}")

        # 提取地区名称（去掉"省"字）
        location_name = self.search_location.replace("省", "").replace("市", "")

        # 判断是否需要先选省份
        need_province_first = (self.district_level == "city" and
                               self.search_location in self.CITY_TO_PROVINCE and
                               self.CITY_TO_PROVINCE[self.search_location] is not None)

        if need_province_first:
            province = self.CITY_TO_PROVINCE[self.search_location]
            self.log(f"  市級搜索，需要先选省份: {province}")

            # 先选择省份
            self.log("  正在选择省份...")
            province_clicked = await self._click_location_element(province)
            if province_clicked:
                self.log(f"  ✅ 省份 {province} 已选择")
                await self.page.wait_for_timeout(1500)

            # 再选择城市
            self.log(f"  正在选择城市: {self.search_location}")
            city_clicked = await self._click_location_element(self.search_location)
            if city_clicked:
                self.log(f"  ✅ 城市 {self.search_location} 已选择")
            else:
                # 尝试带"市"后缀
                city_clicked = await self._click_location_element(location_name + "市")
                if city_clicked:
                    self.log(f"  ✅ 城市 {location_name}市 已选择")

            await self.page.wait_for_timeout(1000)

            # 验证城市是否选择成功
            if await self.verify_selection(self.search_location) or await self.verify_selection(location_name):
                self.log("  ✅ 地区选择完成")
                return True
            else:
                self.log("  ⚠️ 地区选择可能有问题，但继续执行...")
                return True
        else:
            # 省级搜索或直辖市，直接选择
            self.log(f"  地区名称: {location_name}")
            return await self._select_single_location(location_name)

    async def _click_location_element(self, text):
        """点击地区元素"""
        # 策略1: JavaScript点击
        try:
            click_result = await self.page.evaluate(f'''() => {{
                const allElements = document.querySelectorAll('a, span, div, label, li');
                for (const el of allElements) {{
                    const t = el.textContent.trim();
                    if (t === '{text}') {{
                        const style = window.getComputedStyle(el);
                        if (style.display !== 'none' && style.visibility !== 'hidden') {{
                            el.click();
                            return 'clicked';
                        }}
                    }}
                }}
                return null;
            }}''')
            if click_result:
                return True
        except:
            pass

        # 策略2: Playwright点击
        try:
            elem = self.page.locator(f'text="{text}"').first
            if await elem.is_visible(timeout=2000):
                await elem.click()
                return True
        except:
            pass

        return False

    async def _select_single_location(self, location_name):
        """选择单一地区（省级搜索）"""
        # 等待地区列表加载
        await self.page.wait_for_timeout(1000)

        # 策略1: JavaScript点击
        self.log("  策略1: JavaScript点击地区...")
        try:
            click_result = await self.page.evaluate(f'''() => {{
                const allElements = document.querySelectorAll('a, span, div, label, li');
                for (const el of allElements) {{
                    const text = el.textContent.trim();
                    if (text === '{location_name}' ||
                        text === '{location_name}省' ||
                        text === '{location_name}市') {{
                        const style = window.getComputedStyle(el);
                        if (style.display !== 'none' && style.visibility !== 'hidden') {{
                            el.click();
                            return 'clicked';
                        }}
                    }}
                }}
                return null;
            }}''')

            if click_result and click_result.startswith('clicked'):
                self.log(f"  策略1成功")
                await self.page.wait_for_timeout(1500)
            else:
                self.log("  策略1未找到地区元素")
        except Exception as e:
            self.log(f"  策略1失败: {e}")

        # 验证
        location_selected = await self.verify_selection(location_name)
        if location_selected:
            self.log("  ✅ 地区选择成功")
        else:
            self.log("  ⚠️ 地区可能未选择成功，尝试备用方法...")

            # 策略2: Playwright点击
            self.log("  策略2: Playwright点击地区...")
            try:
                selectors = [
                    f'text="{location_name}"',
                    f'text="{location_name}省"',
                    f'text="{location_name}市"',
                ]
                for selector in selectors:
                    try:
                        elem = self.page.locator(selector).first
                        if await elem.is_visible(timeout=2000):
                            await elem.click()
                            self.log(f"  策略2成功: {selector}")
                            await self.page.wait_for_timeout(1500)
                            break
                    except:
                        continue
            except Exception as e:
                self.log(f"  策略2失败: {e}")

        # 最终验证
        final_check = await self.verify_selection(location_name)
        if final_check:
            self.log("  ✅ 地区选择完成")
            return True
        else:
            self.log("  ❌ 地区选择可能有问题，但继续执行...")
            return True

    async def verify_selection(self, text):
        """
        验证某个条件是否已被选择

        Args:
            text: 要验证的文字

        Returns:
            bool: 是否已选择
        """
        try:
            # 检查已选条件区域是否包含该文字
            result = await self.page.evaluate(f'''() => {{
                const selectedAreas = document.querySelectorAll('.has-selected, .selected-conditions, [class*="selected"], .tag-list');
                for (const area of selectedAreas) {{
                    if (area.textContent.includes('{text}')) {{
                        return true;
                    }}
                }}
                return false;
            }}''')
            return result
        except:
            return False

    async def cache_current_conditions(self):
        """缓存当前选择的条件"""
        try:
            conditions_area = self.page.locator('.has-selected, .selected-conditions, [class*="selected"]').first
            if await conditions_area.is_visible(timeout=2000):
                conditions_text = await conditions_area.text_content()
                conditions = {
                    'text': conditions_text,
                    'city': self.search_location,
                    'status': self.company_status,
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
            
            # 检查地区
            if self.search_location in current_text or self.search_location.replace('市', '') in current_text:
                checks_passed += 1
                self.log(f"[条件验证] ✅ 地区: {self.search_location}")
            else:
                self.log(f"[条件验证] ⚠️ 地区未明确显示: {self.search_location}")
                # 城市不显示也继续，因为URL中可能已包含
            
            # 检查登记状态
            if self.company_status in current_text:
                checks_passed += 1
                self.log(f"[条件验证] ✅ 登记状态: {self.company_status}")
            else:
                self.log(f"[条件验证] ⚠️ 登记状态未显示: {self.company_status}")
            
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

    async def get_sub_districts_from_page(self):
        """
        从页面获取下级地区列表

        在设置好筛选条件后，等待页面刷新，然后从页面中解析出下级地区列表
        例如：选择湖南省后，获取长沙市、株洲市等地级市
        """
        try:
            # 等待页面刷新（设置筛选条件后需要等待）
            self.log("  等待页面刷新下级地区列表...")
            await self.page.wait_for_timeout(3000)

            # 优先尝试从搜索结果区域获取下级地区
            # 企查查通常在左侧或顶部显示筛选后的地区分布
            districts = []

            # 方法1：尝试查找地区分布元素
            try:
                # 查找包含地区和数量的元素（格式：地区名 (数量)）
                area_elements = await self.page.query_selector_all(
                    '.search-result__province, .province-item, [class*="province"], '
                    '.filter-tag, .area-tag, [class*="area"]'
                )
                for elem in area_elements:
                    try:
                        text = await elem.text_content()
                        if text:
                            # 匹配格式：长沙市 (12345)
                            match = re.search(r'([\u4e00-\u9fa5]+(?:市|州|区|县))\s*[\(（](\d+)[\)）]', text)
                            if match:
                                name = match.group(1)
                                if name not in ['公司', '企业', '集团', '有限', '工程', '实业']:
                                    districts.append(name)
                    except:
                        pass
            except:
                pass

            # 方法2：如果方法1没找到，尝试从整个页面文本中查找
            if not districts:
                body_text = await self.page.text_content('body')
                # 匹配下级地区（地级市或区县）的正则
                # 格式如：长沙市 (12345)、岳麓区 (5678)
                pattern = r'([\u4e00-\u9fa5]+(?:市|州|区|县))\s*[\(（]([\d,]+)[\)）]'
                matches = re.findall(pattern, body_text)

                for match in matches:
                    district_name = match[0]
                    # 排除一些干扰项
                    if district_name not in ['公司', '企业', '集团', '有限', '工程', '实业']:
                        # 排除直辖市和省份名称
                        if district_name not in ['北京市', '天津市', '上海市', '重庆市', '香港', '澳门']:
                            districts.append(district_name)

            # 去重
            districts = list(dict.fromkeys(districts))

            # 过滤：只保留看起来像区县/市名称的（长度2-6个汉字）
            districts = [d for d in districts if 2 <= len(d) <= 6]

            if districts:
                self.log(f"  解析到 {len(districts)} 个下级地区")
                return districts
            else:
                self.log("  未能解析到下级地区")
                return []

        except Exception as e:
            self.log(f"获取下级地区失败: {e}")
            return []

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

            # 先尝试点击"更多"按钮展开行业列表
            try:
                # 先关闭遮罩层
                try:
                    await self.page.evaluate('''() => {
                        const mask = document.querySelector('.expanded-mask, [class*="mask"]');
                        if (mask) mask.style.display = 'none';
                    }''')
                except:
                    pass

                more_btns = self.page.locator("a:has-text('更多'), button:has-text('更多'), span:has-text('更多')")
                count = await more_btns.count()
                for i in range(min(count, 5)):
                    more_btn = more_btns.nth(i)
                    if await more_btn.is_visible(timeout=2000):
                        await more_btn.click(force=True)
                        self.log(f"  [行业选择] 点击'更多'展开行业列表")
                        await self.page.wait_for_timeout(1000)
                        break
            except Exception as e:
                self.log(f"  [行业选择] 点击'更多'失败或无更多按钮: {e}")

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
            
            await self.page.wait_for_timeout(3000)

            # 截图记录选择后状态
            await self.screenshot(f"after_select_{industry_name[:8]}")

            # 验证行业是否真正被选中（检查地址栏是否包含行业名称）
            try:
                # 获取页面顶部地址栏内容 - 通常在面包屑或搜索条件区域
                breadcrumb = await self.page.locator('.breadcrumb, .condition-bar, [class*="address"], [class*="condition"]').first.text_content()
                if breadcrumb and industry_name in breadcrumb:
                    self.log(f"  [行业选择] ✅ 已点击: {industry_name}")
                    return True
            except:
                pass

            # 备选验证：检查页面是否有包含行业名称的可信元素
            try:
                # 等待更长时间让页面更新
                await self.page.wait_for_timeout(2000)
                # 查找"已选行业"或类似的标签
                selected_industry = await self.page.locator('text="已选行业"').first.text_content()
                if selected_industry and industry_name in selected_industry:
                    self.log(f"  [行业选择] ✅ 已点击: {industry_name}")
                    return True
            except:
                pass

            if clicked:
                self.log(f"  [行业选择] ⚠️ 已点击但未确认: {industry_name}")
                return True  # 仍然返回True，因为点击可能成功了
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
        # 优先从磁盘加载TaskManager状态（确保重启后状态不丢失）
        self.task_manager._load()

        if self.task_manager.session:
            pending = self.task_manager.get_pending_industries()
            if pending:
                self.log(f"[断点续爬] 发现未完成任务: {len(pending)} 个行业待处理")
                progress = self.task_manager.get_progress()
                self.log(f"[断点续爬] 进度: {self.task_manager.get_progress_bar()}")

                temp_data = load_temp_data(self.TEMP_DATA_FILE)
                if temp_data:
                    self.data = temp_data.get('data', [])
                    # 恢复industry_tasks状态
                    if 'industry_tasks' in temp_data:
                        for code, task_data in temp_data['industry_tasks'].items():
                            if code in self.task_manager.industry_tasks:
                                # 更新已有任务状态
                                for key, value in task_data.items():
                                    if hasattr(self.task_manager.industry_tasks[code], key):
                                        setattr(self.task_manager.industry_tasks[code], key, value)
                    self.log(f"[断点续爬] 已加载 {len(self.data)} 条历史数据")

                # 从session缓存恢复districts
                if self.task_manager.session.districts_cache:
                    self.districts = self.task_manager.session.districts_cache
                    self.log(f"[断点续爬] 已恢复地区列表: {len(self.districts)} 个地区")

                return True
            else:
                self.log("[会话完成] 所有行业已爬取，无需继续")
                return False
        else:
            self.log("[新建会话] 初始化任务...")
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
                    industry_total = sum(district_data.values())

                    # ====== 数据合理性校验 ======
                    # 如果某行业数据接近制造业合计（>90%），说明行业选择可能未生效
                    if hasattr(self, 'city_total_data') and self.city_total_data:
                        city_total = sum(self.city_total_data.values())
                        if city_total > 0 and industry_total > 0:
                            ratio = industry_total / city_total
                            if ratio > 0.9:
                                self.log(f"  ⚠️ 警告: {industry_name} 数据 ({industry_total}) 接近制造业合计 ({city_total})")
                                self.log(f"  ⚠️ 比例: {ratio*100:.1f}%，行业选择可能未生效！")
                                await self.screenshot(f"WARNING_{industry_code}_data_anomaly")
                                # 重试一次：先取消选择，再重新点击
                                self.log(f"  重试行业选择...")
                                await self.deselect_industry(industry_name)
                                await self.page.wait_for_timeout(1000)
                                if not await self.click_industry(industry_name):
                                    self.log(f"  ❌ 重试失败，跳过该行业")
                                    self.task_manager.fail_industry(industry_code, "行业选择数据异常")
                                    return False
                                # 重新获取数据
                                district_data = await self.get_district_distribution()
                                industry_total = sum(district_data.values()) if district_data else 0
                                self.log(f"  重试后数据: {industry_total} 家企业")

                    for district, count in district_data.items():
                        self.data.append({
                            "区县": district,
                            "行业代码": industry_code,
                            "行业类别": industry_name,
                            "企业数量": count
                        })
                        self.task_manager.update_district(district, industry_code, industry_name, count)

                    self.log(f"  ✅ {industry_name}: {len(district_data)} 个区县, 共 {industry_total} 家企业")
                    
                    # 保存行业明细表 + 截图
                    if hasattr(self, 'city_total_data'):
                        filters = self.get_current_filters()
                        filters['筛选行业'] = industry_name
                        save_industry_detail_table(
                            self.output_dir,
                            industry_code,
                            industry_name,
                            district_data,
                            self.city_total_data,
                            self.search_location,
                            filters
                        )
                        await self.screenshot(f"{industry_name}行业汇总表_[{filters['地区']}]_[{filters['登记状态']}]")
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

                    # 更新执行进度表
                    industry_tasks = [asdict(t) for t in self.task_manager.industry_tasks.values()]
                    update_execution_progress(self.OUTPUT_FILE, industry_tasks)
                    self.log(f"  执行进度表已更新")
                else:
                    self.log(f"  ⚠️ {industry_name}: 未获取到数据")
                    self.task_manager.fail_industry(industry_code, "未获取到区县数据")
                    return False
                
                # 取消选择行业
                await self.deselect_industry(industry_name)

                # 保存临时数据（包含完整状态）
                save_temp_data(self.TEMP_DATA_FILE, {
                    "processed_industries": [t.code for t in self.task_manager.get_completed_industries()],
                    "data": self.data,
                    "session": asdict(self.task_manager.session) if self.task_manager.session else None,
                    "industry_tasks": {code: asdict(t) for code, t in self.task_manager.industry_tasks.items()}
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
                filters = self.get_current_filters()
                save_city_summary_table(self.output_dir, manufacturing_data, self.search_location, filters)
                await self.screenshot(f"城市汇总表_[{filters['地区']}]_[{filters['登记状态']}]")
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
        # ====== 开始爬取前再次验证筛选条件 ======
        if not await self.verify_filters_are_set():
            self.log("❌ [错误] 筛选条件验证失败，无法继续爬取行业数据！")
            await self.screenshot("FILTER_VERIFICATION_FAILED_BATCH")
            return False

        pending_industries = self.task_manager.get_pending_industries()
        total = len(pending_industries)

        if total == 0:
            self.log("所有行业已完成爬取")
            return True

        self.log(f"待处理行业数: {total}")
        self.log(f"每 {restart_interval} 个行业重启浏览器")

        completed_count = 0

        # 进度回调：报告当前状态
        def report_progress(industry_idx, total, current_industry, completed, browser_restart):
            if self.progress_callback:
                self.progress_callback({
                    'current': industry_idx + 1,
                    'total': total,
                    'current_industry': current_industry,
                    'completed': completed,
                    'browser_restart_count': browser_restart
                })

        for i, task in enumerate(pending_industries):
            industry_code = task.code
            industry_name = task.name

            progress = f"[{i+1}/{total}]"
            self.log(f"{progress} 处理: {industry_name} ({industry_code})")

            # 调用进度回调
            report_progress(i, total, industry_name, completed_count, self.browser_restart_count)

            try:
                success = await self.crawl_single_industry(industry_code, industry_name)
                if success:
                    completed_count += 1
                
                # 每restart_interval个行业，重启浏览器
                if completed_count > 0 and completed_count % restart_interval == 0:
                    self.browser_restart_count += 1
                    self.log(f"已完成 {completed_count} 个行业，重启浏览器 (第{self.browser_restart_count}次)...")

                    # 保存当前进度（包含完整状态）
                    save_temp_data(self.TEMP_DATA_FILE, {
                        "processed_industries": [t.code for t in self.task_manager.get_completed_industries()],
                        "data": self.data,
                        "session": asdict(self.task_manager.session) if self.task_manager.session else None,
                        "industry_tasks": {code: asdict(t) for code, t in self.task_manager.industry_tasks.items()}
                    })

                    # 关闭浏览器
                    await self.close_browser()

                    # 重新启动浏览器
                    await self.init_browser()

                    # 从磁盘重新加载TaskManager状态（恢复in_progress状态）
                    self.task_manager._load()

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

                # 保存当前进度（包含完整状态）
                save_temp_data(self.TEMP_DATA_FILE, {
                    "processed_industries": [t.code for t in self.task_manager.get_completed_industries()],
                    "data": self.data,
                    "session": asdict(self.task_manager.session) if self.task_manager.session else None,
                    "industry_tasks": {code: asdict(t) for code, t in self.task_manager.industry_tasks.items()}
                })

                # 尝试重启浏览器
                self.browser_restart_count += 1
                self.log(f"尝试重启浏览器 (第{self.browser_restart_count}次)...")
                try:
                    await self.close_browser()
                    await self.init_browser()

                    # 从磁盘重新加载TaskManager状态（恢复in_progress状态）
                    self.task_manager._load()

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
            self.log(f"{self.search_location}制造业企业数据爬虫启动")
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

            # 续爬时检查是否需要创建Excel模板
            if is_resume and os.path.exists(self.OUTPUT_FILE):
                self.log(f"检测到现有Excel文件，复用: {self.OUTPUT_FILE}")
                # 将已恢复的数据写入Excel
                if self.data:
                    self.log(f"将 {len(self.data)} 条已恢复数据写入Excel...")
                    update_excel_data(self.OUTPUT_FILE, self.data)
                    update_all_district_sheets(self.OUTPUT_FILE, self.data, self.districts, MANUFACTURING_SUBCATEGORIES)
                    update_summary_sheet(self.OUTPUT_FILE, self.data)
                # 更新执行进度表
                if self.task_manager.industry_tasks:
                    industry_tasks = [asdict(t) for t in self.task_manager.industry_tasks.values()]
                    update_execution_progress(self.OUTPUT_FILE, industry_tasks)
                    self.log("执行进度表已更新（续爬）")
            else:
                self.log("创建Excel文件...")
                create_excel_template(self.OUTPUT_FILE)
                create_district_sheets(self.OUTPUT_FILE, self.districts, MANUFACTURING_SUBCATEGORIES)
                create_summary_sheet(self.OUTPUT_FILE, self.districts, MANUFACTURING_SUBCATEGORIES)

            # 初始化执行进度表
            if self.task_manager.industry_tasks:
                industry_tasks = [asdict(t) for t in self.task_manager.industry_tasks.values()]
                update_execution_progress(self.OUTPUT_FILE, industry_tasks)
                self.log("执行进度表已初始化")

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

            # 从页面获取下级地区列表
            self.log("获取下级地区列表...")
            self.districts = await self.get_sub_districts_from_page()
            if not self.districts:
                self.log("⚠️ 未能获取下级地区，使用默认列表")
                # 默认列表（省级使用常见地级市，市级使用常见区县）
                self.districts = []
            else:
                self.log(f"  发现 {len(self.districts)} 个下级地区: {', '.join(self.districts[:5])}...")

            # 新建会话：此时districts已获取，初始化任务会话
            if not is_resume and not self.task_manager.session:
                self.log("[新建会话] 初始化任务...")
                industries = dict(MANUFACTURING_SUBCATEGORIES)
                self.task_manager.init_session(
                    city=self.search_location,
                    status_filter=self.company_status,
                    industries=industries,
                    districts=self.districts
                )
                self.index_cache.set_city(self.search_location)
                self.index_cache.set_districts(self.districts)
                self.index_cache.set_industries(industries)

            # ====== 强制验证筛选条件 ======
            self.log("=" * 60)
            self.log("验证筛选条件设置...")
            if not await self.verify_filters_are_set():
                self.log("❌ [错误] 筛选条件验证失败，无法继续爬取！")
                self.log("请检查：1) 地区是否正确选择 2) 登记状态是否勾选 3) 制造业是否已点击")
                await self.screenshot("FILTER_VERIFICATION_FAILED")
                return
            self.log("=" * 60)

            # 获取制造业汇总数据
            if not await self.crawl_all_industries():
                return

            # 批量爬取各行业数据（每10个行业重启浏览器）
            if not await self.crawl_industries_batch(restart_interval=10):
                return
            
            # ====== 步骤3: 所有行业完成后保存汇总明细表 ======
            self.log("保存汇总明细表...")
            filters = self.get_current_filters()
            summary_file = save_summary_table(self.output_dir, self.data, self.search_location, filters)
            self.log(f"  汇总明细表已保存: {summary_file}")
            
            self.log("更新Excel汇总表...")
            update_summary_sheet(self.OUTPUT_FILE, self.data)
            update_all_district_sheets(self.OUTPUT_FILE, self.data, self.districts, MANUFACTURING_SUBCATEGORIES)
            
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
            self.log(f"  - 地区汇总表: 行业明细表/00_{self.search_location}制造业总表.xlsx")
            self.log(f"  - 行业明细表: 行业明细表/*.xlsx (31个文件)")
            self.log(f"  - 汇总明细表: {self.search_location}制造业企业数量明细表.xlsx")
            self.log("=" * 60)

            # 生成简报
            self.generate_summary_report()

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

    def generate_summary_report(self):
        """生成爬取简报"""
        import json
        from datetime import datetime

        # 统计数据
        completed_industries = self.task_manager.get_completed_industries()
        failed_industries = self.task_manager.get_failed_industries()

        # 计算企业总数（self.data是列表，每项包含"企业数量"字段）
        total_enterprises = sum(item.get('企业数量', 0) for item in self.data) if self.data else 0

        # 各行业企业数量
        industry_counts = {}
        for item in self.data:
            name = item.get('行业类别', '未知')
            count = item.get('企业数量', 0)
            industry_counts[name] = industry_counts.get(name, 0) + count

        # 简报内容
        report = {
            "生成时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "爬取地区": self.search_location,
            "行业总数": len(MANUFACTURING_SUBCATEGORIES),
            "已完成行业": len(completed_industries),
            "失败行业": len(failed_industries),
            "浏览器重启次数": self.browser_restart_count,
            "企业数据记录数": len(self.data),
            "企业总数": total_enterprises,
            "输出目录": self.output_dir,
            "各行业企业数": industry_counts
        }

        # 保存简报JSON
        report_file = os.path.join(self.output_dir, "爬取简报.json")
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        # 打印简报
        self.log("=" * 60)
        self.log("【爬取简报】")
        self.log("=" * 60)
        self.log(f"  爬取地区: {report['爬取地区']}")
        self.log(f"  行业总数: {report['行业总数']}")
        self.log(f"  已完成行业: {report['已完成行业']}")
        self.log(f"  失败行业: {report['失败行业']}")
        self.log(f"  浏览器重启次数: {report['浏览器重启次数']}")
        self.log(f"  企业数据记录数: {report['企业数据记录数']:,} 条")
        self.log(f"  企业总数: {report['企业总数']:,} 家")
        self.log(f"  输出目录: {report['输出目录']}")
        self.log(f"  简报文件: {report_file}")
        self.log("=" * 60)

        return report


async def run_with_retry(max_retries: int = 10, retry_delay: int = 30):
    """带自动重试的运行函数

    Args:
        max_retries: 最大重试次数
        retry_delay: 重试间隔(秒)
    """
    retry_count = 0
    output_dir = None  # 保存输出目录，确保重试时使用同一个目录
    user_config = None  # 保存用户配置

    # ============================================================
    # 获取用户输入配置
    # ============================================================
    print("\n" + "=" * 60)
    print("企查查行业搜索爬虫")
    print("=" * 60)
    print()

    # 获取用户输入
    user_input = get_user_input()
    user_config = user_input

    # 根据用户输入更新全局配置
    config.CRAWL_KEYWORD = user_config["keyword"]
    config.SEARCH_LOCATION = user_config["search_location"]
    config.DISTRICT_LEVEL = user_config["district_level"]
    config.COMPANY_STATUS = user_config["company_status"]

    # 下级地区会在程序运行时从页面动态获取
    print(f"  搜索地区: {user_config['search_location']}")
    print(f"  搜索层级: {'省级（查看地级市分布）' if user_config['district_level'] == 'province' else '市级（查看区县分布）'}")
    print()

    # ============================================================
    # 检查是否有未完成的目录
    # ============================================================
    from config_changsha import find_latest_incomplete_dir
    existing_dir = find_latest_incomplete_dir()
    if existing_dir:
        print(f"\n发现未完成的爬取目录: {existing_dir}")
        resume = input("是否继续上次的爬取? (Y/n): ").strip().lower()
        if resume == 'n':
            output_dir = None
            user_input["keyword"] = input("请输入新的关键字（直接回车使用原关键字）: ").strip()
            if user_input["keyword"]:
                user_config["keyword"] = user_input["keyword"]
                config.CRAWL_KEYWORD = user_config["keyword"]
        else:
            output_dir = existing_dir

    while retry_count < max_retries:
        retry_count += 1

        print(f"\n{'=' * 60}")
        print(f"第 {retry_count}/{max_retries} 次运行")
        print(f"{'=' * 60}\n")

        # 如果已有输出目录，继续使用同一个目录续爬
        # 传递用户配置给爬虫
        crawler = ChangshaCrawler(output_dir=output_dir, user_config=user_config)
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
