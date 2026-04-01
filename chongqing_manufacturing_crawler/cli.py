#!/usr/bin/env python3
"""
企查查行业搜索爬虫 - 统一CLI入口
支持菜单选择不同功能，全新交互式界面
"""
import asyncio
import sys
import os
import threading
import time
import json

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 默认数据文件夹
DEFAULT_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
DEFAULT_OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")

# 搜索配置保存文件
SEARCH_CONFIG_FILE = os.path.join(DEFAULT_DATA_DIR, "search_config.json")


def save_search_config(config):
    """保存搜索配置到JSON文件"""
    try:
        os.makedirs(DEFAULT_DATA_DIR, exist_ok=True)
        with open(SEARCH_CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        print(f"   ⚠️ 保存配置失败: {e}")
        return False


def load_search_config():
    """从JSON文件加载搜索配置"""
    try:
        if os.path.exists(SEARCH_CONFIG_FILE):
            with open(SEARCH_CONFIG_FILE, 'r', encoding='utf-8') as f:
                config = json.load(f)
                return config
    except Exception as e:
        print(f"   ⚠️ 读取配置失败: {e}")
    return None


def print_banner():
    """打印横幅"""
    print("\n" + "=" * 60)
    print("    企查查行业搜索爬虫 - 制造业企业数据采集工具")
    print("=" * 60)


async def save_cookie():
    """保存Cookie"""
    from playwright.async_api import async_playwright
    import json

    COOKIE_FILE = "data/qcc_cookies.json"

    print("\n" + "=" * 60)
    print("【Cookie登录管理】")
    print("=" * 60)

    print("\n🔐 开始登录流程...")
    print("   将打开浏览器，请扫码或输入账号密码登录")
    print("   登录成功后，Cookie将自动保存")
    print("   按 Ctrl+C 可取消登录\n")

    try:
        input("   按回车键继续打开浏览器...")

        async with async_playwright() as p:
            # 启动浏览器
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context()
            page = await context.new_page()

            # 访问企查查
            print("\n   正在打开企查查网站...")
            await page.goto("https://www.qcc.com", wait_until="networkidle", timeout=60000)

            # 等待用户登录
            print("\n" + "=" * 40)
            print("   ⚠️ 请在浏览器中扫码或登录")
            print("   ⚠️ 登录成功后，点击任意页面确保登录状态生效")
            print("   ⚠️ 确认登录后，回到此窗口按回车键保存Cookie")
            print("=" * 40)

            input("\n   登录成功后，按回车键保存Cookie...")

            # 获取Cookie
            cookies = await context.cookies()

            if cookies:
                # 保存Cookie
                os.makedirs(os.path.dirname(COOKIE_FILE), exist_ok=True)
                cookie_data = {
                    "cookies": cookies,
                    "save_time": time.strftime("%Y-%m-%d %H:%M:%S")
                }
                with open(COOKIE_FILE, 'w', encoding='utf-8') as f:
                    json.dump(cookie_data, f, ensure_ascii=False, indent=2)

                print(f"\n   ✅ Cookie已保存到: {COOKIE_FILE}")
                print(f"   📊 共保存 {len(cookies)} 个Cookie")
            else:
                print("\n   ❌ 未获取到Cookie，登录可能失败")

            await browser.close()

    except KeyboardInterrupt:
        print("\n\n   已取消登录")
        return False
    except Exception as e:
        print(f"\n   ❌ 登录过程出错: {e}")
        import traceback
        traceback.print_exc()
        return False

    return True


def print_menu():
    """打印主菜单"""
    print("\n【主菜单】请选择功能：")
    print("  0. 🔐 登录/更新Cookie - 扫码登录企查查，保存登录状态")
    print("  1. 🚀 启动爬虫采集 - 采集制造业数据")
    print("  2. 📊 表格格式转换 - 将明细数据转换为目标格式")
    print("  3. ✅ 数据完整性核实 - 验证已爬取数据的准确性")
    print("  4. 📁 打开数据目录 - 在 Finder 中打开输出目录")
    print("  5. ❌ 退出程序")
    print()


def run_crawler():
    """运行爬虫 - 交互式引导"""
    from crawler_changsha import run_with_retry, ChangshaCrawler
    from config_changsha import find_latest_incomplete_dir
    import config

    print("\n" + "=" * 60)
    print("【爬虫采集向导】")
    print("=" * 60)

    # 检查是否有未完成的目录
    existing_dir = find_latest_incomplete_dir()

    # 选择爬取模式
    print("\n请选择爬取模式：")
    print("   1. 🔄 继续上次爬取 - 从中断的行业继续（如果有未完成的数据）")
    print("   2. 🆕 全新爬取 - 清空数据，从头开始")
    if existing_dir:
        print(f"\n   📁 发现未完成目录: {existing_dir}")
        print("   💡 建议选择 '继续上次爬取'")

    mode_choice = input("\n   > ").strip()

    if mode_choice == "2":
        # 全新爬取
        existing_dir = None
        print("\n   已选择【全新爬取模式】")
    else:
        # 继续上次（或新建）
        if not existing_dir:
            print("\n   未发现未完成的数据，将开始全新爬取")
        else:
            print(f"\n   已选择【继续上次爬取】: {existing_dir}")

    # 检查是否有保存的搜索配置
    saved_config = load_search_config()
    user_config = None

    if not existing_dir:
        # 获取用户输入
        if saved_config:
            print("\n📁 发现保存的搜索配置：")
            print(f"   地区: {saved_config.get('search_location', '')}")
            print(f"   层级: {saved_config.get('district_level', '')}")
            print(f"   国标行业: {saved_config.get('industry', '制造业')}")
            print(f"   登记状态: {saved_config.get('company_status', '')}")
            print("\n是否使用此配置？")
            print("   1. ✅ 使用保存的配置")
            print("   2. ✏️ 输入新配置")

            use_saved = input("\n   > ").strip()
            if use_saved == "1":
                user_config = saved_config
                print("\n   ✅ 已使用保存的配置")

        if not user_config:
            # 输入新配置
            print("\n📍 请输入要搜索的地区（省份/城市）：")
            search_location = input("   > ").strip()

            if not search_location:
                print("   ❌ 地区不能为空")
                return

            print("\n📊 请选择搜索层级：")
            print("   1. 省级 - 查看地级市分布（如：湖南省 → 长沙市、株洲市等）")
            print("   2. 市级 - 查看区县分布（如：长沙市 → 岳麓区、芙蓉区等）")
            district_level = input("   > ").strip()

            if district_level == "1":
                district_level = "province"
                level_desc = "省级"
            else:
                district_level = "city"
                level_desc = "市级"

            print("\n🏢 请输入登记状态筛选（直接回车使用默认'存续'）：")
            print("   常见选项：存续（在业）、迁出、注销、吊销等")
            company_status = input("   > ").strip()
            if not company_status:
                company_status = "存续（在业）"

            # 构建用户配置字典
            user_config = {
                "keyword": "制造业",
                "search_location": search_location,
                "district_level": district_level,
                "company_status": company_status,
                "industry": "制造业"
            }

            # 保存配置
            if save_search_config(user_config):
                print("\n   ✅ 配置已保存")

        # 显示配置确认
        level_desc = "省级" if user_config["district_level"] == "province" else "市级"
        print("\n" + "-" * 40)
        print("【配置确认】")
        print(f"   搜索地区: {user_config['search_location']} ({level_desc})")
        print(f"   登记状态: {user_config['company_status']}")
        print(f"   国标行业: {user_config.get('industry', user_config.get('keyword', '制造业'))}")
        print("-" * 40)

        # 更新全局配置
        config.SEARCH_LOCATION = user_config["search_location"]
        config.DISTRICT_LEVEL = user_config["district_level"]
        config.COMPANY_STATUS = user_config["company_status"]
        config.CRAWL_KEYWORD = user_config["keyword"]
        if "industry" in user_config:
            config.INDUSTRY = user_config["industry"]
    else:
        # 继续上次爬取，加载保存的配置
        if saved_config:
            user_config = saved_config
        else:
            print("   ⚠️ 未找到保存的配置，将使用全局配置")
            user_config = None

    print("\n" + "=" * 60)
    print("【开始爬取】")
    print("=" * 60)

    # 进度显示线程
    progress_info = {'running': False, 'current': 0, 'total': 31, 'current_industry': '', 'completed': 0, 'browser_restart': 0}

    def progress_printer():
        """定期打印进度"""
        while progress_info['running']:
            if progress_info['current'] > 0 or progress_info['completed'] > 0:
                print(f"\r   进度: {progress_info['completed']}/{progress_info['total']} | 当前: {progress_info['current_industry'][:15]}... | 浏览器重启: {progress_info['browser_restart']}次", end='', flush=True)
            time.sleep(1)

    # 创建爬虫实例以获取进度回调
    class ProgressCrawler(ChangshaCrawler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.progress_info = progress_info

        async def crawl_industries_batch(self, restart_interval=10):
            # 设置进度回调
            def callback(info):
                self.progress_info['current'] = info['current']
                self.progress_info['total'] = info['total']
                self.progress_info['current_industry'] = info.get('current_industry', '')
                self.progress_info['completed'] = info['completed']
                self.progress_info['browser_restart'] = info['browser_restart_count']

            self.progress_callback = callback

            # 调用父类方法
            return await super().crawl_industries_batch(restart_interval)

    async def run_with_progress():
        nonlocal existing_dir

        retry_count = 0
        max_retries = 10
        retry_delay = 30

        while retry_count < max_retries:
            retry_count += 1

            print(f"\n   第 {retry_count}/{max_retries} 次运行...")

            crawler = ProgressCrawler(output_dir=existing_dir, user_config=user_config)
            if existing_dir is None:
                existing_dir = crawler.output_dir

            progress_info['running'] = True

            try:
                success = await crawler.run()
                progress_info['running'] = False

                if success:
                    print("\n\n   ✅ 爬取成功完成!")
                    return True
                else:
                    print(f"\n\n   ❌ 运行失败,将在 {retry_delay} 秒后重试...")
                    if retry_count < max_retries:
                        time.sleep(retry_delay)
            except Exception as e:
                progress_info['running'] = False
                print(f"\n\n   ❌ 程序出错: {e}")
                import traceback
                traceback.print_exc()
                return False

        print(f"\n\n   ❌ 已达到最大重试次数 ({max_retries}),停止运行")
        return False

    # 启动进度显示线程
    progress_thread = threading.Thread(target=progress_printer, daemon=True)
    progress_thread.start()

    # 运行爬虫
    result = asyncio.run(run_with_progress())

    progress_info['running'] = False
    time.sleep(1)
    print()  # 换行


def run_converter():
    """运行表格转换 - 交互式引导"""
    from table_converter import TableConverter

    print("\n" + "=" * 60)
    print("【表格格式转换】")
    print("=" * 60)
    print(f"\n📂 默认数据目录: {DEFAULT_DATA_DIR}")

    # 列出数据目录中的Excel文件
    excel_files = []
    if os.path.exists(DEFAULT_DATA_DIR):
        # 遍历子目录找xlsx文件
        for item in os.listdir(DEFAULT_DATA_DIR):
            item_path = os.path.join(DEFAULT_DATA_DIR, item)
            if os.path.isdir(item_path):
                for f in os.listdir(item_path):
                    if f.endswith('.xlsx'):
                        excel_files.append(os.path.join(item, f))

    if not excel_files:
        print(f"\n   ❌ 数据目录为空: {DEFAULT_DATA_DIR}")
        print("   💡 请先运行爬虫采集数据")
        return

    print(f"\n   找到 {len(excel_files)} 个数据文件:")
    for i, f in enumerate(excel_files, 1):
        print(f"   {i}. {os.path.basename(f)}")

    print(f"\n   请选择要转换的文件编号 (1-{len(excel_files)})")
    print("   或者直接输入文件路径:")

    try:
        choice = input("   > ").strip()

        if choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(excel_files):
                input_file = os.path.join(DEFAULT_DATA_DIR, excel_files[idx])
            else:
                print("   ❌ 无效的选择")
                return
        else:
            input_file = choice if os.path.exists(choice) else os.path.join(DEFAULT_DATA_DIR, choice)

        if not os.path.exists(input_file):
            print(f"   ❌ 文件不存在: {input_file}")
            return

        # 从文件名推断城市名
        base_name = os.path.basename(input_file)
        city_name = "重庆市"
        for keyword in ["湖南", "长沙", "重庆", "四川", "贵州"]:
            if keyword in base_name:
                city_name = keyword
                break

        # 允许用户确认/修改城市名
        print(f"\n   当前城市名: {city_name}")
        city_input = input("   回车确认，或输入新的城市名: ").strip()
        if city_input:
            city_name = city_input

        # 输出文件名
        output_name = base_name.replace('.xlsx', '_转换结果.xlsx')
        output_file = os.path.join(DEFAULT_OUTPUT_DIR, output_name)
        os.makedirs(DEFAULT_OUTPUT_DIR, exist_ok=True)

        print("\n" + "-" * 40)
        print("【配置确认】")
        print(f"   输入文件: {input_file}")
        print(f"   输出文件: {output_file}")
        print(f"   城市名称: {city_name}")
        print("-" * 40)

        confirm = input("\n   确认开始转换? (Y/n): ").strip().lower()
        if confirm == 'n':
            print("   已取消")
            return

        # 运行转换
        print("\n   正在转换...")
        converter = TableConverter(city_name=city_name)

        if not converter.load_from_excel(input_file):
            print("   ❌ 加载数据失败")
            return

        validation = converter.validate()
        print(f"\n   【数据验证】")
        print(f"   行业数: {validation['industries']}")
        print(f"   区县数: {validation['districts']}")
        print(f"   全市合计: {validation['city_total']:,}")

        if validation['warnings']:
            print("   ⚠️ 警告:")
            for w in validation['warnings']:
                print(f"      - {w}")

        if not converter.convert(output_file):
            print("   ❌ 转换失败")
            return

        print(f"\n   ✅ 转换完成!")
        print(f"   📁 文件已保存: {output_file}")

        # 询问是否打开
        open_file = input("   是否打开文件? (Y/n): ").strip().lower()
        if open_file != 'n':
            os.system(f'open "{output_file}"')

    except KeyboardInterrupt:
        print("\n\n   已取消")
    except Exception as e:
        print(f"\n   ❌ 错误: {e}")
        import traceback
        traceback.print_exc()


def run_verification():
    """运行数据核实"""
    from data_verification import verify_data

    print("\n" + "=" * 60)
    print("【数据完整性核实】")
    print("=" * 60)
    print(f"\n📂 数据目录: {DEFAULT_DATA_DIR}")

    try:
        verify_data(DEFAULT_DATA_DIR)
    except KeyboardInterrupt:
        print("\n\n   已取消")
    except Exception as e:
        print(f"\n   ❌ 错误: {e}")
        import traceback
        traceback.print_exc()


def open_data_dir():
    """打开数据目录"""
    path = DEFAULT_OUTPUT_DIR if os.path.exists(DEFAULT_OUTPUT_DIR) else DEFAULT_DATA_DIR
    print(f"\n📁 打开目录: {path}")
    os.system(f'open "{path}"')


def main():
    """主入口"""
    print_banner()

    while True:
        print_menu()
        choice = input("【请输入选项】> ").strip()

        if choice == '0':
            asyncio.run(save_cookie())
        elif choice == '1':
            run_crawler()
        elif choice == '2':
            run_converter()
        elif choice == '3':
            run_verification()
        elif choice == '4':
            open_data_dir()
        elif choice == '5':
            print("\n再见! 👋\n")
            break
        else:
            print("\n   ❌ 无效选项，请输入 0-5")


if __name__ == "__main__":
    main()
