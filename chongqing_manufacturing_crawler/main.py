#!/usr/bin/env python3
"""
企查查爬虫 - 配置文件驱动CLI入口
支持命令行参数和配置文件，无需交互式输入

运行方式:
    python main.py                                    # 使用config.json配置
    python main.py 湖南省                            # 命令行指定关键词
    python main.py --keyword 广东省 --mode resume   # 命令行指定所有参数
    python main.py --help                            # 查看帮助

配置文件: config.json
"""
import asyncio
import json
import os
import sys
import argparse
from pathlib import Path

# 项目根目录
PROJECT_ROOT = Path(__file__).parent
CONFIG_FILE = PROJECT_ROOT / "config.json"
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "output"


def print_banner():
    """打印横幅"""
    print("\n" + "=" * 60)
    print("    企查查行业搜索爬虫 - 配置文件驱动版 v1.0")
    print("=" * 60)


def print_usage():
    """打印使用说明"""
    print("""
【使用说明】

1. 快速启动（使用config.json配置）:
   python main.py

2. 命令行指定关键词（优先于配置文件）:
   python main.py 湖南省

3. 命令行指定所有参数:
   python main.py --keyword 湖南省 --industry 制造业 --status 存续（在业）

4. 仅运行爬虫:
   python main.py --crawler

5. 仅运行转换器:
   python main.py --converter

6. 查看帮助:
   python main.py --help

【登录模块 - Cookie管理】

首次运行需要登录:
   python login.py              # 交互式登录（推荐）
   python login.py --auto      # 自动等待60秒后保存
   python login.py --check     # 检查Cookie状态

其他命令:
   python cli.py               # 旧版CLI（保留）

【config.json 配置说明】

{
    "crawler": {
        "enabled": true,
        "search_box_keyword": "湖南省",   // 搜索框关键词
        "省份地区": "湖南省",             // 下拉框选择的地址
        "district_level": "city",          // city(市级)/province(省级)
        "国标行业": "制造业",              // 国标行业
        "company_status": "存续（在业）",  // 登记状态
        "mode": "new"                     // new(全新)/resume(继续)
    }
}

【常见问题】

Q: Cookie过期了怎么办？
A: 运行 python login.py 重新扫码登录

Q: 如何切换到其他省份？
A: python main.py 广东省  # 命令行指定
   或修改 config.json 中的 search_box_keyword

Q: 如何继续上次中断的爬取？
A: python main.py --mode resume
""")


def check_cookie_status():
    """检查Cookie状态 - 通过检测浏览器配置目录"""
    cookie_path = PROJECT_ROOT / "data" / "browser_profile"
    if not cookie_path.exists():
        return False
    # 检查是否有Chromium的Cookies文件
    default_path = cookie_path / "Default"
    if default_path.exists():
        cookies_file = default_path / "Cookies"
        if cookies_file.exists() and cookies_file.stat().st_size > 0:
            return True
    # 或者检查根目录的Cookies
    cookies_file = cookie_path / "Cookies"
    if cookies_file.exists() and cookies_file.stat().st_size > 0:
        return True
    return False


def load_config(args):
    """加载配置文件，命令行参数优先"""
    # 默认配置
    default_config = {
        "login": {
            "cookie_saved": False,
            "cookie_path": "data/browser_profile"
        },
        "crawler": {
            "enabled": True,
            "search_box_keyword": "湖南省",
            "省份地区": "湖南省",
            "district_level": "city",
            "industry_keyword": "制造业",
            "国标行业": "制造业",
            "company_status": "存续（在业）",
            "mode": "new",
            "open_browser": False
        },
        "converter": {
            "enabled": False
        },
        "verification": {
            "enabled": False
        }
    }

    # 从配置文件加载
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)
    else:
        print(f"⚠️ 配置文件不存在，使用默认配置: {CONFIG_FILE}")
        config = default_config

    # 合并crawler配置
    crawler_config = config.get('crawler', {})

    # 命令行参数优先 - 位置参数和--keyword选项效果相同
    effective_keyword = args.keyword or args.keyword_opt
    if effective_keyword:
        crawler_config['search_box_keyword'] = effective_keyword
        crawler_config['省份地区'] = effective_keyword  # 同时设置两者

    if args.industry:
        crawler_config['industry_keyword'] = args.industry
        crawler_config['国标行业'] = args.industry

    if args.status:
        crawler_config['company_status'] = args.status

    if args.province:
        crawler_config['省份地区'] = args.province

    if args.district_level:
        crawler_config['district_level'] = args.district_level

    if args.mode:
        crawler_config['mode'] = args.mode

    config['crawler'] = crawler_config

    # 自动检测Cookie状态
    cookie_saved = check_cookie_status()
    config['login'] = {
        'cookie_saved': cookie_saved,
        'cookie_path': str(PROJECT_ROOT / "data" / "browser_profile")
    }

    return config


def print_config(config):
    """打印配置"""
    crawler = config.get('crawler', {})
    login = config.get('login', {})

    print("\n【当前配置】")
    print(f"   搜索框关键词: {crawler.get('search_box_keyword', '湖南省')}")
    print(f"   省份地区: {crawler.get('省份地区', '湖南省')}")
    print(f"   搜索层级: {'省级' if crawler.get('district_level') == 'province' else '市级'}")
    print(f"   国标行业: {crawler.get('国标行业', '制造业')}")
    print(f"   登记状态: {crawler.get('company_status', '存续（在业）')}")
    print(f"   运行模式: {'全新' if crawler.get('mode') == 'new' else '继续上次'}")
    print(f"   Cookie状态: {'✅ 已保存' if login.get('cookie_saved') else '❌ 未保存'}")

    if not login.get('cookie_saved'):
        print("\n   ⚠️ 警告: Cookie未保存，爬虫可能无法正常运行！")
        print("   💡 请先运行 python cli.py 选择'0'登录")


def run_crawler(config):
    """运行爬虫"""
    from crawler_changsha import ChangshaCrawler

    crawler_config = config.get('crawler', {})
    if not crawler_config.get('enabled', False):
        print("   爬虫功能未启用")
        return True

    # 检查Cookie
    cookie_saved = config.get('login', {}).get('cookie_saved', False)
    if not cookie_saved:
        print("   ❌ Cookie未保存，无法运行爬虫")
        print("   💡 请先运行以下命令登录:")
        print("      python login.py              # 交互式登录")
        print("      python login.py --auto      # 自动等待60秒")
        print("      python login.py --check     # 检查Cookie状态")
        return False

    # 读取配置
    search_box_keyword = crawler_config.get('search_box_keyword', '湖南省')
    search_location = crawler_config.get('省份地区', '湖南省')
    district_level = crawler_config.get('district_level', 'city')
    company_status = crawler_config.get('company_status', '存续（在业）')
    industry = crawler_config.get('国标行业', '制造业')
    mode = crawler_config.get('mode', 'new')

    # 构建用户配置
    user_config = {
        "keyword": search_box_keyword,
        "search_location": search_location,
        "district_level": district_level,
        "company_status": company_status,
        "industry": industry
    }

    print(f"\n🚀 开始爬取: {search_location}")

    # 确定输出目录
    output_dir = None
    if mode == 'resume':
        try:
            from config_changsha import find_latest_incomplete_dir
            existing_dir = find_latest_incomplete_dir()
            if existing_dir:
                output_dir = existing_dir
                print(f"   📁 继续目录: {existing_dir}")
        except:
            pass

    async def run():
        crawler = ChangshaCrawler(output_dir=output_dir, user_config=user_config)
        try:
            return await crawler.run()
        except Exception as e:
            print(f"   ❌ 爬虫运行出错: {e}")
            import traceback
            traceback.print_exc()
            return False

    success = asyncio.run(run())
    return success


def run_converter(config):
    """运行表格转换"""
    from table_converter import TableConverter

    converter_config = config.get('converter', {})
    if not converter_config.get('enabled', False):
        return True

    input_file = converter_config.get('input_file', '')
    city_name = converter_config.get('city_name', '湖南省')

    if not input_file or not os.path.exists(input_file):
        print(f"   ❌ 输入文件不存在: {input_file}")
        return False

    print(f"\n🔄 开始转换: {input_file}")

    base_name = os.path.basename(input_file)
    output_name = base_name.replace('.xlsx', '_转换结果.xlsx')
    output_file = os.path.join(DEFAULT_OUTPUT_DIR, output_name)
    os.makedirs(DEFAULT_OUTPUT_DIR, exist_ok=True)

    converter = TableConverter(city_name=city_name)

    if not converter.load_from_excel(input_file):
        print("   ❌ 加载数据失败")
        return False

    validation = converter.validate()
    print(f"\n   行业数: {validation['industries']}")
    print(f"   区县数: {validation['districts']}")
    print(f"   全市合计: {validation['city_total']:,}")

    if validation.get('warnings'):
        for w in validation['warnings']:
            print(f"   ⚠️ {w}")

    if not converter.convert(output_file):
        print("   ❌ 转换失败")
        return False

    print(f"\n   ✅ 转换完成: {output_file}")
    return True


def run_verification(config):
    """运行数据核实"""
    from data_verification import verify_data

    verification_config = config.get('verification', {})
    if not verification_config.get('enabled', False):
        return True

    data_dir = verification_config.get('data_dir', str(DEFAULT_DATA_DIR))

    print(f"\n🔍 开始核实: {data_dir}")

    try:
        verify_data(data_dir)
        return True
    except Exception as e:
        print(f"   ❌ 核实出错: {e}")
        return False


def main():
    """主入口"""
    parser = argparse.ArgumentParser(
        description='企查查爬虫 - 配置文件驱动版',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python main.py                                    # 使用配置文件的默认配置
  python main.py 广东省                            # 指定搜索关键词
  python main.py --keyword 广东省 --mode resume   # 指定所有参数
  python main.py --crawler                         # 仅运行爬虫
  python main.py --converter                       # 仅运行转换器
  python main.py --help                            # 查看帮助
        """
    )

    parser.add_argument('keyword', nargs='?', default=None, help='搜索关键词（如：湖南省/广东省）')
    parser.add_argument('--keyword', '-k', dest='keyword_opt', help='搜索关键词（与位置参数效果相同）')
    parser.add_argument('--province', '-p', help='省份地区（如：湖南省）')
    parser.add_argument('--industry', '-i', help='国标行业（如：制造业）')
    parser.add_argument('--status', '-s', help='登记状态（如：存续（在业））')
    parser.add_argument('--district-level', '-d', choices=['province', 'city'], help='搜索层级')
    parser.add_argument('--mode', '-m', choices=['new', 'resume'], help='运行模式')
    parser.add_argument('--crawler', action='store_true', help='仅运行爬虫')
    parser.add_argument('--converter', action='store_true', help='仅运行转换器')
    parser.add_argument('--verify', action='store_true', help='仅运行数据核实')
    parser.add_argument('--config', '-c', type=str, help='配置文件路径')
    parser.add_argument('--usage', '-u', action='store_true', help='显示使用说明')
    parser.add_argument('--version', '-v', action='version', version='%(prog)s 1.0')

    args = parser.parse_args()

    # 显示使用说明
    if args.usage:
        print_usage()
        return 0

    print_banner()

    # 使用说明
    print_usage()

    # 加载配置
    config = load_config(args)
    print_config(config)

    print("\n" + "-" * 60)

    success = True

    # 确定运行模式
    run_crawler_flag = args.crawler or (not args.converter and not args.verify)
    run_converter_flag = args.converter
    run_verify_flag = args.verify

    # 运行爬虫
    if run_crawler_flag:
        success = run_crawler(config) and success

    # 运行转换器
    if run_converter_flag:
        success = run_converter(config) and success

    # 运行核实
    if run_verify_flag:
        success = run_verification(config) and success

    # 完成
    print("\n" + "=" * 60)
    if success:
        print("✅ 任务执行完成")
    else:
        print("⚠️ 部分任务执行失败")
    print("=" * 60)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
