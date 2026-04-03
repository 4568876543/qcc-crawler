#!/usr/bin/env python3
"""
企查查爬虫 - 登录模块
支持命令行参数和配置文件，无需交互式输入

运行方式:
    python login.py                     # 交互式登录
    python login.py --auto              # 自动等待60秒后保存
    python login.py --check             # 仅检查登录状态
    python login.py --url https://...  # 登录指定URL
"""
import asyncio
import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
CONFIG_FILE = PROJECT_ROOT / "config.json"
BROWSER_PROFILE = PROJECT_ROOT / "data" / "browser_profile"


def print_banner():
    print("\n" + "=" * 60)
    print("    企查查爬虫 - 登录模块")
    print("=" * 60)


async def check_login_status(page):
    """检查页面登录状态"""
    try:
        text = await page.inner_text('body')
        if '登录 | 注册' in text or '登录/注册' in text:
            return False, "未登录"
        elif '退出' in text:
            return True, "已登录"
        else:
            return None, "状态未知"
    except:
        return None, "检查失败"


async def save_cookie_cli(auto_save=False, url="https://www.qcc.com"):
    """保存Cookie - 支持CLI无交互"""
    from playwright.async_api import async_playwright

    print("\n🔐 开始登录流程...")
    print(f"   目标URL: {url}")
    print(f"   Cookie目录: {BROWSER_PROFILE}")
    print(f"   自动保存模式: {'开启' if auto_save else '关闭'}")

    if not auto_save:
        print("\n   💡 提示: 将打开浏览器，请在界面中扫码登录")
        print("   📱 登录成功后，Cookie自动保存到本地")

    context = None
    try:
        async with async_playwright() as p:
            # 确保目录存在
            os.makedirs(BROWSER_PROFILE, exist_ok=True)

            print("\n   正在启动浏览器...")
            context = await p.chromium.launch_persistent_context(
                str(BROWSER_PROFILE),
                headless=False,
                slow_mo=50,
                viewport={'width': 1280, 'height': 800}
            )
            page = context.pages[0] if context.pages else await context.new_page()

            print("   正在打开网站...")
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(2000)

            # 检查当前登录状态
            is_logged_in, status_msg = await check_login_status(page)
            print(f"   当前状态: {status_msg}")

            if is_logged_in:
                print("\n   ✅ 已检测到登录状态，Cookie已生效")
                await context.close()
                return True

            # 触发登录界面
            print("\n   正在触发登录界面...")
            login_triggered = False

            selectors = [
                "a[data-litbcon='登录']",
                ".login-btn",
                ".header-login-btn",
                "a[href*='login']",
            ]

            for selector in selectors:
                try:
                    elements = await page.query_selector_all(selector)
                    for el in elements:
                        if await el.is_visible():
                            await el.click(timeout=2000)
                            print(f"   ✅ 点击登录按钮: {selector}")
                            login_triggered = True
                            await page.wait_for_timeout(1500)
                            break
                except:
                    continue

            if not login_triggered:
                await page.evaluate('''() => {
                    document.querySelectorAll('.login-btn, .sign-in, a, button').forEach(el => {
                        if (el.textContent.includes('登录') && el.offsetParent !== null) {
                            el.click();
                        }
                    });
                }''')
                await page.wait_for_timeout(1500)

            if auto_save:
                # 自动模式：等待60秒
                print("\n   ⏳ 自动模式：等待60秒供扫码...")
                await page.wait_for_timeout(60000)
            else:
                # 交互模式：等待用户按回车
                print("\n   📱 请在浏览器中扫码登录")
                print("   ✅ 登录成功后，按回车保存Cookie (或 Ctrl+C 取消)")
                try:
                    input()
                except EOFError:
                    print("   ⏳ 非交互环境，自动等待60秒...")
                    await page.wait_for_timeout(60000)

            # 验证登录状态
            print("\n   🔍 验证登录状态...")
            await page.goto(url, wait_until="domcontentloaded")
            await page.wait_for_timeout(2000)

            is_logged_in, status_msg = await check_login_status(page)
            print(f"   当前状态: {status_msg}")

            if is_logged_in:
                print("\n   ✅ 登录成功，Cookie已保存")
            else:
                print("\n   ⚠️ 未检测到登录状态，Cookie可能未保存成功")

            await context.close()
            return is_logged_in

    except KeyboardInterrupt:
        print("\n\n   已取消登录")
        if context:
            await context.close()
        return False
    except Exception as e:
        print(f"\n   ❌ 登录出错: {e}")
        if context:
            await context.close()
        return False


async def check_cookie_status():
    """检查Cookie文件状态"""
    print("\n🔍 检查Cookie状态...")

    if not BROWSER_PROFILE.exists():
        print(f"   ❌ Cookie目录不存在: {BROWSER_PROFILE}")
        return False

    # 检查Cookies文件
    default_dir = BROWSER_PROFILE / "Default"
    cookies_file = None

    if default_dir.exists():
        cookies_file = default_dir / "Cookies"
    elif (BROWSER_PROFILE / "Cookies").exists():
        cookies_file = BROWSER_PROFILE / "Cookies"

    if cookies_file and cookies_file.exists():
        size = cookies_file.stat().st_size
        print(f"   ✅ Cookie文件存在: {cookies_file}")
        print(f"   📊 文件大小: {size} bytes")
        return size > 0
    else:
        print(f"   ❌ Cookie文件不存在或为空")
        return False


def main():
    parser = argparse.ArgumentParser(description='企查查爬虫 - 登录模块')
    parser.add_argument('--auto', action='store_true', help='自动模式：等待60秒后自动保存')
    parser.add_argument('--check', action='store_true', help='仅检查Cookie状态')
    parser.add_argument('--url', default='https://www.qcc.com', help='登录URL')

    args = parser.parse_args()

    print_banner()

    # 检查模式
    if args.check:
        result = asyncio.run(check_cookie_status())
        return 0 if result else 1

    # 登录模式
    result = asyncio.run(save_cookie_cli(auto_save=args.auto, url=args.url))

    print("\n" + "=" * 60)
    if result:
        print("✅ 登录完成")
    else:
        print("❌ 登录失败")
    print("=" * 60)

    return 0 if result else 1


if __name__ == "__main__":
    sys.exit(main())
