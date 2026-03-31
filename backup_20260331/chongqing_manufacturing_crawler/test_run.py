#!/usr/bin/env python3
"""直接运行爬虫（带自动重试）"""
import asyncio
import sys
sys.path.insert(0, '/Users/luodong/trae/02-Projects/企查查行业搜索/chongqing_manufacturing_crawler')

from crawler_changsha import run_with_retry

async def main():
    print("=" * 60)
    print("长沙市制造业企业数据爬虫 - 自动重试模式")
    print("=" * 60)
    print()
    print("功能说明:")
    print("  - 每次爬取保存到独立的时间戳目录")
    print("  - 意外退出后自动重启")
    print("  - 可疑数据自动标记")
    print("  - 最大重试次数: 10次")
    print("  - 重试间隔: 30秒")
    print()
    
    success = await run_with_retry(max_retries=10, retry_delay=30)
    
    if success:
        print("\n任务完成!")
    else:
        print("\n任务失败,请检查日志")

if __name__ == "__main__":
    asyncio.run(main())
