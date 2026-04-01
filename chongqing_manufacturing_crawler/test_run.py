#!/usr/bin/env python3
"""企查查行业搜索爬虫 - 自动重试模式"""
import asyncio
import sys
sys.path.insert(0, '/Users/luodong/trae/02-Projects/企查查行业搜索/chongqing_manufacturing_crawler')

from crawler_changsha import run_with_retry

async def main():
    success = await run_with_retry(max_retries=10, retry_delay=30)

    if success:
        print("\n任务完成!")
    else:
        print("\n任务失败,请检查日志")

if __name__ == "__main__":
    asyncio.run(main())
