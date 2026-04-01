#!/usr/bin/env python3
"""
表格转换命令行工具
将爬取的明细数据转换为目标格式

用法:
    python -m table_converter.cli --input data/hunan_test/湖南省制造业企业数量明细表.xlsx --output output/result.xlsx
"""
import argparse
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from table_converter import TableConverter


def main():
    parser = argparse.ArgumentParser(
        description='将爬取数据转换为目标格式（Sheet1横向宽表 + Sheet2行业排名表）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
示例:
    python -m table_converter.cli -i data/hunan_test/湖南省制造业企业数量明细表.xlsx -o output/result.xlsx
    python -m table_converter.cli --input data/hunan_test/湖南省制造业企业数量明细表.xlsx --output output/result.xlsx --city 湖南省
        '''
    )

    parser.add_argument('--input', '-i', required=True, help='输入文件路径（爬取的明细数据Excel）')
    parser.add_argument('--output', '-o', required=True, help='输出文件路径')
    parser.add_argument('--city', '-c', default='重庆市', help='城市名称（默认：重庆市）')
    parser.add_argument('--top-n', '-n', type=int, default=10, help='Sheet2每个行业取前N名（默认：10）')
    parser.add_argument('--verbose', '-v', action='store_true', help='显示详细信息')

    args = parser.parse_args()

    # 检查输入文件
    if not os.path.exists(args.input):
        print(f"[错误] 输入文件不存在: {args.input}")
        sys.exit(1)

    # 创建输出目录
    output_dir = os.path.dirname(args.output)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    print("=" * 60)
    print("表格转换工具")
    print("=" * 60)
    print(f"输入文件: {args.input}")
    print(f"输出文件: {args.output}")
    print(f"城市名称: {args.city}")
    print(f"行业排名: 前{args.top_n}名")
    print("-" * 60)

    # 创建转换器
    converter = TableConverter(city_name=args.city)

    # 设置 Sheet2 的 top_n
    converter.sheet2_generator.top_n = args.top_n

    # 加载数据
    if not converter.load_from_excel(args.input):
        sys.exit(1)

    # 验证
    validation = converter.validate()
    print(f"\n[验证结果]")
    print(f"  行业数: {validation['industries']}")
    print(f"  区县数: {validation['districts']}")
    print(f"  全市合计: {validation['city_total']:,}")

    if validation['warnings']:
        print(f"  警告:")
        for warning in validation['warnings']:
            print(f"    - {warning}")

    # 转换
    print("\n[转换中...]")
    if not converter.convert(args.output):
        sys.exit(1)

    print("\n" + "=" * 60)
    print("转换完成！")
    print("=" * 60)


if __name__ == "__main__":
    main()
