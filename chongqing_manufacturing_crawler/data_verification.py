#!/usr/bin/env python3
"""
数据核实模块 - 验证爬取数据的完整性和一致性
核对：把31个行业的本行业企业数按区县加总，是否等于企查查公布的该区县制造业总数
"""
import os
import sys
from datetime import datetime
import pandas as pd


def verify_data(output_dir):
    """核实数据一致性"""
    print("=" * 70)
    print("【数据完整性核实】")
    print("=" * 70)
    print(f"核实时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"数据目录: {output_dir}")
    print()

    # 查找行业明细表文件夹
    industry_dir = os.path.join(output_dir, '行业明细表')
    if not os.path.exists(industry_dir):
        print("❌ 错误: 找不到 '行业明细表' 文件夹")
        return False

    industry_files = [f for f in os.listdir(industry_dir) if f.endswith('.xlsx') and not f.startswith('00_')]
    if not industry_files:
        print("❌ 错误: 行业明细表文件夹为空")
        return False

    print(f"📁 行业明细表: {len(industry_files)} 个文件")
    print()

    # ===== 读取所有行业明细表 =====
    print("【正在读取行业明细表...】")

    # 按区县汇总本行业企业数
    district_industry_sum = {}  # {区县: 所有行业的本行业企业数之和}

    for f in industry_files:
        file_path = os.path.join(industry_dir, f)

        try:
            df = pd.read_excel(file_path, sheet_name='区县明细', header=3)

            for idx, row in df.iterrows():
                district = str(row.get('区县', '')).strip()
                if not district or district == 'nan' or district == '合计':
                    continue
                if '区' in district or '县' in district or '市' in district:
                    try:
                        ind_count = int(row.get('本行业企业数', 0))
                        district_industry_sum[district] = district_industry_sum.get(district, 0) + ind_count
                    except (ValueError, TypeError):
                        pass

        except Exception as e:
            print(f"   ⚠️ 读取 {f} 出错: {e}")
            continue

    print(f"   读取完成，发现 {len(district_industry_sum)} 个区县")
    print()

    # ===== 显示区县汇总结果 =====
    print("=" * 70)
    print("【各区县企业数量核实】")
    print("=" * 70)
    print(f"{'区县':<12} {'行业明细汇总':<15} {'企查查公布'}{'差异':<10} {'状态'}")
    print("-" * 70)

    sorted_districts = sorted(district_industry_sum.items(), key=lambda x: x[1], reverse=True)
    total_from_industry = 0

    # 企查查公布的制造业总数
    qcc_total_by_district = {}

    # 优先从00_汇总文件读取（如果存在）
    summary_file = os.path.join(industry_dir, '00_湖南省市制造业总表.xlsx')
    if os.path.exists(summary_file):
        try:
            summary_df = pd.read_excel(summary_file, sheet_name='区县汇总', header=3)
            for idx, row in summary_df.iterrows():
                district = str(row.get('区县', '')).strip()
                if district and district != 'nan' and district != '合计':
                    if '区' in district or '县' in district or '市' in district:
                        try:
                            qcc_total = int(row.get('制造业企业总数', 0))
                            if qcc_total > 0:
                                qcc_total_by_district[district] = qcc_total
                        except:
                            pass
            print(f"   ✅ 已从汇总文件读取企查查公布数据")
        except Exception as e:
            print(f"   ⚠️ 汇总文件读取失败，将从行业明细表读取: {e}")
            # 回退到从行业文件读取
            for f in industry_files[:3]:
                file_path = os.path.join(industry_dir, f)
                try:
                    df = pd.read_excel(file_path, sheet_name='区县明细', header=3)
                    for idx, row in df.iterrows():
                        district = str(row.get('区县', '')).strip()
                        if district and district != 'nan' and district != '合计':
                            if '区' in district or '县' in district or '市' in district:
                                try:
                                    qcc_total = int(row.get('该区县制造业总数', 0))
                                    if district not in qcc_total_by_district:
                                        qcc_total_by_district[district] = qcc_total
                                except:
                                    pass
                except:
                    pass
    else:
        # 从行业文件读取
        for f in industry_files[:3]:
            file_path = os.path.join(industry_dir, f)
            try:
                df = pd.read_excel(file_path, sheet_name='区县明细', header=3)
                for idx, row in df.iterrows():
                    district = str(row.get('区县', '')).strip()
                    if district and district != 'nan' and district != '合计':
                        if '区' in district or '县' in district or '市' in district:
                            try:
                                qcc_total = int(row.get('该区县制造业总数', 0))
                                if district not in qcc_total_by_district:
                                    qcc_total_by_district[district] = qcc_total
                            except:
                                pass
            except:
                pass

    # 计算差异
    for district, industry_sum in sorted_districts:
        qcc_total = qcc_total_by_district.get(district, 0)
        diff = abs(industry_sum - qcc_total)
        diff_ratio = (diff / qcc_total * 100) if qcc_total > 0 else 0

        total_from_industry += industry_sum

        # 标记
        marker = ""
        if qcc_total == 0:
            marker = " ⚠️无对照数据"
        elif diff_ratio < 1:
            marker = " ✅"
        elif diff_ratio < 5:
            marker = " ⚠️"
        else:
            marker = " ❌"

        print(f"{district:<12} {industry_sum:>12,} {qcc_total:>12,} {diff:>8,} {diff_ratio:>5.1f}%{marker}")

    print("-" * 70)
    print(f"{'合计':<12} {total_from_industry:>12,}")
    print()

    # ===== 数据质量评估 =====
    print("=" * 70)
    print("【数据质量评估】")
    print("=" * 70)

    # 计算总体差异
    total_qcc = sum(qcc_total_by_district.values())
    total_diff = abs(total_from_industry - total_qcc)
    total_ratio = (total_diff / total_qcc * 100) if total_qcc > 0 else 0

    print(f"  行业明细汇总总数: {total_from_industry:,}")
    print(f"  企查查公布总数:   {total_qcc:,}")
    print(f"  差异:           {total_diff:,} ({total_ratio:.2f}%)")

    if total_ratio < 1:
        print("  ✅ 数据一致性优秀")
    elif total_ratio < 5:
        print("  ✅ 数据一致性良好")
    elif total_ratio < 10:
        print("  ⚠️ 数据基本一致")
    else:
        print("  ❌ 数据存在较大差异")

    # 额外分析：如果差异接近100%，说明数据可能被重复计数了
    if 90 < total_ratio < 110:
        print("  ⚠️ 提示: 差异接近100%，数据可能存在重复计数问题")

    if len(district_industry_sum) >= 10:
        print(f"  ✅ 区县覆盖完整 ({len(district_industry_sum)} 个区县)")
    elif len(district_industry_sum) >= 5:
        print(f"  ⚠️ 区县覆盖较少 ({len(district_industry_sum)} 个区县)")
    else:
        print(f"  ❌ 区县覆盖严重不足 ({len(district_industry_sum)} 个区县)")

    print()
    print(f"核实完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    return total_ratio < 10


if __name__ == "__main__":
    default_dir = "data/hunan_test"

    if len(sys.argv) > 1:
        output_dir = sys.argv[1]
    else:
        output_dir = default_dir

    success = verify_data(output_dir)
    sys.exit(0 if success else 1)
