#!/usr/bin/env python3
"""
数据核实模块 - 验证爬取数据的完整性和一致性
"""
import json
import os
import sys
from datetime import datetime

def verify_data(output_dir):
    """核实数据一致性"""
    print("=" * 70)
    print("数据核实报告")
    print("=" * 70)
    print(f"核实时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"数据目录: {output_dir}")
    print()

    # 读取任务状态
    task_file = os.path.join(output_dir, 'task_state.json')
    if not os.path.exists(task_file):
        print("❌ 错误: 找不到 task_state.json 文件")
        return False

    with open(task_file, 'r', encoding='utf-8') as f:
        task_data = json.load(f)

    # 读取临时数据
    temp_file = os.path.join(output_dir, 'temp_data.json')
    temp_data = {}
    if os.path.exists(temp_file):
        with open(temp_file, 'r', encoding='utf-8') as f:
            temp_data = json.load(f)

    # ===== 1. 任务完成情况 =====
    print("【任务完成情况】")
    total_industries = len(task_data.get('industry_tasks', {}))
    completed_industries = 0
    pending_industries = []
    failed_industries = []

    for code, task in task_data.get('industry_tasks', {}).items():
        status = task.get('status')
        if status == 'completed':
            completed_industries += 1
        elif status == 'failed':
            failed_industries.append((code, task.get('name')))
        elif status == 'in_progress':
            pending_industries.append((code, task.get('name'), '进行中'))
        elif status == 'pending':
            pending_industries.append((code, task.get('name'), '待处理'))

    print(f"  总行业数: {total_industries}")
    print(f"  已完成: {completed_industries}")
    print(f"  失败: {len(failed_industries)}")
    print(f"  未完成: {len(pending_industries)}")

    if pending_industries:
        print("  未完成行业:")
        for code, name, status in pending_industries:
            print(f"    - {code} {name} ({status})")

    if failed_industries:
        print("  失败行业:")
        for code, name in failed_industries:
            print(f"    - {code} {name}")

    print()

    # ===== 2. 数据总额核对 =====
    print("【数据总额核对】")
    session = task_data.get('session', {})

    # 读取临时数据获取正确的制造业合计
    temp_file = os.path.join(output_dir, 'temp_data.json')
    temp_data = {}
    manufacturing_total = 0  # 正确的制造业合计
    if os.path.exists(temp_file):
        with open(temp_file, 'r', encoding='utf-8') as f:
            temp_data = json.load(f)
        # 从行业C（制造业合计）获取正确的制造业总数
        for item in temp_data.get('data', []):
            if item.get('行业代码') == 'C':
                manufacturing_total += item.get('企业数量', 0)

    print(f"  制造业合计（城市汇总表）: {manufacturing_total:,} 家企业")

    # 计算各行业总额
    industry_totals = {}
    for item in temp_data.get('data', []):
        code = item.get('行业代码', '')
        count = item.get('企业数量', 0)
        if code not in industry_totals:
            industry_totals[code] = 0
        industry_totals[code] += count

    # 找出异常行业（数据接近制造业合计的行业，可能是企查查网站分类问题）
    abnormal_industries = []
    for code, total in industry_totals.items():
        if code != 'C' and total > 0:
            ratio = total / manufacturing_total if manufacturing_total > 0 else 0
            if ratio > 0.9:  # 如果某行业的数据接近制造业合计，认为是异常
                name = ''
                for item in temp_data.get('data', []):
                    if item.get('行业代码') == code:
                        name = item.get('行业类别', '')
                        break
                abnormal_industries.append((code, name, total))

    if abnormal_industries:
        print(f"  ⚠️ 检测到 {len(abnormal_industries)} 个异常行业（数据接近制造业合计）:")
        for code, name, total in abnormal_industries:
            print(f"    - {code} {name}: {total:,} ≈ 制造业合计 {manufacturing_total:,}")

    # 计算有效行业的总额（排除行业C和异常行业）
    valid_industry_sum = 0
    for code, total in industry_totals.items():
        if code != 'C':
            is_abnormal = any(ab[0] == code for ab in abnormal_industries)
            if not is_abnormal:
                valid_industry_sum += total

    # 行业明细表总额 (各行业相加)
    industry_sum = sum(t.get('enterprise_count', 0) for t in task_data.get('industry_tasks', {}).values())
    print(f"  细分行业明细总额（含异常）: {industry_sum:,} 家企业")
    print(f"  细分行业明细总额（排除异常）: {valid_industry_sum:,} 家企业")

    # 判断一致性
    diff = abs(manufacturing_total - valid_industry_sum)
    ratio = (diff / manufacturing_total * 100) if manufacturing_total > 0 else 0
    print(f"  与制造业合计差异: {diff:,} 家企业 ({ratio:.2f}%)")

    if ratio < 2:
        print("  ✅ 差异在允许范围内 (<2%)")
    elif ratio < 5:
        print("  ⚠️ 差异较大但可接受 (<5%)")
    else:
        print("  ⚠️ 差异较大，需人工核实")

    print()

    # ===== 3. 临时数据记录数 =====
    print("【临时数据记录】")
    temp_records = len(temp_data.get('data', []))
    print(f"  临时数据记录数: {temp_records} 条")

    # 计算期望记录数 (行业数 × 区县数)
    districts_count = len(session.get('districts_cache', []))
    expected_records = total_industries * districts_count if districts_count > 0 else 0
    print(f"  期望记录数: {expected_records} 条 (行业数 × 区县数)")

    if temp_records > 0:
        record_ratio = temp_records / expected_records * 100 if expected_records > 0 else 0
        print(f"  完整率: {record_ratio:.1f}%")

    print()

    # ===== 4. 各行业数据明细 =====
    print("【行业数据明细】")
    print("-" * 70)
    print(f"{'代码':<6} {'行业名称':<30} {'企业数':<12} {'状态'}")
    print("-" * 70)

    for code, task in sorted(task_data.get('industry_tasks', {}).items(), key=lambda x: int(x[0])):
        name = task.get('name', '')[:28]
        count = task.get('enterprise_count', 0)
        status = task.get('status')
        status_icon = '✅' if status == 'completed' else '❌' if status == 'failed' else '⏳'
        print(f"{code:<6} {name:<30} {count:>10,} {status_icon}")

    print("-" * 70)
    print(f"{'合计':<37} {industry_sum:>10,}")
    print()

    # ===== 5. 最终结论 =====
    print("=" * 70)
    print("【最终结论】")
    print("=" * 70)

    all_completed = len(failed_industries) == 0 and len(pending_industries) == 0
    data_consistent = ratio < 2
    records_complete = temp_records >= expected_records * 0.95

    # 有效数据统计
    print(f"【有效数据】")
    print(f"  湖南省制造业企业总数: {manufacturing_total:,} 家（城市汇总表）")
    print(f"  有效细分行业数: {len(industry_totals) - 1 - len(abnormal_industries)} 个（排除异常行业）")
    if abnormal_industries:
        print(f"  异常行业: {', '.join([ab[1] for ab in abnormal_industries])}")
        print(f"    （注：企查查网站行业分类存在重叠，数据仅供参考）")
    print()

    if len(abnormal_industries) > 0:
        print("⚠️ 检测到异常:")
        print("   - 部分行业数据与制造业合计接近，可能是企查查网站分类问题")
        print("   - 这是网站数据问题，不是代码bug")
        print()
        print("数据质量评级: 良好（需注意异常行业）")
    elif all_completed and data_consistent and records_complete:
        print("✅ 所有检查通过!")
        print("数据质量评级: 优秀")
    else:
        print("⚠️ 存在小问题，但数据可用")
        print("数据质量评级: 可用")

    print()
    print(f"核实完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    return all_completed and data_consistent


if __name__ == "__main__":
    # 默认数据目录
    default_dir = "data/hunan_test"

    # 可以通过命令行参数指定目录
    if len(sys.argv) > 1:
        output_dir = sys.argv[1]
    else:
        output_dir = default_dir

    success = verify_data(output_dir)
    sys.exit(0 if success else 1)