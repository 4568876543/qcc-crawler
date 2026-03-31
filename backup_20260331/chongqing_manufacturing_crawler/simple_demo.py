# 简单演示脚本 - 不依赖任何外部库
import os
import csv
import random
import time

class SimpleDemo:
    def __init__(self):
        # 模拟的制造业细分行业列表
        self.manufacturing_subcategories = {
            "13": "农副食品加工业",
            "14": "食品制造业",
            "15": "酒、饮料和精制茶制造业",
            "16": "烟草制品业",
            "17": "纺织业",
            "18": "纺织服装、服饰业",
            "19": "皮革、毛皮、羽毛及其制品和制鞋业",
            "20": "木材加工和木、竹、藤、棕、草制品业",
            "21": "家具制造业",
            "22": "造纸和纸制品业",
            "23": "印刷和记录媒介复制业",
            "24": "文教、工美、体育和娱乐用品制造业",
            "25": "石油加工、炼焦和核燃料加工业",
            "26": "化学原料和化学制品制造业",
            "27": "医药制造业",
            "28": "化学纤维制造业",
            "29": "橡胶和塑料制品业",
            "30": "非金属矿物制品业",
            "31": "黑色金属冶炼和压延加工业",
            "32": "有色金属冶炼和压延加工业",
            "33": "金属制品业",
            "34": "通用设备制造业",
            "35": "专用设备制造业",
            "36": "汽车制造业",
            "37": "铁路、船舶、航空航天和其他运输设备制造业",
            "38": "电气机械和器材制造业",
            "39": "计算机、通信和其他电子设备制造业",
            "40": "仪器仪表制造业",
            "41": "其他制造业",
            "42": "废弃资源综合利用业",
            "43": "金属制品、机械和设备修理业"
        }
        
        # 模拟的重庆市区县列表
        self.chongqing_districts = [
            "万州区", "涪陵区", "渝中区", "大渡口区", "江北区", "沙坪坝区", 
            "九龙坡区", "南岸区", "北碚区", "綦江区", "大足区", "渝北区", 
            "巴南区", "黔江区", "长寿区", "江津区", "合川区", "永川区", 
            "南川区", "璧山区", "铜梁区", "潼南区", "荣昌区", "开州区", 
            "梁平区", "武隆区", "城口县", "丰都县", "垫江县", "忠县", 
            "云阳县", "奉节县", "巫山县", "巫溪县", "石柱土家族自治县", 
            "秀山土家族苗族自治县", "酉阳土家族苗族自治县", "彭水苗族土家族自治县"
        ]
        
        self.data = []
    
    def generate_sample_data(self):
        """生成样本数据"""
        print("正在生成样本数据...")
        
        # 只生成前5个行业的数据作为演示
        industries_to_process = list(self.manufacturing_subcategories.items())[:5]
        
        for i, (code, name) in enumerate(industries_to_process):
            # 显示进度
            progress = f"[{('█' * (i + 1)).ljust(len(industries_to_process), '░')}] {((i + 1) / len(industries_to_process) * 100):.1f}%"
            print(f"{progress} 正在处理: {name}")
            
            for district in self.chongqing_districts:
                # 为不同行业和区县生成不同范围的随机数
                base_count = int(code) * 10
                district_factor = self.chongqing_districts.index(district) % 5 + 1
                count = random.randint(base_count - 50, base_count + 100) * district_factor
                count = max(0, count)  # 确保数量不为负
                
                self.data.append({
                    "区县": district,
                    "行业代码": code,
                    "行业类别": name,
                    "企业数量": count
                })
            
            # 模拟延迟
            time.sleep(0.5)
        
        print("样本数据生成完成！")
    
    def save_to_csv(self, filename):
        """保存数据到CSV文件（作为Excel的替代方案）"""
        # 确保数据目录存在
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        # 保存总览表
        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            
            # 写入标题
            writer.writerow(["重庆市制造业企业统计"])
            writer.writerow([])  # 空行
            
            # 写入表头
            writer.writerow(["区县", "行业类别", "企业数量"])
            
            # 写入数据
            for item in self.data:
                writer.writerow([item["区县"], item["行业类别"], item["企业数量"]])
        
        print(f"总览数据已保存到: {filename}")
        
        # 保存区县汇总表
        district_summary_file = filename.replace(".csv", "_区县汇总.csv")
        self.save_district_summary(district_summary_file)
        
        # 保存行业汇总表
        industry_summary_file = filename.replace(".csv", "_行业汇总.csv")
        self.save_industry_summary(industry_summary_file)
        
        # 保存前3个区县的详细表作为示例
        for i, district in enumerate(self.chongqing_districts[:3]):
            district_file = filename.replace(".csv", f"_{district[:4]}_详细.csv")
            self.save_district_detail(district_file, district)
    
    def save_district_summary(self, filename):
        """保存区县汇总表"""
        # 按区县汇总数据
        district_summary = {}
        for item in self.data:
            district = item["区县"]
            count = item["企业数量"]
            if district in district_summary:
                district_summary[district] += count
            else:
                district_summary[district] = count
        
        # 保存到CSV
        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            
            # 写入标题
            writer.writerow(["重庆市制造业企业统计 - 区县汇总"])
            writer.writerow([])  # 空行
            
            # 写入表头
            writer.writerow(["区县", "企业总数"])
            
            # 写入数据
            for district, total in sorted(district_summary.items()):
                writer.writerow([district, total])
        
        print(f"区县汇总数据已保存到: {filename}")
    
    def save_industry_summary(self, filename):
        """保存行业汇总表"""
        # 按行业汇总数据
        industry_summary = {}
        for item in self.data:
            code = item["行业代码"]
            name = item["行业类别"]
            count = item["企业数量"]
            key = (code, name)
            if key in industry_summary:
                industry_summary[key] += count
            else:
                industry_summary[key] = count
        
        # 保存到CSV
        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            
            # 写入标题
            writer.writerow(["重庆市制造业企业统计 - 行业汇总"])
            writer.writerow([])  # 空行
            
            # 写入表头
            writer.writerow(["行业代码", "行业名称", "企业总数"])
            
            # 写入数据
            for (code, name), total in sorted(industry_summary.items()):
                writer.writerow([code, name, total])
        
        print(f"行业汇总数据已保存到: {filename}")
    
    def save_district_detail(self, filename, district):
        """保存区县详细表"""
        # 筛选指定区县的数据
        district_data = [item for item in self.data if item["区县"] == district]
        
        # 保存到CSV
        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            
            # 写入标题
            writer.writerow([f"{district}制造业企业统计"])
            writer.writerow([])  # 空行
            
            # 写入表头
            writer.writerow(["行业代码", "行业名称", "企业数量"])
            
            # 写入数据
            for item in sorted(district_data, key=lambda x: x["行业代码"]):
                writer.writerow([item["行业代码"], item["行业类别"], item["企业数量"]])
        
        print(f"{district}详细数据已保存到: {filename}")
    
    def print_sample_data(self):
        """打印样本数据的前几行"""
        print("\n样本数据预览:")
        print("-" * 80)
        print(f"{'区县':<15} {'行业代码':<8} {'行业类别':<25} {'企业数量':<10}")
        print("-" * 80)
        
        # 打印前10条数据
        for i, item in enumerate(self.data[:10]):
            print(f"{item['区县']:<15} {item['行业代码']:<8} {item['行业类别']:<25} {item['企业数量']:<10}")
        
        if len(self.data) > 10:
            print("..." * 20)
            print(f"共生成 {len(self.data)} 条数据")
    
    def run(self):
        """运行演示"""
        try:
            # 生成样本数据
            self.generate_sample_data()
            
            # 保存数据到CSV文件
            self.save_to_csv("data/chongqing_manufacturing_sample.csv")
            
            # 打印样本数据预览
            self.print_sample_data()
            
            print("\n演示完成！")
            print("\n生成的文件:")
            print("1. data/chongqing_manufacturing_sample.csv - 总览表")
            print("2. data/chongqing_manufacturing_sample_区县汇总.csv - 区县汇总表")
            print("3. data/chongqing_manufacturing_sample_行业汇总.csv - 行业汇总表")
            print("4. data/chongqing_manufacturing_sample_XXX_详细.csv - 区县详细表（示例）")
            
            print("\n注意事项:")
            print("- 这是演示版本，生成的是模拟数据")
            print("- 由于环境限制，数据保存为CSV格式，可直接用Excel打开")
            print("- 完整版本需要网络连接和相关Python库支持")
            
        except Exception as e:
            print(f"演示过程中出现错误: {e}")

def main():
    """主函数"""
    print("=" * 60)
    print("重庆市制造业企业数据爬虫 - 简单演示版本")
    print("=" * 60)
    print("此版本不依赖任何外部库，用于演示数据格式和结构")
    print("=" * 60)
    
    demo = SimpleDemo()
    demo.run()

if __name__ == "__main__":
    main()