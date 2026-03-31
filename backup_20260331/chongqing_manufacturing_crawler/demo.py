# 演示脚本 - 模拟爬虫功能（无需网络连接）
import os
import json
import random
import time
from utils.excel_utils import *
from utils.data_utils import *

class DemoCrawler:
    def __init__(self):
        self.data = []  # 存储爬取的数据
        self.processed_districts = set()  # 已处理的区县
        self.processed_industries = set()  # 已处理的行业
        
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
    
    def simulate_login(self):
        """模拟登录过程"""
        print("正在模拟登录企查查...")
        time.sleep(1)
        print("请输入收到的验证码: 123456")
        time.sleep(1)
        print("登录成功")
        return True
    
    def simulate_navigation(self):
        """模拟导航过程"""
        print("正在导航到制造业筛选页面...")
        time.sleep(1)
        print("已导航到制造业筛选页面")
        return True
    
    def simulate_get_industry_district_counts(self, industry_code, industry_name):
        """模拟获取指定行业在各区县的企业数量"""
        print(f"正在获取 {industry_name} 在各区县的企业数量...")
        
        # 模拟随机延迟
        delay = random.uniform(0.5, 1.5)
        time.sleep(delay)
        
        # 生成模拟数据
        district_counts = {}
        for district in self.chongqing_districts:
            # 为不同行业和区县生成不同范围的随机数
            base_count = int(industry_code) * 10
            district_factor = self.chongqing_districts.index(district) % 5 + 1
            count = random.randint(base_count - 50, base_count + 100) * district_factor
            count = max(0, count)  # 确保数量不为负
            
            district_counts[district] = count
            
            # 记录数据
            self.data.append({
                "区县": district,
                "行业代码": industry_code,
                "行业类别": industry_name,
                "企业数量": count
            })
            
            # 更新已处理的区县和行业
            self.processed_districts.add(district)
            self.processed_industries.add(f"{industry_code}-{industry_name}")
        
        # 保存临时数据
        temp_data = {
            "processed_districts": list(self.processed_districts),
            "processed_industries": list(self.processed_industries),
            "data": self.data
        }
        save_temp_data("data/temp_data.json", temp_data)
        
        # 更新Excel文件
        excel_data = [[item["区县"], item["行业类别"], item["企业数量"]] for item in self.data]
        update_excel_data("data/chongqing_manufacturing_companies.xlsx", excel_data)
        
        # 为每个区县更新工作表
        for item in self.data:
            update_district_sheet(
                "data/chongqing_manufacturing_companies.xlsx", 
                item["区县"], 
                item["行业代码"], 
                item["行业类别"], 
                item["企业数量"]
            )
        
        print(f"已获取 {industry_name} 在各区县的企业数量")
        return district_counts
    
    def run(self):
        """运行演示爬虫"""
        try:
            # 确保数据目录存在
            os.makedirs("data", exist_ok=True)
            
            # 尝试加载临时数据
            temp_data = load_temp_data("data/temp_data.json")
            if temp_data:
                self.processed_districts = set(temp_data.get("processed_districts", []))
                self.processed_industries = set(temp_data.get("processed_industries", []))
                self.data = temp_data.get("data", [])
            
            # 模拟登录
            if not self.simulate_login():
                return
            
            # 模拟导航
            if not self.simulate_navigation():
                return
            
            # 创建Excel模板
            create_excel_template("data/chongqing_manufacturing_companies.xlsx")
            
            # 为每个区县创建工作表
            create_district_sheets("data/chongqing_manufacturing_companies.xlsx", 
                                 self.chongqing_districts, 
                                 self.manufacturing_subcategories)
            
            # 创建汇总工作表
            create_summary_sheet("data/chongqing_manufacturing_companies.xlsx", 
                               self.chongqing_districts, 
                               self.manufacturing_subcategories)
            
            # 遍历所有制造业细分行业（这里只演示前5个行业，实际使用时可以遍历全部）
            industries_to_process = list(self.manufacturing_subcategories.items())[:5]
            total_industries = len(industries_to_process)
            processed_count = 0
            
            for code, name in industries_to_process:
                # 检查是否已处理
                if f"{code}-{name}" in self.processed_industries:
                    print(f"跳过已处理的行业: {name}")
                    processed_count += 1
                    continue
                
                # 显示进度
                progress = get_progress_bar(processed_count, total_industries)
                print(f"{progress} 正在处理: {name}")
                
                # 模拟获取行业区县企业数量
                self.simulate_get_industry_district_counts(code, name)
                
                processed_count += 1
            
            # 更新汇总工作表
            update_summary_sheet("data/chongqing_manufacturing_companies.xlsx", self.data)
            
            print("\n演示完成！")
            print("生成的Excel文件路径: data/chongqing_manufacturing_companies.xlsx")
            print("注意：这是演示版本，生成的是模拟数据。要获取真实数据，请运行原始爬虫脚本（需要网络连接）。")
            
        except Exception as e:
            print(f"演示过程中出现错误: {e}")

def main():
    """主函数"""
    print("=" * 60)
    print("重庆市制造业企业数据爬虫 - 演示版本")
    print("=" * 60)
    print("此版本无需网络连接，用于演示爬虫功能和数据格式")
    print("=" * 60)
    
    crawler = DemoCrawler()
    crawler.run()

if __name__ == "__main__":
    main()