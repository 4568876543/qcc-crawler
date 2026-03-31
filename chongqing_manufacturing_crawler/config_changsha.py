# 长沙市制造业爬虫配置文件
import os
from datetime import datetime

# 登录信息
PHONE_NUMBER = ""  # 手机号（无空格）
# 注意：验证码需要手动输入

# 基础URL
BASE_URL = "https://www.qcc.com"
ADVANCED_SEARCH_URL = "https://www.qcc.com/web/search/advance?hasState=true"

# 筛选条件
CITY = "湖南省"  # 测试用，改为湖南省
INDUSTRY_CATEGORY = "制造业"
INDUSTRY_CODE = "C"  # 国标行业代码
COMPANY_STATUS = "存续/在业"  # 仅存续/在业状态
CRAWL_KEYWORD = "湖南制造业"  # 爬取关键字，用于生成输出目录名

# 制造业细分行业列表
# 注意：行业名称必须与企查查页面完全一致
MANUFACTURING_SUBCATEGORIES = {
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
    "25": "石油、煤炭及其他燃料加工业",
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

# 长沙市区县列表
# 实际运行时会从页面动态获取最新列表
CHANGSHA_DISTRICTS = [
    "芙蓉区", "天心区", "岳麓区", "开福区", "雨花区", "望城区",
    "长沙县", "浏阳市", "宁乡市"
]

# 爬虫配置
HEADLESS = False  # 是否无头模式运行（建议False以便调试和输入验证码）
SLOWMO = 50  # 操作延迟，单位毫秒
TIMEOUT = 30000  # 超时时间，单位毫秒
PAGE_SIZE = 40  # 每页显示的企业数量

# 数据存储基目录
DATA_BASE_DIR = "data/crawl_results"  # 爬取结果存放基目录
MERGED_OUTPUT_FILE = "data/长沙制造业企业数量统计_汇总.xlsx"  # 最终汇总文件

# 动态输出目录生成函数
def get_output_dir(keyword=None):
    """
    生成带时间戳的输出目录路径
    格式: data/crawl_results/关键字_YYYYMMDD_HHMMSS
    """
    if keyword is None:
        keyword = CRAWL_KEYWORD
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dir_name = f"{keyword}_{timestamp}"
    output_dir = os.path.join(DATA_BASE_DIR, dir_name)
    os.makedirs(output_dir, exist_ok=True)
    return output_dir

def get_output_files(output_dir):
    """
    获取指定输出目录下的文件路径
    """
    return {
        "excel": os.path.join(output_dir, "数据统计.xlsx"),
        "temp": os.path.join(output_dir, "temp_data.json"),
        "task": os.path.join(output_dir, "task_state.json"),
        "index_cache": os.path.join(output_dir, "index_cache.json"),
        "validation_dir": os.path.join(output_dir, "validation")
    }

def find_latest_incomplete_dir(keyword=None):
    """
    查找最近的未完成的爬取目录
    
    Returns:
        str: 未完成目录的路径，如果没有则返回None
    """
    if keyword is None:
        keyword = CRAWL_KEYWORD
    
    if not os.path.exists(DATA_BASE_DIR):
        return None
    
    # 查找所有匹配关键字的目录
    matching_dirs = []
    for name in os.listdir(DATA_BASE_DIR):
        if name.startswith(keyword):
            dir_path = os.path.join(DATA_BASE_DIR, name)
            if os.path.isdir(dir_path):
                temp_file = os.path.join(dir_path, "temp_data.json")
                task_file = os.path.join(dir_path, "task_state.json")
                # 检查是否有临时数据文件且任务未完成
                if os.path.exists(temp_file):
                    try:
                        import json
                        with open(temp_file, 'r') as f:
                            temp_data = json.load(f)
                            # 如果有已处理的行业但未全部完成
                            processed = temp_data.get('processed_industries', [])
                            if len(processed) > 0 and len(processed) < 31:  # 31个行业
                                matching_dirs.append((dir_path, name, len(processed)))
                    except:
                        pass
    
    # 按处理进度排序，返回进度最高的
    if matching_dirs:
        matching_dirs.sort(key=lambda x: x[2], reverse=True)
        return matching_dirs[0][0]
    
    return None

# 兼容旧版本的静态路径（运行时会被动态路径覆盖）
OUTPUT_FILE = "data/长沙制造业企业数量统计.xlsx"
TEMP_DATA_FILE = "data/changsha_temp_data.json"  # 临时数据文件，用于断点续爬
TASK_FILE = "data/changsha_task_state.json"  # 任务状态文件
INDEX_CACHE_FILE = "data/changsha_index_cache.json"  # 索引缓存文件
VALIDATION_DIR = "logs/changsha_validation"  # 验证报告目录

# 自动保存配置
AUTO_SAVE_INTERVAL = 5  # 每完成N个行业自动保存一次
SAVE_ON_EACH_INDUSTRY = True  # 每完成一个行业立即保存

# 反爬策略
RANDOM_DELAY_MIN = 1  # 随机延迟最小值，单位秒
RANDOM_DELAY_MAX = 3  # 随机延迟最大值，单位秒
MAX_RETRIES = 3  # 最大重试次数

# 条件验证配置
VERIFY_CONDITIONS_ON_RESUME = True  # 续爬时是否验证筛选条件
CONDITION_TOLERANCE = 1  # 允许的条件差异数量（除行业大类外）
