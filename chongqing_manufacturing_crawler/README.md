# 企查查行业搜索爬虫

基于 Playwright 的企查查网站数据爬虫，用于爬取各省/市制造业细分行业在各区县的企业数量分布数据。

## 功能特点

- **配置文件驱动**：所有参数从 `config.json` 读取，无需交互式输入
- **自动登录**：支持 Cookie 持久化，扫码登录一次，后续自动使用
- **断点续爬**：程序中断后可从上次位置继续，无需重头开始
- **数据校验**：自动检测异常数据（如某行业数据接近制造业合计），异常时自动重试
- **智能重试**：数据异常时自动重新选择行业并验证
- **实时进度**：Excel 文件第一个工作表实时显示执行进度
- **Claude Code 兼容**：零交互，Claude Code 环境永不停顿

## 项目结构

```
chongqing_manufacturing_crawler/
├── main.py                     # 主入口（配置文件驱动）
├── login.py                    # 登录模块
├── cli.py                      # 旧版CLI（保留）
├── config.json                 # 配置文件
├── crawler_changsha.py         # 主爬虫程序
├── table_converter/            # 表格转换模块
│   ├── converter.py
│   └── sheet1_generator.py
├── utils/
│   ├── excel_utils.py          # Excel 操作工具
│   ├── data_utils.py           # 数据处理工具
│   ├── task_manager.py         # 任务状态管理
│   ├── index_cache.py         # 索引缓存
│   └── data_validator.py      # 数据验证器
└── data/                      # 数据存储目录
    ├── browser_profile/        # 浏览器Cookie（登录状态）
    ├── crawl_results/          # 爬取结果
    └── *.xlsx                 # 输出的 Excel 文件
```

## 安装依赖

```bash
pip install playwright pandas openpyxl
playwright install chromium
```

## 快速开始

### 1. 首次登录

```bash
python login.py              # 交互式登录（推荐）
python login.py --auto       # 自动等待60秒
python login.py --check      # 检查登录状态
```

### 2. 启动爬虫

```bash
python main.py              # 使用config.json配置
python main.py 广东省       # 命令行指定关键词
python main.py --usage      # 显示完整使用说明
```

### 3. 配置文件 (config.json)

```json
{
    "crawler": {
        "enabled": true,
        "search_box_keyword": "湖南省",
        "省份地区": "湖南省",
        "district_level": "city",
        "国标行业": "制造业",
        "company_status": "存续（在业）",
        "mode": "new"
    }
}
```

| 字段 | 说明 | 示例 |
|------|------|------|
| search_box_keyword | 搜索框关键词 | "湖南省" |
| 省份地区 | 下拉框选择的地址 | "湖南省" |
| district_level | 搜索层级 | "city"(市级), "province"(省级) |
| 国标行业 | 国标行业 | "制造业" |
| company_status | 登记状态 | "存续（在业）" |
| mode | 运行模式 | "new"(全新), "resume"(继续) |

## 命令行参数

```bash
# 基本用法
python main.py                              # 使用config.json
python main.py 广东省                        # 指定关键词
python main.py --keyword 广东省 --mode resume  # 指定所有参数

# 仅运行特定模块
python main.py --crawler                     # 仅爬虫
python main.py --converter                   # 仅转换器
python main.py --verify                      # 仅数据核实

# 帮助
python main.py --help                        # 显示帮助
python main.py --usage                       # 显示使用说明
```

## 使用流程

### 1. 检查登录状态

```bash
python login.py --check
```

### 2. 登录（如需）

```bash
python login.py
# 浏览器打开后扫码登录
# 登录成功后按回车确认
```

### 3. 配置爬取参数

编辑 `config.json`：
```json
{
    "crawler": {
        "search_box_keyword": "湖南省",
        "省份地区": "湖南省",
        "mode": "new"
    }
}
```

### 4. 启动爬虫

```bash
python main.py
```

## 数据输出

```
data/crawl_results/湖南省_20260403_123456/
├── 行业明细表/
│   ├── 00_湖南省制造业总表.xlsx     # 城市汇总表
│   ├── 13_农副食品加工业.xlsx       # 各行业明细
│   └── ...
├── screenshots/                      # 截图
│   └── *.png
└── validation/                       # 验证结果
```

## 常见问题

### 1. Cookie失效

**症状**：程序报错 "Cookie未保存"

**解决**：
```bash
python login.py
```

### 2. 行业选择错误

**症状**：点击到高级搜索而不是行业

**解决**：程序已修复，会自动过滤高级搜索链接

### 3. 断点续爬

```bash
python main.py --mode resume
```

### 4. 行政区划漏掉

**症状**：湘西土家族苗族自治州被漏掉

**解决**：程序使用标准行政区划后缀白名单，已包含"州"等后缀

## 更新日志

### v2.0 (2026-04-03)
- 新增 `main.py` 配置文件驱动架构
- 新增 `login.py` 独立登录模块
- 新增 `config.json` 配置文件
- 修复行业选择错误问题（过滤高级搜索）
- 修复行政区划漏掉问题（使用标准后缀白名单）
- 支持 Claude Code 零交互运行

### v1.0 (2026-04-01)
- 初始版本

---

最后更新：2026-04-03
