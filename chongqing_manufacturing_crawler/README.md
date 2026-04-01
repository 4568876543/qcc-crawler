# 企查查行业搜索爬虫

基于 Playwright 的企查查网站数据爬虫，用于爬取各省/市制造业细分行业在各区县的企业数量分布数据。

## 功能特点

- **自动登录**：支持 Cookie 持久化，自动扫码登录
- **断点续爬**：程序中断后可从上次位置继续，无需重头开始
- **数据校验**：自动检测异常数据（如某行业数据接近制造业合计），异常时自动重试
- **筛选验证**：爬取前验证地区、登记状态、行业筛选条件是否正确设置
- **实时进度**：Excel 文件第一个工作表实时显示执行进度
- **多维度输出**：
  - 城市汇总表：各区县制造业企业总数
  - 行业明细表：每个细分行业的区县分布
  - 汇总明细表：包含明细数据、区县汇总、行业汇总三个 Sheet
  - 执行进度表：实时监控所有行业爬取状态

## 项目结构

```
chongqing_manufacturing_crawler/
├── config_changsha.py       # 配置文件（交互式输入）
├── crawler_changsha.py       # 主爬虫程序
├── test_run.py               # 启动脚本（自动重试模式）
├── data_verification.py       # 数据核实模块
├── utils/
│   ├── excel_utils.py        # Excel 操作工具
│   ├── data_utils.py         # 数据处理工具
│   ├── task_manager.py       # 任务状态管理（断点续爬）
│   ├── index_cache.py        # 索引缓存
│   └── data_validator.py     # 数据验证器
└── data/                     # 数据存储目录
    ├── *.xlsx                # 输出的 Excel 文件
    ├── temp_data.json         # 临时数据（断点续爬）
    ├── task_state.json       # 任务状态（断点续爬）
    └── 行业明细表/            # 各行业明细表
```

## 安装依赖

```bash
pip install playwright pandas openpyxl
playwright install chromium
```

## 使用方法

### 1. 基本运行

```bash
cd /Users/luodong/trae/02-Projects/企查查行业搜索/chongqing_manufacturing_crawler
python3 test_run.py
```

### 2. 交互式输入

首次运行会提示输入：
- **关键字**：输入"湖南省"或"长沙市"等
- **地区**：再次确认地区名称

### 3. 登录

程序会自动打开浏览器：
1. 扫描二维码登录企查查账号
2. 登录成功后程序会自动保存 Cookie
3. 后续运行无需重复登录

### 4. 等待完成

程序会自动完成：
1. 设置筛选条件（地区、存续状态、制造业）
2. 获取制造业各区县汇总数据
3. 逐个爬取 31 个细分行业
4. 保存所有 Excel 表格

## 配置文件说明 (config_changsha.py)

```python
SEARCH_LOCATION = "湖南省"        # 搜索地区（支持省/市）
DISTRICT_LEVEL = "province"     # 地区层级：province(省) 或 city(市)
COMPANY_STATUS = "存续"          # 登记状态筛选
MANUFACTURING_SUBCATEGORIES = {  # 制造业细分行业字典
    "13": "农副食品加工业",
    "14": "食品制造业",
    # ... 共 31 个行业
}
OUTPUT_DIR = "data/hunan_test"   # 输出目录
```

## 常见问题与解决方案

### 1. 登录失败

**症状**：程序卡在登录页面，无法继续

**解决方案**：
- 检查网络连接
- 手动扫码登录后，Cookie 会自动保存
- 删除 `data/qcc_cookies.json` 重新登录

### 2. 行业选择失败

**症状**：日志显示 `[行业选择] ⚠️ 已点击但未确认`

**解决方案**：
- 程序会自动重试
- 检查是否有弹窗遮挡
- 检查 `logs/screenshots/` 目录下的截图

### 3. 数据异常（关键！）

**症状**：
```
⚠️ 警告: 某行业数据 (500000) 接近制造业合计 (510000)
⚠️ 比例: 98.0%，行业选择可能未生效！
```

**原因**：行业选择点击后实际未生效，获取的是全行业数据

**解决方案**：
- 程序已自动重试
- 如仍失败，检查截图 `WARNING_*_data_anomaly.png`
- 可能是弹窗或页面响应问题，手动检查筛选条件

### 4. 筛选条件未设置

**症状**：
```
❌ [验证失败] 地区筛选条件未设置
❌ [错误] 筛选条件验证失败，无法继续爬取！
```

**原因**：页面加载不完整或选择操作失败

**解决方案**：
- 检查 `logs/screenshots/FILTER_VERIFICATION_FAILED.png` 截图
- 确保浏览器可见，页面正常加载
- 删除 `data/browser_profile/` 重新开始

### 5. 断点续爬失败

**症状**：程序重新开始，没有继续上次进度

**解决方案**：
- 检查 `data/task_state.json` 是否存在
- 检查 `data/temp_data.json` 是否存在且完整
- 删除这两个文件，重新开始爬取

### 6. Excel 文件无法写入

**症状**：`Permission denied` 或文件被占用

**解决方案**：
- 关闭正在打开的 Excel 文件
- 检查文件是否有只读属性

## 数据核实

运行数据核实模块检查数据质量：

```bash
python3 data_verification.py data/hunan_test
```

**核实内容**：
- 任务完成情况
- 数据总额核对（城市汇总表 vs 细分行业明细）
- 异常行业检测（数据 > 制造业合计的 90%）
- 数据质量评级

## 输出文件说明

### Excel 文件结构

每个 Excel 文件包含：

```
第1行: 筛选条件: 地区=湖南省 | 登记状态=存续 | 行业=制造业
第2行: 生成时间: 2026-04-01 14:30:00
第3行: [标题]
...
```

### 执行进度表（第一个工作表）

| 序号 | 行业代码 | 行业名称 | 状态 | 企业数量 |
|-----|---------|---------|------|---------|
| 1 | 13 | 农副食品加工业 | completed | 63,341 |
| 2 | 14 | 食品制造业 | completed | 58,953 |
| ... | | | | |
| ▼ 待爬取行业 | | | | |
| 6 | 15 | 酒、饮料和精制茶制造业 | 待爬取 | 0 |

**状态颜色**：
- 🟢 绿色：completed（已完成）
- 🟡 黄色：in_progress（进行中）
- ⚪ 灰色：pending（待处理）
- 🔴 红色：failed（失败）

## 截图说明

截图保存在 `logs/screenshots/` 目录，命名包含筛选条件：

```
城市汇总表_[湖南省]_[存续].png
农副食品加工业行业汇总表_[湖南省]_[存续].png
WARNING_31_data_anomaly.png    # 数据异常警告截图
FILTER_VERIFICATION_FAILED.png # 筛选条件验证失败截图
```

## 注意事项

1. **登录维护**：定期清理 Cookie 可能导致重新登录，保持 Cookie 有效
2. **合理间隔**：程序内置随机延迟，请勿过度修改
3. **数据核对**：完成后运行 `data_verification.py` 核实数据一致性
4. **异常处理**：发现数据异常时，程序会自动重试，无需手动干预
5. **遵守规则**：请合理使用，遵守网站的使用条款

## 技术支持

- 查看日志：`logs/changsha_crawler.log`
- 查看截图：`logs/screenshots/`
- 检查临时数据：`data/temp_data.json`
- 检查任务状态：`data/task_state.json`

---

最后更新：2026-04-01
