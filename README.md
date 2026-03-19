# 期货数据爬虫和分析系统

一个完整的期货数据采集、技术分析和操作推荐系统，支持多数据源爬取、实时行情分析、技术指标计算和交易建议生成。

## 功能特性

### 数据采集
- 支持多数据源：东方财富期货、新浪期货、同花顺期货
- 实时行情爬取
- 历史K线数据获取
- 市场概览数据
- 自动重试和错误处理

### 技术分析
- **趋势指标**：SMA、EMA、MACD
- **震荡指标**：RSI、KDJ
- **波动指标**：布林带(BOLL)、ATR
- **成交量分析**
- 多时间周期分析

### 操作推荐
- 综合技术信号分析
- 多空方向判断
- 信心度评估
- 风险等级评估
- 市场情绪分析

### 数据存储
- CSV格式存储
- JSON格式存储
- SQLite/MySQL数据库存储
- 数据去重和合并

## 项目结构

```
futures_crawler/
├── __init__.py              # 包初始化
├── main.py                  # 主程序入口
├── demo.py                  # 演示脚本
├── base_crawler.py          # 爬虫基类
├── data_storage.py          # 数据存储模块
├── technical_analysis.py    # 技术分析模块
├── recommendation_engine.py # 推荐引擎
├── requirements.txt         # 依赖列表
├── config/
│   └── config.yaml          # 配置文件
├── crawlers/
│   ├── __init__.py
│   ├── eastmoney_crawler.py # 东方财富爬虫
│   ├── sina_crawler.py      # 新浪期货爬虫
│   └── tonghuashun_crawler.py # 同花顺爬虫
├── data/
│   ├── csv/                 # CSV数据存储
│   ├── json/                # JSON数据存储
│   └── futures.db           # SQLite数据库
└── logs/                    # 日志文件目录
```

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

或手动安装：

```bash
pip install requests beautifulsoup4 lxml pandas numpy pyyaml fake-useragent pandas-ta sqlalchemy pymysql
```

### 2. 运行演示

```bash
python3 demo.py
```

这将生成模拟数据并展示系统功能。

### 3. 运行主程序

```bash
# 爬取并分析（默认模式）
python3 main.py

# 仅爬取数据
python3 main.py --mode crawl

# 仅分析模式
python3 main.py --mode analyze

# 指定品种
python3 main.py --symbols RB0 CU0 AU0

# 定时任务模式
python3 main.py --mode schedule
```

## 配置说明

编辑 `config/config.yaml` 文件进行配置：

### 数据源配置

```yaml
sources:
  eastmoney:
    name: "东方财富期货"
    base_url: "https://futures.eastmoney.com"
    enabled: true
    timeout: 30
    retry: 3
```

### 存储配置

```yaml
storage:
  type: "both"  # csv, json, database, both
  database:
    type: "sqlite"  # sqlite, mysql
    sqlite_path: "./data/futures.db"
```

### 分析配置

```yaml
analysis:
  indicators:
    - MA      # 移动平均线
    - MACD    # MACD指标
    - RSI     # 相对强弱指标
    - BOLL    # 布林带
    - KDJ     # KDJ指标
```

## API使用示例

```python
from futures_crawler import (
    EastMoneyCrawler,
    DataStorage,
    TechnicalAnalysis,
    RecommendationEngine
)

# 初始化组件
crawler = EastMoneyCrawler(config)
storage = DataStorage(config)
analysis = TechnicalAnalysis(config)
recommendation = RecommendationEngine(config)

# 爬取行情
quotes = crawler.crawl_realtime_quote()

# 保存数据
storage.save(quotes, data_type='quote')

# 技术分析
for quote in quotes:
    result = analysis.analyze_quote(quote)
    print(result)

# 生成推荐
recommendations = recommendation.generate_batch_recommendations(quotes)
for rec in recommendations:
    print(f"{rec.symbol}: {rec.action} - {rec.reason}")
```

## 支持的期货品种

### 商品期货

| 类别 | 品种 | 交易所 |
|------|------|--------|
| 金属 | 铜(CU)、铝(AL)、锌(ZN)、铅(PB)、镍(NI)、锡(SN)、金(AU)、银(AG) | 上期所 |
| 黑色 | 螺纹钢(RB)、热卷(HC)、铁矿石(I)、焦炭(J)、焦煤(JM) | 上期所/大商所 |
| 化工 | 橡胶(RU)、沥青(BU)、玻璃(FG)、甲醇(MA)、PP、塑料(L)、PVC(V)、乙二醇(EG)、原油(SC) | 多家 |
| 农产品 | 豆粕(M)、豆油(Y)、棕榈油(P)、豆一(A)、玉米(C)、淀粉(CS)、鸡蛋(JD)、生猪(LH)、苹果(AP)、白糖(SR)、棉花(CF) | 大商所/郑商所 |

### 金融期货

| 类别 | 品种 | 说明 |
|------|------|------|
| 股指期货 | IF、IH、IC、IM | 沪深300/上证50/中证500/中证1000 |
| 国债期货 | T、TF、TS、TL | 10年/5年/2年/30年期国债 |

## 输出报告示例

```
============================================================
期货市场操作建议报告
生成时间: 2026-03-19 21:28:29
============================================================

【市场概况】
  统计品种: 50 个
  市场情绪: 偏多

【推荐分布】
  强烈买入: 12 个
  买入: 5 个
  观望: 8 个
  卖出: 5 个
  强烈卖出: 0 个

【做多推荐 TOP10】
  1. CU0 沪铜主力 - 强烈买入
     信心度: 95.0% | 风险: 低风险
     理由: 技术趋势强烈看涨；强势上涨；MACD金叉

【做空推荐 TOP10】
  1. EG0 乙二醇主力 - 卖出
     信心度: 75.0% | 风险: 低风险
     理由: 技术趋势看跌；强势下跌

【风险提示】
  以上建议仅供参考，不构成投资建议。
  期货交易存在风险，投资需谨慎。
```

## 技术指标说明

### MACD (指数平滑移动平均线)
- 金叉：DIF上穿DEA，买入信号
- 死叉：DIF下穿DEA，卖出信号

### RSI (相对强弱指标)
- RSI > 70：超买区域，可能回调
- RSI < 30：超卖区域，可能反弹

### 布林带 (BOLL)
- 价格触及上轨：可能回调
- 价格触及下轨：可能反弹

### KDJ指标
- K > 80：超买
- K < 20：超卖

## 注意事项

1. 本系统仅供学习和研究使用
2. 期货交易风险较高，投资需谨慎
3. 系统建议仅供参考，不构成投资建议
4. 请遵守相关网站的使用条款
5. 注意控制爬取频率，避免对服务器造成压力

## 许可证

MIT License

## 作者

Claude Code

## 更新日志

### v1.0.0 (2026-03-19)
- 初始版本发布
- 支持三大数据源爬取
- 完整的技术分析模块
- 操作推荐引擎
- 多种数据存储方式
