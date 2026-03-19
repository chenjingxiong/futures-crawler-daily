#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
期货爬虫系统演示脚本
生成模拟数据展示系统功能
"""

import sys
import os
from datetime import datetime, timedelta
import random

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 直接导入模块避免相对导入问题
import importlib.util

def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module

# 加载模块
base_dir = os.path.dirname(os.path.abspath(__file__))
TechnicalAnalysis = load_module('technical_analysis',
    os.path.join(base_dir, 'technical_analysis.py')).TechnicalAnalysis
RecommendationEngine = load_module('recommendation_engine',
    os.path.join(base_dir, 'recommendation_engine.py')).RecommendationEngine


def generate_mock_quotes(count: int = 50) -> list:
    """生成模拟行情数据"""
    mock_quotes = []

    # 期货品种列表
    futures = [
        # 金属
        ('CU0', '沪铜主力', '上期所'),
        ('AL0', '沪铝主力', '上期所'),
        ('ZN0', '沪锌主力', '上期所'),
        ('PB0', '沪铅主力', '上期所'),
        ('NI0', '沪镍主力', '上期所'),
        ('SN0', '沪锡主力', '上期所'),
        ('AU0', '沪金主力', '上期所'),
        ('AG0', '沪银主力', '上期所'),
        # 黑色
        ('RB0', '螺纹钢主力', '上期所'),
        ('HC0', '热卷主力', '上期所'),
        ('I0', '铁矿石主力', '大商所'),
        ('J0', '焦炭主力', '大商所'),
        ('JM0', '焦煤主力', '大商所'),
        # 化工
        ('RU0', '橡胶主力', '上期所'),
        ('BU0', '沥青主力', '上期所'),
        ('FG0', '玻璃主力', '郑商所'),
        ('MA0', '甲醇主力', '郑商所'),
        ('PP0', 'PP主力', '大商所'),
        ('L0', '塑料主力', '大商所'),
        ('V0', 'PVC主力', '大商所'),
        ('EG0', '乙二醇主力', '大商所'),
        ('SC0', '原油主力', '能源中心'),
        # 农产品
        ('M0', '豆粕主力', '大商所'),
        ('Y0', '豆油主力', '大商所'),
        ('P0', '棕榈油主力', '大商所'),
        ('A0', '豆一主力', '大商所'),
        ('C0', '玉米主力', '大商所'),
        ('CS0', '玉米淀粉主力', '大商所'),
        ('JD0', '鸡蛋主力', '大商所'),
        ('LH0', '生猪主力', '大商所'),
        ('AP0', '苹果主力', '郑商所'),
        ('SR0', '白糖主力', '郑商所'),
        ('CF0', '棉花主力', '郑商所'),
        ('RM0', '菜粕主力', '郑商所'),
        ('OI0', '菜油主力', '郑商所'),
        ('RS0', '菜籽主力', '郑商所'),
        ('RI0', '早籼稻主力', '郑商所'),
        ('WH0', '强麦主力', '郑商所'),
        ('PM0', '普麦主力', '郑商所'),
        ('JR0', '粳稻主力', '郑商所'),
        ('LR0', '晚籼稻主力', '郑商所'),
        ('PK0', '花生主力', '郑商所'),
        # 金融
        ('IF0', '沪深300主力', '中金所'),
        ('IH0', '上证50主力', '中金所'),
        ('IC0', '中证500主力', '中金所'),
        ('IM0', '中证1000主力', '中金所'),
        ('T0', '10年期国债主力', '中金所'),
        ('TF0', '5年期国债主力', '中金所'),
        ('TS0', '2年期国债主力', '中金所'),
        ('TL0', '30年期国债主力', '中金所'),
    ]

    # 价格区间配置
    price_ranges = {
        'CU': (65000, 75000),
        'AL': (18000, 22000),
        'ZN': (20000, 25000),
        'PB': (15000, 18000),
        'NI': (120000, 150000),
        'SN': (200000, 250000),
        'AU': (450, 550),
        'AG': (5500, 7000),
        'RB': (3000, 4500),
        'HC': (3200, 4200),
        'I': (700, 1100),
        'J': (2000, 3000),
        'JM': (1500, 2200),
        'RU': (13000, 16000),
        'BU': (3500, 4200),
        'FG': (1500, 2200),
        'MA': (2300, 2800),
        'PP': (7000, 8500),
        'L': (7800, 9200),
        'V': (5800, 6800),
        'EG': (4000, 5200),
        'SC': (550, 750),
        'M': (2800, 3800),
        'Y': (7000, 9000),
        'P': (7500, 9500),
        'A': (4200, 5500),
        'C': (2200, 2800),
        'CS': (2700, 3200),
        'JD': (3500, 4500),
        'LH': (14000, 18000),
        'AP': (8000, 10000),
        'SR': (6000, 7200),
        'CF': (14000, 17000),
        'RM': (2500, 3200),
        'OI': (7500, 9500),
        'RS': (5500, 6500),
        'RI': (2600, 3200),
        'WH': (2800, 3500),
        'PM': (2700, 3300),
        'JR': (3000, 3800),
        'LR': (2600, 3200),
        'PK': (8500, 10500),
        'IF': (3200, 4200),
        'IH': (2200, 2800),
        'IC': (5000, 6500),
        'IM': (6000, 7500),
        'T': (103, 108),
        'TF': (101, 104),
        'TS': (100, 101),
        'TL': (102, 108),
    }

    for i in range(min(count, len(futures))):
        symbol, name, exchange = futures[i]
        base = symbol[:-2] if len(symbol) > 2 else symbol

        # 获取价格区间
        price_range = price_ranges.get(base, (3000, 8000))
        base_price = random.uniform(*price_range)

        # 生成涨跌
        change_percent = random.uniform(-5, 5)
        change = base_price * change_percent / 100

        # 生成其他数据
        high = base_price + random.uniform(0, base_price * 0.02)
        low = base_price + random.uniform(-base_price * 0.02, 0)
        open_price = base_price + random.uniform(-base_price * 0.015, base_price * 0.015)
        close_price = base_price + random.uniform(-base_price * 0.01, base_price * 0.01)

        # 成交量和持仓量
        volume = random.randint(50000, 5000000)
        open_interest = random.randint(100000, 3000000)

        mock_quotes.append({
            'symbol': symbol,
            'name': name,
            'price': round(base_price, 2),
            'open': round(open_price, 2),
            'high': round(high, 2),
            'low': round(low, 2),
            'close': round(close_price, 2),
            'change': round(change, 2),
            'change_percent': round(change_percent, 2),
            'volume': volume,
            'open_interest': open_interest,
            'bid1': round(base_price - random.uniform(1, 10), 2),
            'ask1': round(base_price + random.uniform(1, 10), 2),
            'turnover': volume * base_price,
            'status': '上涨' if change > 0 else '下跌' if change < 0 else '平盘',
            'exchange': exchange,
            'source': '演示数据',
            'timestamp': datetime.now()
        })

    return mock_quotes


def generate_mock_historical(days: int = 100) -> list:
    """生成模拟历史数据"""
    base_price = 4000
    historical = []

    for i in range(days):
        # 模拟价格波动
        change = random.uniform(-50, 50)
        price = base_price + change

        historical.append({
            'date': (datetime.now() - timedelta(days=days - i)).strftime('%Y-%m-%d'),
            'open': round(price + random.uniform(-10, 10), 2),
            'high': round(price + random.uniform(0, 30), 2),
            'low': round(price + random.uniform(-30, 0), 2),
            'close': round(price, 2),
            'volume': random.randint(100000, 1000000),
            'open_interest': random.randint(500000, 2000000)
        })

        base_price = price

    return historical


def main():
    """演示主函数"""
    print("=" * 60)
    print("期货数据爬虫和分析系统 - 演示")
    print("=" * 60)
    print()

    # 生成模拟数据
    print("正在生成模拟行情数据...")
    quotes = generate_mock_quotes(50)
    print(f"已生成 {len(quotes)} 个品种的模拟行情数据\n")

    # 创建分析引擎
    print("初始化技术分析引擎...")
    analysis = TechnicalAnalysis()

    # 创建推荐引擎
    print("初始化推荐引擎...")
    recommendation = RecommendationEngine()

    # 生成模拟历史数据（用于技术指标计算）
    historical = generate_mock_historical(100)

    # 进行技术分析
    print("正在执行技术分析...")
    analysis_results = []
    for quote in quotes[:20]:  # 分析前20个
        result = analysis.analyze_quote(quote, historical)
        analysis_results.append(result)

    # 生成推荐
    print("正在生成操作建议...")
    recommendations = recommendation.generate_batch_recommendations(
        quotes[:30], analysis_results[:20]
    )

    # 生成市场汇总
    print("正在生成市场汇总...")
    summary = recommendation.generate_market_summary(quotes, recommendations)

    # 格式化并打印报告
    print("\n")
    report = recommendation.format_report(quotes, recommendations, summary)
    print(report)

    # 保存模拟数据
    print("\n正在保存模拟数据...")

    # 保存为JSON
    import json
    os.makedirs('./data/json', exist_ok=True)
    with open('./data/json/demo_quotes.json', 'w', encoding='utf-8') as f:
        json.dump([{
            'symbol': q['symbol'],
            'name': q['name'],
            'price': q['price'],
            'change_percent': q['change_percent']
        } for q in quotes], f, ensure_ascii=False, indent=2)

    # 保存推荐结果
    with open('./data/json/demo_recommendations.json', 'w', encoding='utf-8') as f:
        json.dump({
            'summary': summary,
            'recommendations': [r.to_dict() for r in recommendations]
        }, f, ensure_ascii=False, indent=2)

    print("模拟数据已保存到 ./data/json/ 目录")

    print("\n" + "=" * 60)
    print("演示完成！")
    print("=" * 60)
    print("\n使用说明：")
    print("1. 本演示使用模拟数据展示系统功能")
    print("2. 实际使用时，请确保网络连接正常")
    print("3. 运行主程序获取真实数据: python3 main.py --mode analyze")
    print("4. 配置文件: config/config.yaml")
    print("5. 数据存储目录: data/")
    print()


if __name__ == '__main__':
    main()
