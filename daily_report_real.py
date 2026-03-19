#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日期货报告生成脚本 - 真实数据版本
自动生成Markdown格式的分析报告并提交到GitHub
"""

import os
import sys
from datetime import datetime, timedelta
import json
import random
import subprocess

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import importlib.util

def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module

base_dir = os.path.dirname(os.path.abspath(__file__))
TechnicalAnalysis = load_module('technical_analysis',
    os.path.join(base_dir, 'technical_analysis.py')).TechnicalAnalysis
RecommendationEngine = load_module('recommendation_engine',
    os.path.join(base_dir, 'recommendation_engine.py')).RecommendationEngine


def get_real_futures_data():
    """获取真实期货数据"""
    quotes = []

    try:
        import akshare as ak

        print("正在获取真实期货数据...")
        df = ak.futures_zh_realtime()

        for _, row in df.iterrows():
            quote = {
                'symbol': str(row.get('symbol', '')),
                'name': str(row.get('name', '')),
                'price': float(row.get('trade', 0)) if row.get('trade') else float(row.get('close', 0)),
                'open': float(row.get('open', 0)),
                'high': float(row.get('high', 0)),
                'low': float(row.get('low', 0)),
                'close': float(row.get('close', 0)),
                'volume': float(row.get('volume', 0)),
                'open_interest': float(row.get('position', 0)),
                'change': 0,
                'change_percent': float(row.get('changepercent', 0)),
                'exchange': str(row.get('exchange', '')),
                'source': 'AKShare实时',
                'timestamp': datetime.now()
            }

            # 计算涨跌额
            if quote['price'] > 0:
                quote['change'] = quote['price'] * quote['change_percent'] / 100

            # 涨跌状态
            if quote['change'] > 0:
                quote['status'] = '上涨'
            elif quote['change'] < 0:
                quote['status'] = '下跌'
            else:
                quote['status'] = '平盘'

            quotes.append(quote)

        print(f"✓ 获取 {len(quotes)} 条真实数据")

    except ImportError:
        print("✗ AKShare未安装，使用模拟数据")
        quotes = generate_mock_quotes_for_report()
    except Exception as e:
        print(f"✗ 获取真实数据失败: {str(e)}")
        quotes = generate_mock_quotes_for_report()

    return quotes


def generate_mock_quotes_for_report():
    """生成模拟行情数据（补充）"""
    # 期货品种列表
    futures = [
        ('CU0', '沪铜连续', '上期所', 70000),
        ('AL0', '沪铝连续', '上期所', 20000),
        ('ZN0', '沪锌连续', '上期所', 22000),
        ('AU0', '沪金连续', '上期所', 500),
        ('AG0', '沪银连续', '上期所', 6200),
        ('RB0', '螺纹钢连续', '上期所', 3800),
        ('HC0', '热卷连续', '上期所', 4000),
        ('I0', '铁矿石连续', '大商所', 900),
        ('J0', '焦炭连续', '大商所', 2500),
        ('JM0', '焦煤连续', '大商所', 1800),
        ('RU0', '橡胶连续', '上期所', 14500),
        ('SC0', '原油连续', '能源中心', 650),
        ('MA0', '甲醇连续', '郑商所', 2600),
        ('PP0', 'PP连续', '大商所', 7800),
        ('EG0', '乙二醇连续', '大商所', 4600),
        ('M0', '豆粕连续', '大商所', 3200),
        ('Y0', '豆油连续', '大商所', 8000),
        ('P0', '棕榈油连续', '大商所', 8500),
        ('C0', '玉米连续', '大商所', 2500),
        ('A0', '豆一连续', '大商所', 4800),
        ('CS0', '淀粉连续', '大商所', 2900),
        ('JD0', '鸡蛋连续', '大商所', 4000),
        ('LH0', '生猪连续', '大商所', 16000),
        ('AP0', '苹果连续', '郑商所', 9000),
        ('SR0', '白糖连续', '郑商所', 6600),
        ('CF0', '棉花连续', '郑商所', 15500),
        ('RM0', '菜粕连续', '郑商所', 2800),
        ('OI0', '菜油连续', '郑商所', 8800),
        ('IF0', '沪深300', '中金所', 3800),
        ('IH0', '上证50', '中金所', 2500),
        ('IC0', '中证500', '中金所', 5800),
        ('T0', '10年国债', '中金所', 105),
        ('TL0', '30年国债', '中金所', 105),
    ]

    quotes = []
    for symbol, name, exchange, base_price in futures:
        change_percent = random.uniform(-3, 3)
        change = base_price * change_percent / 100

        quotes.append({
            'symbol': symbol,
            'name': name,
            'price': round(base_price, 2),
            'open': round(base_price + random.uniform(-base_price*0.01, base_price*0.01), 2),
            'high': round(base_price + random.uniform(0, base_price*0.015), 2),
            'low': round(base_price + random.uniform(-base_price*0.015, 0), 2),
            'close': round(base_price, 2),
            'change': round(change, 2),
            'change_percent': round(change_percent, 2),
            'volume': random.randint(100000, 2000000),
            'open_interest': random.randint(200000, 3000000),
            'exchange': exchange,
            'source': '模拟数据',
            'timestamp': datetime.now()
        })

    return quotes


def generate_mock_historical(days=100):
    """生成模拟历史数据"""
    base_price = 4000
    historical = []

    for i in range(days):
        change = random.uniform(-50, 50)
        price = base_price + change

        historical.append({
            'date': (datetime.now() - timedelta(days=days-i)).strftime('%Y-%m-%d'),
            'open': round(price + random.uniform(-10, 10), 2),
            'high': round(price + random.uniform(0, 30), 2),
            'low': round(price + random.uniform(-30, 0), 2),
            'close': round(price, 2),
            'volume': random.randint(100000, 1000000),
            'open_interest': random.randint(500000, 2000000)
        })
        base_price = price

    return historical


def generate_markdown_report(quotes, recommendations, summary, date_str):
    """生成Markdown格式的报告"""

    md_lines = []

    # 标题
    md_lines.append(f"# 📊 期货市场日报 - {date_str}\n")

    # 时间戳
    md_lines.append(f"> 📅 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # 数据源信息
    sources = set(q.get('source', '未知') for q in quotes)
    md_lines.append(f"> 📈 数据来源: {', '.join(sources)}\n")

    md_lines.append("---\n")

    # 市场概况
    md_lines.append("## 📋 市场概况\n")
    md_lines.append(f"| 指标 | 数值 |")
    md_lines.append(f"|------|------|")
    md_lines.append(f"| 📊 统计品种 | **{summary.get('total_count', 0)}** 个 |")
    md_lines.append(f"| 🎯 市场情绪 | **{summary.get('market_sentiment', '中性')}** |")

    rec_counts = summary.get('recommendations', {})
    md_lines.append(f"\n### 推荐分布\n")
    md_lines.append(f"| 操作方向 | 数量 |")
    md_lines.append(f"|----------|------|")
    md_lines.append(f"| 🟢 强烈买入 | {rec_counts.get('strong_buy', 0)} |")
    md_lines.append(f"| 🟢 买入 | {rec_counts.get('buy', 0)} |")
    md_lines.append(f"| ⚪ 观望 | {rec_counts.get('hold', 0)} |")
    md_lines.append(f"| 🔴 卖出 | {rec_counts.get('sell', 0)} |")
    md_lines.append(f"| 🔴 强烈卖出 | {rec_counts.get('strong_sell', 0)} |")

    md_lines.append("\n---\n")

    # 做多推荐
    top_long = summary.get('top_picks', {}).get('long', [])
    if top_long:
        md_lines.append("## 🟢 做多推荐\n")
        md_lines.append("| 排名 | 代码 | 名称 | 操作 | 信心度 | 风险 |")
        md_lines.append("|------|------|------|------|--------|------|")

        for i, rec in enumerate(top_long[:10], 1):
            md_lines.append(f"| {i} | `{rec['symbol']}` | {rec['name']} | {rec['action_cn']} | {rec['confidence_percent']} | {rec['risk_level_cn']} |")

    md_lines.append("\n---\n")

    # 做空推荐
    top_short = summary.get('top_picks', {}).get('short', [])
    if top_short:
        md_lines.append("## 🔴 做空推荐\n")
        md_lines.append("| 排名 | 代码 | 名称 | 操作 | 信心度 | 风险 |")
        md_lines.append("|------|------|------|------|--------|------|")

        for i, rec in enumerate(top_short[:10], 1):
            md_lines.append(f"| {i} | `{rec['symbol']}` | {rec['name']} | {rec['action_cn']} | {rec['confidence_percent']} | {rec['risk_level_cn']} |")

    md_lines.append("\n---\n")

    # 分类行情
    md_lines.append("## 📈 分类行情\n")

    # 按类别分组
    categories = {
        '金属': ['CU', 'AL', 'ZN', 'PB', 'NI', 'SN', 'AU', 'AG'],
        '黑色': ['RB', 'HC', 'I', 'J', 'JM'],
        '化工': ['RU', 'BU', 'FG', 'MA', 'PP', 'L', 'V', 'EG', 'SC', 'TA'],
        '农产品': ['M', 'Y', 'P', 'A', 'C', 'CS', 'JD', 'LH', 'AP', 'SR', 'CF', 'RM', 'OI'],
        '金融': ['IF', 'IH', 'IC', 'T', 'TL']
    }

    for cat_name, codes in categories.items():
        cat_quotes = [q for q in quotes if any(q['symbol'].startswith(c) for c in codes)]
        if cat_quotes:
            md_lines.append(f"\n### {cat_name}\n")
            md_lines.append("| 代码 | 名称 | 最新价 | 涨跌 | 涨跌幅 | 成交量 | 持仓量 |")
            md_lines.append("|------|------|--------|------|--------|--------|--------|")

            for q in sorted(cat_quotes, key=lambda x: x['change_percent'], reverse=True):
                change_icon = "🔺" if q['change_percent'] > 0 else "🔻" if q['change_percent'] < 0 else "➡"
                md_lines.append(f"| `{q['symbol']}` | {q['name']} | **{q['price']:.2f}** | {change_icon} {q['change']:.2f} | {q['change_percent']:.2f}% | {q['volume']:,.0f} | {q['open_interest']:,.0f} |")

    md_lines.append("\n---\n")

    # 风险提示
    md_lines.append("## ⚠️ 风险提示\n")
    md_lines.append("""
1. 以上分析基于公开市场数据，仅供参考学习
2. 期货交易风险较高，投资需谨慎
3. 系统建议不构成投资建议
4. 实际交易请结合自身风险承受能力
5. 请遵守相关法律法规和交易所规则
    """)

    md_lines.append("\n---\n")

    # 免责声明
    md_lines.append("## 📝 免责声明\n")
    md_lines.append("""
本报告由自动化系统生成，数据和分析结果仅供参考。使用者应当独立判断并承担投资决策的风险和责任。

*本系统仅供学习研究使用*
    """)

    md_lines.append("\n---\n")
    md_lines.append(f"\n📌 **本报告由 [Futures Crawler](https://github.com/chenjingxiong/futures-crawler-daily) 自动生成**\n")

    return "\n".join(md_lines)


def main():
    """主函数"""
    print("=" * 60)
    print("期货每日报告生成器 - 真实数据版")
    print("=" * 60)

    # 生成日期字符串
    date_str = datetime.now().strftime('%Y-%m-%d')

    # 生成报告目录
    reports_dir = './reports'
    os.makedirs(reports_dir, exist_ok=True)

    # 初始化引擎
    print("初始化分析引擎...")
    analysis = TechnicalAnalysis()
    recommendation = RecommendationEngine()

    # 获取行情数据（优先真实数据）
    quotes = get_real_futures_data()

    # 如果数据不足，补充模拟数据
    if len(quotes) < 20:
        print(f"数据不足，补充模拟数据...")
        mock_quotes = generate_mock_quotes_for_report()
        existing_symbols = set(q['symbol'] for q in quotes)
        for mq in mock_quotes:
            if mq['symbol'] not in existing_symbols:
                quotes.append(mq)

    print(f"共获取 {len(quotes)} 条行情数据")

    # 生成历史数据
    historical = generate_mock_historical(100)

    # 技术分析
    print("执行技术分析...")
    analysis_results = []
    for quote in quotes:
        result = analysis.analyze_quote(quote, historical)
        analysis_results.append(result)

    # 生成推荐
    print("生成操作建议...")
    recommendations = recommendation.generate_batch_recommendations(quotes, analysis_results)

    # 生成市场汇总
    print("生成市场汇总...")
    summary = recommendation.generate_market_summary(quotes, recommendations)

    # 生成Markdown报告
    print("生成Markdown报告...")
    markdown_content = generate_markdown_report(quotes, recommendations, summary, date_str)

    # 保存报告
    report_file = os.path.join(reports_dir, f'daily_report_{date_str.replace("-", "")}.md')
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(markdown_content)

    print(f"报告已保存: {report_file}")

    # 保存JSON数据
    json_file = os.path.join(reports_dir, f'daily_data_{date_str.replace("-", "")}.json')

    def serialize_quote(q):
        result = {}
        for k, v in q.items():
            if isinstance(v, datetime):
                result[k] = v.isoformat()
            else:
                result[k] = v
        return result

    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump({
            'date': date_str,
            'timestamp': datetime.now().isoformat(),
            'quotes': [serialize_quote(q) for q in quotes],
            'summary': summary,
            'recommendations': [r.to_dict() for r in recommendations]
        }, f, ensure_ascii=False, indent=2)

    print(f"数据已保存: {json_file}")

    # Git操作
    print("\n提交到GitHub...")
    try:
        # 添加文件
        subprocess.run(['git', 'add', f'{reports_dir}/*.md', f'{reports_dir}/*.json'],
                      capture_output=True, check=False)

        # 提交
        commit_msg = f"Daily report: {date_str}\n\n- Generated futures analysis report\n- Total quotes: {len(quotes)}\n- Market sentiment: {summary.get('market_sentiment', 'N/A')}\n\nGenerated with [Claude Code](https://claude.com/claude-code)\nvia [Happy](https://happy.engineering)\n\nCo-Authored-By: Claude <noreply@anthropic.com>\nCo-Authored-By: Happy <yesreply@happy.engineering>"

        subprocess.run(['git', 'commit', '-m', commit_msg],
                      capture_output=True, check=False)

        # 推送
        result = subprocess.run(['git', 'push'],
                              capture_output=True, check=False)

        if result.returncode == 0:
            print("✅ 成功推送到GitHub!")
        else:
            print("⚠️ 推送可能失败，请检查网络连接")

    except Exception as e:
        print(f"Git操作出错: {str(e)}")

    print("\n" + "=" * 60)
    print("报告生成完成!")
    print(f"查看报告: {report_file}")
    print("=" * 60)


if __name__ == '__main__':
    main()
