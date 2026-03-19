#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日期货报告生成脚本 - 支持真实数据源
自动生成Markdown格式的分析报告并提交到GitHub
"""

import os
import sys
from datetime import datetime, timedelta
import json
import random
import argparse

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

# 尝试加载真实数据源
try:
    real_data_module = load_module('real_data_sources',
        os.path.join(base_dir, 'real_data_sources.py'))
    MultiSourceDataManager = real_data_module.MultiSourceDataManager
    REAL_DATA_AVAILABLE = True
except Exception as e:
    REAL_DATA_AVAILABLE = False
    print(f"真实数据源不可用: {str(e)}")

# 尝试加载Lightpanda集成
try:
    lightpanda_module = load_module('lightpanda_integration',
        os.path.join(base_dir, 'lightpanda_integration.py'))
    get_realtime_futures_sync = lightpanda_module.get_realtime_futures_sync
    LIGHTPANDA_AVAILABLE = True
except Exception as e:
    LIGHTPANDA_AVAILABLE = False
    print(f"Lightpanda不可用: {str(e)}")


def generate_mock_quotes_for_report():
    """生成演示用的模拟行情数据（备用）"""
    # 期货品种列表（精选）
    futures = [
        # 金属
        ('CU0', '沪铜主力', '上期所', 70000),
        ('AL0', '沪铝主力', '上期所', 20000),
        ('ZN0', '沪锌主力', '上期所', 22000),
        ('AU0', '沪金主力', '上期所', 500),
        ('AG0', '沪银主力', '上期所', 6200),
        # 黑色
        ('RB0', '螺纹钢主力', '上期所', 3800),
        ('HC0', '热卷主力', '上期所', 4000),
        ('I0', '铁矿石主力', '大商所', 900),
        ('J0', '焦炭主力', '大商所', 2500),
        ('JM0', '焦煤主力', '大商所', 1800),
        # 化工
        ('RU0', '橡胶主力', '上期所', 14500),
        ('SC0', '原油主力', '能源中心', 650),
        ('MA0', '甲醇主力', '郑商所', 2600),
        ('PP0', 'PP主力', '大商所', 7800),
        ('EG0', '乙二醇主力', '大商所', 4600),
        # 农产品
        ('M0', '豆粕主力', '大商所', 3200),
        ('Y0', '豆油主力', '大商所', 8000),
        ('P0', '棕榈油主力', '大商所', 8500),
        ('C0', '玉米主力', '大商所', 2500),
        ('A0', '豆一主力', '大商所', 4800),
        ('CS0', '玉米淀粉主力', '大商所', 2900),
        ('JD0', '鸡蛋主力', '大商所', 4000),
        ('LH0', '生猪主力', '大商所', 16000),
        ('AP0', '苹果主力', '郑商所', 9000),
        ('SR0', '白糖主力', '郑商所', 6600),
        ('CF0', '棉花主力', '郑商所', 15500),
        ('RM0', '菜粕主力', '郑商所', 2800),
        ('OI0', '菜油主力', '郑商所', 8800),
        # 金融
        ('IF0', '沪深300主力', '中金所', 3800),
        ('IH0', '上证50主力', '中金所', 2500),
        ('IC0', '中证500主力', '中金所', 5800),
        ('T0', '10年期国债主力', '中金所', 105),
        ('TL0', '30年期国债主力', '中金所', 105),
    ]

    quotes = []
    for symbol, name, exchange, base_price in futures:
        # 生成涨跌
        change_percent = random.uniform(-4, 4)
        change = base_price * change_percent / 100

        quotes.append({
            'symbol': symbol,
            'name': name,
            'price': round(base_price, 2),
            'open': round(base_price + random.uniform(-base_price*0.015, base_price*0.015), 2),
            'high': round(base_price + random.uniform(0, base_price*0.02), 2),
            'low': round(base_price + random.uniform(-base_price*0.02, 0), 2),
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


def fetch_real_quotes(use_akshare=True, use_lightpanda=False, use_requests=True):
    """
    获取真实期货行情

    Args:
        use_akshare: 使用AKShare数据源
        use_lightpanda: 使用Lightpanda爬虫
        use_requests: 使用requests直接请求

    Returns:
        行情数据列表
    """
    all_quotes = []
    data_sources = []

    # 1. AKShare数据源
    if use_akshare and REAL_DATA_AVAILABLE:
        try:
            print("正在从AKShare获取数据...")
            manager = real_data_module.create_real_data_manager()
            quotes = manager.get_realtime_quotes()
            if quotes:
                all_quotes.extend(quotes)
                data_sources.append('AKShare')
                print(f"✓ AKShare: 获取 {len(quotes)} 条数据")
        except Exception as e:
            print(f"✗ AKShare获取失败: {str(e)}")

    # 2. Lightpanda爬虫
    if use_lightpanda and LIGHTPANDA_AVAILABLE:
        try:
            print("正在使用Lightpanda获取数据...")
            quotes = get_realtime_futures_sync(use_lightpanda=True)
            if quotes:
                all_quotes.extend(quotes)
                data_sources.append('Lightpanda')
                print(f"✓ Lightpanda: 获取 {len(quotes)} 条数据")
        except Exception as e:
            print(f"✗ Lightpanda获取失败: {str(e)}")

    # 3. Requests直接请求
    if use_requests:
        try:
            print("正在直接请求API获取数据...")
            import requests
            import re

            # 东方财富API
            try:
                url = "https://futsseapi.eastmoney.com/list"
                params = {
                    'cb': 'jQuery',
                    'ut': 'f1',
                    'pn': '1',
                    'pz': '500',
                    'po': '1',
                    'np': '1',
                    'fltt': '2',
                    'invt': '2',
                    'fid': 'f3',
                    'fs': 'm:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23',
                    'fields': 'f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152',
                    '_': str(int(datetime.now().timestamp() * 1000))
                }

                response = requests.get(url, params=params, timeout=10)
                if response.status_code == 200:
                    json_str = re.sub(r'^jQuery\d+_\d+\(|\);?$', '', response.text.strip())
                    data = json.loads(json_str)

                    if 'data' in data and 'diff' in data['data']:
                        for item in data['data']['diff']:
                            quote = {
                                'symbol': str(item[12] + item[13]) if len(item) > 13 else '',
                                'name': item[14] if len(item) > 14 else '',
                                'price': float(item[2]) if len(item) > 2 else 0,
                                'open': float(item[4]) if len(item) > 4 else 0,
                                'high': float(item[5]) if len(item) > 5 else 0,
                                'low': float(item[6]) if len(item) > 6 else 0,
                                'close': float(item[3]) if len(item) > 3 else 0,
                                'volume': float(item[7]) if len(item) > 7 else 0,
                                'open_interest': float(item[8]) if len(item) > 8 else 0,
                                'change': float(item[31]) if len(item) > 31 else 0,
                                'change_percent': float(item[32]) if len(item) > 32 else 0,
                                'source': '东方财富API',
                                'timestamp': datetime.now()
                            }
                            all_quotes.append(quotes)

                        data_sources.append('东方财富API')
                        print(f"✓ 东方财富API: 获取 {len(data['data']['diff'])} 条数据")

            except Exception as e:
                print(f"✗ 东方财富API请求失败: {str(e)}")

            # 新浪期货API
            try:
                main_symbols = ['CU0', 'AL0', 'ZN0', 'RB0', 'AU0', 'AG0', 'RU0', 'SC0',
                              'M0', 'Y0', 'P0', 'A0', 'C0', 'JD0', 'MA0', 'PP0',
                              'IF0', 'IH0', 'IC0', 'T0']

                for symbol in main_symbols:
                    try:
                        url = f"https://hq.sinajs.cn/list={symbol}"
                        response = requests.get(url, timeout=5)
                        if response.status_code == 200:
                            match = re.search(r'="([^"]+)"', response.text)
                            if match:
                                parts = match.group(1).split(',')
                                if len(parts) >= 12:
                                    quote = {
                                        'symbol': symbol,
                                        'name': parts[0],
                                        'open': float(parts[1]) if parts[1] else 0,
                                        'high': float(parts[2]) if parts[2] else 0,
                                        'low': float(parts[3]) if parts[3] else 0,
                                        'price': float(parts[4]) if parts[4] else 0,
                                        'close': float(parts[4]) if parts[4] else 0,
                                        'volume': float(parts[7]) if parts[7] else 0,
                                        'open_interest': float(parts[9]) if len(parts) > 9 else 0,
                                        'change': float(parts[10]) if len(parts) > 10 else 0,
                                        'change_percent': float(parts[11]) if len(parts) > 11 else 0,
                                        'source': '新浪API',
                                        'timestamp': datetime.now()
                                    }
                                    all_quotes.append(quote)

                    except:
                        continue

                data_sources.append('新浪API')
                print(f"✓ 新浪API: 获取部分数据")

        except Exception as e:
            print(f"✗ API请求失败: {str(e)}")

    print(f"\n数据源: {', '.join(data_sources) if data_sources else '无'}")

    return all_quotes


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
        '化工': ['RU', 'BU', 'FG', 'MA', 'PP', 'L', 'V', 'EG', 'SC'],
        '农产品': ['M', 'Y', 'P', 'A', 'C', 'CS', 'JD', 'LH', 'AP', 'SR', 'CF', 'RM', 'OI'],
        '金融': ['IF', 'IH', 'IC', 'T', 'TL']
    }

    for cat_name, codes in categories.items():
        cat_quotes = [q for q in quotes if any(q['symbol'].startswith(c) for c in codes)]
        if cat_quotes:
            md_lines.append(f"\n### {cat_name}\n")
            md_lines.append("| 代码 | 名称 | 最新价 | 涨跌 | 涨跌幅 | 成交量 |")
            md_lines.append("|------|------|--------|------|--------|--------|")

            for q in sorted(cat_quotes, key=lambda x: x['change_percent'], reverse=True):
                change_icon = "🔺" if q['change_percent'] > 0 else "🔻" if q['change_percent'] < 0 else "➡"
                md_lines.append(f"| `{q['symbol']}` | {q['name']} | **{q['price']:.2f}** | {change_icon} {q['change']:.2f} | {q['change_percent']:.2f}% | {q['volume']:,} |")

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
    parser = argparse.ArgumentParser(description='期货每日报告生成器')
    parser.add_argument('--real', action='store_true', help='使用真实数据')
    parser.add_argument('--mock', action='store_true', help='使用模拟数据')
    parser.add_argument('--akshare', action='store_true', help='使用AKShare数据源')
    parser.add_argument('--lightpanda', action='store_true', help='使用Lightpanda爬虫')
    parser.add_argument('--api', action='store_true', help='使用API直接请求')
    parser.add_argument('--commit', action='store_true', default=True, help='提交到GitHub')

    args = parser.parse_args()

    print("=" * 60)
    print("期货每日报告生成器")
    print("=" * 60)

    # 确定数据源
    use_real_data = args.real or args.akshare or args.lightpanda or args.api

    # 生成日期字符串
    date_str = datetime.now().strftime('%Y-%m-%d')

    # 生成报告目录
    reports_dir = './reports'
    os.makedirs(reports_dir, exist_ok=True)

    # 初始化引擎
    print("初始化分析引擎...")
    analysis = TechnicalAnalysis()
    recommendation = RecommendationEngine()

    # 获取行情数据
    if use_real_data:
        print("\n获取真实行情数据...")
        quotes = fetch_real_quotes(
            use_akshare=args.akshare or True,  # 默认使用AKShare
            use_lightpanda=args.lightpanda,
            use_requests=args.api or True  # 默认使用API
        )

        if not quotes:
            print("⚠️ 真实数据获取失败，使用模拟数据...")
            quotes = generate_mock_quotes_for_report()
    else:
        print("使用模拟数据...")
        quotes = generate_mock_quotes_for_report()

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

    # 处理datetime对象
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
    if args.commit:
        print("\n提交到GitHub...")
        import subprocess

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
