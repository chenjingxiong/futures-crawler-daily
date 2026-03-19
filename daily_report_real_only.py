#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日期货报告生成脚本 - 仅使用真实数据版本
移除所有模拟数据，全部采用AKShare、Lightpanda等真实数据源
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List
import json
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


def get_real_quotes():
    """仅获取真实期货行情"""
    print("=" * 60)
    print("获取真实期货数据...")
    print("=" * 60)

    from real_data_only import RealFuturesDataManager

    manager = RealFuturesDataManager()
    quotes = manager.get_all_realtime_quotes()

    if not quotes:
        print("\n❌ 错误：无法获取任何真实数据！")
        print("请检查：")
        print("  1. 网络连接是否正常")
        print("  2. AKShare是否正确安装 (pip install akshare)")
        print("  3. 数据源API是否可访问")
        print("\n退出程序...")
        sys.exit(1)

    print(f"\n✅ 成功获取 {len(quotes)} 个品种的真实数据")
    print(f"   数据来源: {set(q.get('source', '未知') for q in quotes)}")

    return quotes


def get_real_historical_data(symbols: List[str]) -> Dict[str, List]:
    """获取真实历史数据"""
    print("\n获取历史数据...")

    from real_data_only import RealFuturesDataManager

    manager = RealFuturesDataManager()
    historical_data = {}

    # 为每个品种获取历史数据（限制数量以提高速度）
    for i, symbol in enumerate(symbols[:20]):  # 最多获取前20个品种的历史数据
        try:
            hist = manager.get_historical_data(symbol, days=100)
            if hist:
                historical_data[symbol] = hist
                print(f"  ✓ {symbol}: {len(hist)}条")
            else:
                # 如果获取失败，生成基于当前价格的技术分析模拟数据
                print(f"  ⚠ {symbol}: 使用实时数据进行分析")
        except Exception as e:
            print(f"  ✗ {symbol}: 获取失败")

    return historical_data


def generate_markdown_report(quotes, recommendations, summary, date_str):
    """生成Markdown格式的报告"""
    md_lines = []

    # 标题
    md_lines.append(f"# 📊 期货市场日报 - {date_str}\n")
    md_lines.append(f"> 📅 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # 数据源信息
    sources = set(q.get('source', '未知') for q in quotes)
    md_lines.append(f"> 📈 数据来源: {', '.join(sources)} (真实数据)\n")
    md_lines.append("> ⚠️ 本系统仅使用真实市场数据，不包含任何模拟数据\n")

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
        md_lines.append("| 排名 | 代码 | 名称 | 操作 | 信心度 | 风险 | 理由 |")
        md_lines.append("|------|------|------|------|--------|------|------|")

        for i, rec in enumerate(top_long[:15], 1):
            reason_short = rec['reason'][:30] + '...' if len(rec['reason']) > 30 else rec['reason']
            md_lines.append(f"| {i} | `{rec['symbol']}` | {rec['name']} | {rec['action_cn']} | {rec['confidence_percent']} | {rec['risk_level_cn']} | {reason_short} |")

    md_lines.append("\n---\n")

    # 做空推荐
    top_short = summary.get('top_picks', {}).get('short', [])
    if top_short:
        md_lines.append("## 🔴 做空推荐\n")
        md_lines.append("| 排名 | 代码 | 名称 | 操作 | 信心度 | 风险 | 理由 |")
        md_lines.append("|------|------|------|------|--------|------|------|")

        for i, rec in enumerate(top_short[:15], 1):
            reason_short = rec['reason'][:30] + '...' if len(rec['reason']) > 30 else rec['reason']
            md_lines.append(f"| {i} | `{rec['symbol']}` | {rec['name']} | {rec['action_cn']} | {rec['confidence_percent']} | {rec['risk_level_cn']} | {reason_short} |")

    md_lines.append("\n---\n")

    # 分类行情
    md_lines.append("## 📈 分类行情\n")

    # 按类别分组
    categories = {
        '贵金属': ['AU', 'AG'],
        '基本金属': ['CU', 'AL', 'ZN', 'PB', 'NI', 'SN'],
        '黑色系': ['RB', 'HC', 'I', 'J', 'JM'],
        '能源化工': ['RU', 'SC', 'MA', 'PP', 'L', 'V', 'EG', 'TA', 'BU', 'FG'],
        '农产品': ['M', 'Y', 'P', 'A', 'C', 'CS', 'JD', 'LH', 'AP', 'SR', 'CF', 'RM', 'OI'],
        '金融期货': ['IF', 'IH', 'IC', 'IM', 'T', 'TF', 'TS', 'TL']
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

    # 数据说明
    md_lines.append("## 📊 数据说明\n")
    md_lines.append(f"""
- 本报告全部使用真实市场数据
- 数据来源: {', '.join(sources)}
- 数据获取时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- 品种数量: {len(quotes)} 个
""")

    md_lines.append("\n---\n")

    # 风险提示
    md_lines.append("## ⚠️ 风险提示\n")
    md_lines.append("""
1. 以上分析基于真实市场数据，仅供参考学习
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
    md_lines.append(f"\n🔗 **数据来源**: AKShare、东方财富、新浪期货、Lightpanda\n")

    return "\n".join(md_lines)


def main():
    """主函数"""
    print("\n" + "=" * 60)
    print("📊 期货每日报告生成器 - 真实数据版")
    print("=" * 60)
    print("⚠️  注意：本系统仅使用真实数据，不包含任何模拟数据")
    print()

    # 生成日期字符串
    date_str = datetime.now().strftime('%Y-%m-%d')

    # 生成报告目录
    reports_dir = './reports'
    os.makedirs(reports_dir, exist_ok=True)

    # 初始化引擎
    print("初始化分析引擎...")
    analysis = TechnicalAnalysis()
    recommendation = RecommendationEngine()

    # 获取真实行情数据（强制使用真实数据）
    quotes = get_real_quotes()

    # 验证数据有效性
    valid_quotes = [q for q in quotes if q.get('price', 0) > 0]
    if len(valid_quotes) < len(quotes):
        print(f"⚠️  过滤了 {len(quotes) - len(valid_quotes)} 条无效数据")
    quotes = valid_quotes

    if not quotes:
        print("\n❌ 没有有效数据，无法生成报告")
        sys.exit(1)

    # 获取历史数据（用于技术分析）
    symbols = list(set([q['symbol'][:2] + '0' if len(q['symbol']) > 2 else q['symbol'] for q in quotes]))
    historical_data = get_real_historical_data(symbols)

    # 技术分析
    print("\n执行技术分析...")
    analysis_results = []

    for quote in quotes:
        symbol_base = quote['symbol'][:2] if len(quote['symbol']) > 2 else quote['symbol']

        # 使用历史数据或基于实时数据进行分析
        historical = historical_data.get(symbol_base, None)

        if not historical:
            # 如果没有历史数据，创建一个简化的分析（基于实时数据）
            result = analysis.analyze_quote(quote, [])
        else:
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

    print(f"\n✅ 报告已保存: {report_file}")

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
            'data_source': 'real_only',  # 标记为仅真实数据
            'quotes': [serialize_quote(q) for q in quotes],
            'summary': summary,
            'recommendations': [r.to_dict() for r in recommendations]
        }, f, ensure_ascii=False, indent=2)

    print(f"✅ 数据已保存: {json_file}")

    # Git操作
    print("\n提交到GitHub...")
    try:
        # 添加文件
        subprocess.run(['git', 'add', f'{reports_dir}/*.md', f'{reports_dir}/*.json'],
                      capture_output=True, check=False)

        # 提交
        sources_str = ', '.join(set(q.get('source', '未知') for q in quotes))
        commit_msg = f"""Daily report: {date_str} (Real Data Only)

- Generated futures analysis report
- Total quotes: {len(quotes)}
- Market sentiment: {summary.get('market_sentiment', 'N/A')}
- Data sources: {sources_str}
- All data is REAL, no simulation

Generated with [Claude Code](https://claude.com/claude-code)
via [Happy](https://happy.engineering)

Co-Authored-By: Claude <noreply@anthropic.com>
Co-Authored-By: Happy <yesreply@happy.engineering>"""

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
        print(f"⚠️ Git操作出错: {str(e)}")

    print("\n" + "=" * 60)
    print("✅ 报告生成完成!")
    print(f"📄 查看报告: {report_file}")
    print(f"🌐 GitHub: https://github.com/chenjingxiong/futures-crawler-daily")
    print("=" * 60)


if __name__ == '__main__':
    main()
