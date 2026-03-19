# -*- coding: utf-8 -*-
"""
操作推荐引擎
基于技术分析和市场情绪生成操作建议
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from collections import defaultdict

try:
    from .technical_analysis import TechnicalAnalysis
except ImportError:
    from technical_analysis import TechnicalAnalysis


class TradingSignal:
    """交易信号类"""

    def __init__(self, symbol: str, name: str, action: str,
                 confidence: float, reason: str, risk_level: str = 'medium'):
        """
        初始化交易信号

        Args:
            symbol: 品种代码
            name: 品种名称
            action: 操作方向 (buy, sell, hold)
            confidence: 信心度 (0-1)
            reason: 原因
            risk_level: 风险等级 (low, medium, high)
        """
        self.symbol = symbol
        self.name = name
        self.action = action
        self.confidence = confidence
        self.reason = reason
        self.risk_level = risk_level
        self.timestamp = datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'symbol': self.symbol,
            'name': self.name,
            'action': self.action,
            'action_cn': self._get_action_cn(),
            'confidence': self.confidence,
            'confidence_percent': f"{self.confidence * 100:.1f}%",
            'reason': self.reason,
            'risk_level': self.risk_level,
            'risk_level_cn': self._get_risk_cn(),
            'timestamp': self.timestamp.isoformat()
        }

    def _get_action_cn(self) -> str:
        """获取中文操作名称"""
        action_map = {
            'strong_buy': '强烈买入',
            'buy': '买入',
            'hold': '持有观望',
            'sell': '卖出',
            'strong_sell': '强烈卖出'
        }
        return action_map.get(self.action, self.action)

    def _get_risk_cn(self) -> str:
        """获取中文风险等级"""
        risk_map = {
            'low': '低风险',
            'medium': '中等风险',
            'high': '高风险'
        }
        return risk_map.get(self.risk_level, self.risk_level)


class RecommendationEngine:
    """推荐引擎"""

    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化推荐引擎

        Args:
            config: 配置字典
        """
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        self.technical_analysis = TechnicalAnalysis(config)

        # 推荐配置
        rec_config = self.config.get('recommendation', {})
        self.bullish_signals = rec_config.get('bullish_signals', {})
        self.bearish_signals = rec_config.get('bearish_signals', {})
        self.thresholds = rec_config.get('thresholds', {})

    def generate_recommendation(self, quote: Dict[str, Any],
                                analysis: Dict[str, Any] = None) -> TradingSignal:
        """
        生成单个品种的推荐

        Args:
            quote: 行情数据
            analysis: 技术分析结果

        Returns:
            交易信号
        """
        symbol = quote.get('symbol', '')
        name = quote.get('name', '')

        # 如果没有提供分析，进行技术分析
        if analysis is None:
            analysis = self.technical_analysis.analyze_quote(quote)

        # 计算综合得分
        score = self._calculate_score(analysis)

        # 确定操作方向
        action = self._determine_action(score)

        # 计算信心度
        confidence = self._calculate_confidence(analysis, score)

        # 获取风险等级
        risk_level = self._assess_risk(quote, analysis)

        # 生成原因说明
        reason = self._generate_reason(analysis, action)

        return TradingSignal(
            symbol=symbol,
            name=name,
            action=action,
            confidence=confidence,
            reason=reason,
            risk_level=risk_level
        )

    def _calculate_score(self, analysis: Dict[str, Any]) -> float:
        """计算综合得分"""
        signals = analysis.get('signals', [])
        score = 0

        for signal in signals:
            strength = signal.get('strength', 0)
            is_bullish = signal.get('bullish', True)

            if is_bullish:
                score += strength
            else:
                score -= strength

        # 考虑趋势强度
        strength = analysis.get('strength', 0)
        score += strength * 0.5

        return score

    def _determine_action(self, score: float) -> str:
        """根据得分确定操作方向"""
        strong_buy = self.thresholds.get('strong_buy', 4)
        buy = self.thresholds.get('buy', 2)
        sell = self.thresholds.get('sell', -2)
        strong_sell = self.thresholds.get('strong_sell', -4)

        if score >= strong_buy:
            return 'strong_buy'
        elif score >= buy:
            return 'buy'
        elif score <= strong_sell:
            return 'strong_sell'
        elif score <= sell:
            return 'sell'
        else:
            return 'hold'

    def _calculate_confidence(self, analysis: Dict[str, Any],
                              score: float) -> float:
        """计算信心度"""
        base_confidence = 0.5

        # 信号数量越多，信心度越高
        signals = analysis.get('signals', [])
        signal_count = len(signals)
        base_confidence += min(signal_count * 0.05, 0.2)

        # 得分绝对值越大，信心度越高
        score_abs = abs(score)
        base_confidence += min(score_abs * 0.1, 0.2)

        # 技术指标完整性
        indicators = analysis.get('indicators', {})
        indicator_count = len([v for v in indicators.values() if v != 0])
        base_confidence += min(indicator_count * 0.03, 0.1)

        return min(max(base_confidence, 0.3), 0.95)

    def _assess_risk(self, quote: Dict[str, Any],
                     analysis: Dict[str, Any]) -> str:
        """评估风险等级"""
        risk_score = 0

        # 价格波动率
        price = quote.get('price', 0)
        high = quote.get('high', 0)
        low = quote.get('low', 0)

        if price > 0 and high > 0 and low > 0:
            volatility = (high - low) / price
            risk_score += volatility * 50

        # RSI极值风险
        indicators = analysis.get('indicators', {})
        rsi = indicators.get('rsi', 50)

        if rsi > 80 or rsi < 20:
            risk_score += 2

        # 趋势风险
        trend = analysis.get('trend', '')
        if '强烈' in trend:
            risk_score += 1

        # 确定风险等级
        if risk_score < 3:
            return 'low'
        elif risk_score < 6:
            return 'medium'
        else:
            return 'high'

    def _generate_reason(self, analysis: Dict[str, Any], action: str) -> str:
        """生成推荐原因"""
        reasons = []

        # 添加趋势信息
        trend = analysis.get('trend', '中性')
        if trend != '中性':
            reasons.append(f"技术趋势{trend}")

        # 添加主要信号
        signals = analysis.get('signals', [])
        bullish_signals = [s for s in signals if s.get('bullish', True)]
        bearish_signals = [s for s in signals if not s.get('bullish', True)]

        if action in ['strong_buy', 'buy']:
            top_signals = sorted(bullish_signals,
                               key=lambda x: x.get('strength', 0),
                               reverse=True)[:3]
            for s in top_signals:
                reasons.append(s.get('name', ''))
        elif action in ['strong_sell', 'sell']:
            top_signals = sorted(bearish_signals,
                               key=lambda x: x.get('strength', 0),
                               reverse=True)[:3]
            for s in top_signals:
                reasons.append(s.get('name', ''))

        # 添加技术指标信息
        indicators = analysis.get('indicators', {})
        indicator_info = []

        rsi = indicators.get('rsi')
        if rsi:
            if rsi > 70:
                indicator_info.append(f"RSI超买({rsi:.1f})")
            elif rsi < 30:
                indicator_info.append(f"RSI超卖({rsi:.1f})")

        macd_hist = indicators.get('macd_histogram')
        if macd_hist is not None:
            if macd_hist > 0:
                indicator_info.append("MACD多头")
            else:
                indicator_info.append("MACD空头")

        if indicator_info:
            reasons.extend(indicator_info)

        return "；".join(reasons) if reasons else "综合分析结果"

    def generate_batch_recommendations(self,
                                       quotes: List[Dict[str, Any]],
                                       analysis_results: List[Dict[str, Any]] = None) -> List[TradingSignal]:
        """
        批量生成推荐

        Args:
            quotes: 行情列表
            analysis_results: 技术分析结果列表

        Returns:
            交易信号列表
        """
        recommendations = []

        if analysis_results is None:
            analysis_results = self.technical_analysis.batch_analyze(quotes)

        for i, quote in enumerate(quotes):
            analysis = analysis_results[i] if i < len(analysis_results) else None
            signal = self.generate_recommendation(quote, analysis)
            recommendations.append(signal)

        return recommendations

    def generate_market_summary(self, quotes: List[Dict[str, Any]],
                                recommendations: List[TradingSignal] = None) -> Dict[str, Any]:
        """
        生成市场汇总

        Args:
            quotes: 行情列表
            recommendations: 推荐列表

        Returns:
            市场汇总
        """
        if recommendations is None:
            recommendations = self.generate_batch_recommendations(quotes)

        summary = {
            'timestamp': datetime.now().isoformat(),
            'total_count': len(quotes),
            'recommendations': {
                'strong_buy': 0,
                'buy': 0,
                'hold': 0,
                'sell': 0,
                'strong_sell': 0
            },
            'top_picks': {
                'long': [],
                'short': []
            },
            'high_risk': [],
            'market_sentiment': '中性'
        }

        # 统计推荐分布
        for rec in recommendations:
            action = rec.action
            summary['recommendations'][action] = summary['recommendations'].get(action, 0) + 1

            # 收集高信心推荐
            if rec.confidence >= 0.7:
                if action in ['strong_buy', 'buy']:
                    summary['top_picks']['long'].append(rec.to_dict())
                elif action in ['strong_sell', 'sell']:
                    summary['top_picks']['short'].append(rec.to_dict())

            # 收集高风险品种
            if rec.risk_level == 'high':
                summary['high_risk'].append(rec.to_dict())

        # 排序并限制数量
        summary['top_picks']['long'].sort(key=lambda x: x['confidence'], reverse=True)
        summary['top_picks']['long'] = summary['top_picks']['long'][:10]

        summary['top_picks']['short'].sort(key=lambda x: x['confidence'], reverse=True)
        summary['top_picks']['short'] = summary['top_picks']['short'][:10]

        summary['high_risk'] = summary['high_risk'][:5]

        # 判断市场情绪
        bullish_count = summary['recommendations']['strong_buy'] + summary['recommendations']['buy']
        bearish_count = summary['recommendations']['strong_sell'] + summary['recommendations']['sell']

        if bullish_count > bearish_count * 1.5:
            summary['market_sentiment'] = '偏多'
        elif bearish_count > bullish_count * 1.5:
            summary['market_sentiment'] = '偏空'
        else:
            summary['market_sentiment'] = '中性'

        return summary

    def format_report(self, quotes: List[Dict[str, Any]],
                      recommendations: List[TradingSignal] = None,
                      summary: Dict[str, Any] = None) -> str:
        """
        格式化报告

        Args:
            quotes: 行情列表
            recommendations: 推荐列表
            summary: 市场汇总

        Returns:
            格式化报告文本
        """
        if recommendations is None:
            recommendations = self.generate_batch_recommendations(quotes)

        if summary is None:
            summary = self.generate_market_summary(quotes, recommendations)

        lines = []
        lines.append("=" * 60)
        lines.append(f"期货市场操作建议报告")
        lines.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 60)
        lines.append("")

        # 市场概况
        lines.append("【市场概况】")
        lines.append(f"  统计品种: {summary['total_count']} 个")
        lines.append(f"  市场情绪: {summary['market_sentiment']}")
        lines.append("")

        # 推荐分布
        lines.append("【推荐分布】")
        rec_counts = summary['recommendations']
        lines.append(f"  强烈买入: {rec_counts.get('strong_buy', 0)} 个")
        lines.append(f"  买入: {rec_counts.get('buy', 0)} 个")
        lines.append(f"  观望: {rec_counts.get('hold', 0)} 个")
        lines.append(f"  卖出: {rec_counts.get('sell', 0)} 个")
        lines.append(f"  强烈卖出: {rec_counts.get('strong_sell', 0)} 个")
        lines.append("")

        # 多头推荐
        top_long = summary.get('top_picks', {}).get('long', [])
        if top_long:
            lines.append("【做多推荐 TOP10】")
            for i, rec in enumerate(top_long, 1):
                lines.append(f"  {i}. {rec['symbol']} {rec['name']} - {rec['action_cn']}")
                lines.append(f"     信心度: {rec['confidence_percent']} | 风险: {rec['risk_level_cn']}")
                lines.append(f"     理由: {rec['reason']}")
                lines.append("")

        # 空头推荐
        top_short = summary.get('top_picks', {}).get('short', [])
        if top_short:
            lines.append("【做空推荐 TOP10】")
            for i, rec in enumerate(top_short, 1):
                lines.append(f"  {i}. {rec['symbol']} {rec['name']} - {rec['action_cn']}")
                lines.append(f"     信心度: {rec['confidence_percent']} | 风险: {rec['risk_level_cn']}")
                lines.append(f"     理由: {rec['reason']}")
                lines.append("")

        # 风险提示
        high_risk = summary.get('high_risk', [])
        if high_risk:
            lines.append("【高风险品种】")
            for rec in high_risk:
                lines.append(f"  {rec['symbol']} {rec['name']} - {rec['action_cn']} ({rec['risk_level_cn']})")
            lines.append("")

        # 免责声明
        lines.append("【风险提示】")
        lines.append("  以上建议仅供参考，不构成投资建议。")
        lines.append("  期货交易存在风险，投资需谨慎。")
        lines.append("  请根据自身风险承受能力做出投资决策。")
        lines.append("")
        lines.append("=" * 60)

        return "\n".join(lines)
