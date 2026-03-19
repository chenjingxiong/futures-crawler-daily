# -*- coding: utf-8 -*-
"""
技术分析模块
实现各种技术指标计算和分析
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import statistics

import numpy as np
import pandas as pd
import pandas_ta as ta


class TechnicalIndicators:
    """技术指标计算类"""

    @staticmethod
    def sma(data: List[float], period: int) -> List[float]:
        """
        简单移动平均线 (SMA)

        Args:
            data: 价格序列
            period: 周期

        Returns:
            SMA序列
        """
        df = pd.DataFrame({'price': data})
        df['sma'] = df['price'].rolling(window=period).mean()
        return df['sma'].fillna(0).tolist()

    @staticmethod
    def ema(data: List[float], period: int) -> List[float]:
        """
        指数移动平均线 (EMA)

        Args:
            data: 价格序列
            period: 周期

        Returns:
            EMA序列
        """
        df = pd.DataFrame({'price': data})
        df['ema'] = df['price'].ewm(span=period, adjust=False).mean()
        return df['ema'].fillna(0).tolist()

    @staticmethod
    def macd(data: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Dict[str, List[float]]:
        """
        MACD指标

        Args:
            data: 价格序列
            fast: 快线周期
            slow: 慢线周期
            signal: 信号线周期

        Returns:
            {'macd': [...], 'signal': [...], 'histogram': [...]}
        """
        df = pd.DataFrame({'price': data})

        # 计算MACD
        ema_fast = df['price'].ewm(span=fast, adjust=False).mean()
        ema_slow = df['price'].ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line

        return {
            'macd': macd_line.fillna(0).tolist(),
            'signal': signal_line.fillna(0).tolist(),
            'histogram': histogram.fillna(0).tolist()
        }

    @staticmethod
    def rsi(data: List[float], period: int = 14) -> List[float]:
        """
        相对强弱指标 (RSI)

        Args:
            data: 价格序列
            period: 周期

        Returns:
            RSI序列
        """
        df = pd.DataFrame({'price': data})

        # 计算价格变化
        delta = df['price'].diff()

        # 分离上涨和下跌
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)

        # 计算平均涨跌幅
        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()

        # 计算RSI
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        return rsi.fillna(50).tolist()

    @staticmethod
    def bollinger_bands(data: List[float], period: int = 20, std_dev: float = 2) -> Dict[str, List[float]]:
        """
        布林带 (BOLL)

        Args:
            data: 价格序列
            period: 周期
            std_dev: 标准差倍数

        Returns:
            {'upper': [...], 'middle': [...], 'lower': [...]}
        """
        df = pd.DataFrame({'price': data})

        # 中轨 = SMA
        middle = df['price'].rolling(window=period).mean()

        # 标准差
        std = df['price'].rolling(window=period).std()

        # 上下轨
        upper = middle + (std * std_dev)
        lower = middle - (std * std_dev)

        return {
            'upper': upper.fillna(0).tolist(),
            'middle': middle.fillna(0).tolist(),
            'lower': lower.fillna(0).tolist()
        }

    @staticmethod
    def kdj(high: List[float], low: List[float], close: List[float],
            n: int = 9, m1: int = 3, m2: int = 3) -> Dict[str, List[float]]:
        """
        KDJ指标

        Args:
            high: 最高价序列
            low: 最低价序列
            close: 收盘价序列
            n: RSV周期
            m1: K值平滑周期
            m2: D值平滑周期

        Returns:
            {'k': [...], 'd': [...], 'j': [...]}
        """
        df = pd.DataFrame({
            'high': high,
            'low': low,
            'close': close
        })

        # 计算RSV
        low_n = df['low'].rolling(window=n).min()
        high_n = df['high'].rolling(window=n).max()
        rsv = (df['close'] - low_n) / (high_n - low_n) * 100

        # 计算K值
        k = rsv.ewm(com=m1 - 1, adjust=False).mean()

        # 计算D值
        d = k.ewm(com=m2 - 1, adjust=False).mean()

        # 计算J值
        j = 3 * k - 2 * d

        return {
            'k': k.fillna(50).tolist(),
            'd': d.fillna(50).tolist(),
            'j': j.fillna(50).tolist()
        }

    @staticmethod
    def atr(high: List[float], low: List[float], close: List[float],
            period: int = 14) -> List[float]:
        """
        平均真实波幅 (ATR)

        Args:
            high: 最高价序列
            low: 最低价序列
            close: 收盘价序列
            period: 周期

        Returns:
            ATR序列
        """
        df = pd.DataFrame({
            'high': high,
            'low': low,
            'close': close
        })

        # 计算真实波幅
        prev_close = df['close'].shift(1)
        tr1 = df['high'] - df['low']
        tr2 = abs(df['high'] - prev_close)
        tr3 = abs(df['low'] - prev_close)
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        # 计算ATR
        atr = tr.rolling(window=period).mean()

        return atr.fillna(0).tolist()


class TechnicalAnalysis:
    """技术分析引擎"""

    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化技术分析

        Args:
            config: 配置字典
        """
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        self.indicators = TechnicalIndicators()

    def analyze_quote(self, quote: Dict[str, Any],
                     historical: List[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        分析单个行情

        Args:
            quote: 行情数据
            historical: 历史数据（用于计算技术指标）

        Returns:
            分析结果
        """
        analysis = {
            'symbol': quote.get('symbol', ''),
            'name': quote.get('name', ''),
            'timestamp': datetime.now().isoformat(),
            'price': quote.get('price', 0),
            'change': quote.get('change', 0),
            'change_percent': quote.get('change_percent', 0),
            'signals': [],
            'indicators': {},
            'trend': '',
            'strength': 0
        }

        # 基本信号
        analysis['signals'].extend(self._get_basic_signals(quote))

        # 如果有历史数据，计算技术指标
        if historical and len(historical) >= 20:
            analysis['indicators'] = self._calculate_indicators(historical)
            analysis['signals'].extend(self._get_technical_signals(
                quote,
                analysis['indicators']
            ))

        # 综合分析
        analysis['trend'], analysis['strength'] = self._synthesize_signals(analysis['signals'])

        return analysis

    def _get_basic_signals(self, quote: Dict[str, Any]) -> List[Dict[str, Any]]:
        """获取基本信号"""
        signals = []
        price = quote.get('price', 0)
        change = quote.get('change', 0)
        change_percent = quote.get('change_percent', 0)
        volume = quote.get('volume', 0)

        # 价格趋势
        if change_percent > 0.05:
            signals.append({
                'type': 'trend',
                'name': '强势上涨',
                'strength': 2,
                'bullish': True
            })
        elif change_percent > 0.02:
            signals.append({
                'type': 'trend',
                'name': '温和上涨',
                'strength': 1,
                'bullish': True
            })
        elif change_percent < -0.05:
            signals.append({
                'type': 'trend',
                'name': '强势下跌',
                'strength': 2,
                'bullish': False
            })
        elif change_percent < -0.02:
            signals.append({
                'type': 'trend',
                'name': '温和下跌',
                'strength': 1,
                'bullish': False
            })

        # 成交量
        if volume > 0:
            # 这里可以结合历史成交量判断放量/缩量
            pass

        return signals

    def _calculate_indicators(self, historical: List[Dict[str, Any]]) -> Dict[str, Any]:
        """计算技术指标"""
        if not historical:
            return {}

        # 提取数据
        closes = [h.get('close', 0) for h in historical]
        highs = [h.get('high', 0) for h in historical]
        lows = [h.get('low', 0) for h in historical]
        volumes = [h.get('volume', 0) for h in historical]

        indicators = {}

        try:
            # MA
            indicators['ma5'] = self.indicators.sma(closes, 5)[-1] if len(closes) >= 5 else 0
            indicators['ma10'] = self.indicators.sma(closes, 10)[-1] if len(closes) >= 10 else 0
            indicators['ma20'] = self.indicators.sma(closes, 20)[-1] if len(closes) >= 20 else 0
            indicators['ma60'] = self.indicators.sma(closes, 60)[-1] if len(closes) >= 60 else 0

            # EMA
            indicators['ema12'] = self.indicators.ema(closes, 12)[-1] if len(closes) >= 12 else 0
            indicators['ema26'] = self.indicators.ema(closes, 26)[-1] if len(closes) >= 26 else 0

            # MACD
            macd_result = self.indicators.macd(closes)
            if macd_result['macd']:
                indicators['macd'] = macd_result['macd'][-1]
                indicators['macd_signal'] = macd_result['signal'][-1]
                indicators['macd_histogram'] = macd_result['histogram'][-1]

            # RSI
            rsi_result = self.indicators.rsi(closes)
            if rsi_result:
                indicators['rsi'] = rsi_result[-1]

            # BOLL
            boll_result = self.indicators.bollinger_bands(closes)
            if boll_result['upper']:
                indicators['boll_upper'] = boll_result['upper'][-1]
                indicators['boll_middle'] = boll_result['middle'][-1]
                indicators['boll_lower'] = boll_result['lower'][-1]

            # KDJ
            kdj_result = self.indicators.kdj(highs, lows, closes)
            if kdj_result['k']:
                indicators['kdj_k'] = kdj_result['k'][-1]
                indicators['kdj_d'] = kdj_result['d'][-1]
                indicators['kdj_j'] = kdj_result['j'][-1]

            # ATR
            atr_result = self.indicators.atr(highs, lows, closes)
            if atr_result:
                indicators['atr'] = atr_result[-1]

        except Exception as e:
            self.logger.error(f"计算技术指标失败: {str(e)}")

        return indicators

    def _get_technical_signals(self, quote: Dict[str, Any],
                               indicators: Dict[str, Any]) -> List[Dict[str, Any]]:
        """获取技术信号"""
        signals = []
        price = quote.get('price', 0)

        try:
            # MACD信号
            if 'macd' in indicators and 'macd_signal' in indicators:
                macd = indicators['macd']
                macd_signal = indicators['macd_signal']
                macd_hist = indicators.get('macd_histogram', 0)

                if macd > macd_signal and macd_hist > 0:
                    signals.append({
                        'type': 'macd',
                        'name': 'MACD金叉',
                        'strength': 2,
                        'bullish': True
                    })
                elif macd < macd_signal and macd_hist < 0:
                    signals.append({
                        'type': 'macd',
                        'name': 'MACD死叉',
                        'strength': 2,
                        'bullish': False
                    })

            # RSI信号
            rsi = indicators.get('rsi', 50)
            if rsi < 30:
                signals.append({
                    'type': 'rsi',
                    'name': f'RSI超卖({rsi:.1f})',
                    'strength': 1.5,
                    'bullish': True
                })
            elif rsi > 70:
                signals.append({
                    'type': 'rsi',
                    'name': f'RSI超买({rsi:.1f})',
                    'strength': 1.5,
                    'bullish': False
                })

            # 均线信号
            ma20 = indicators.get('ma20', 0)
            if ma20 > 0:
                if price > ma20:
                    signals.append({
                        'type': 'ma',
                        'name': '价格站上MA20',
                        'strength': 1,
                        'bullish': True
                    })
                else:
                    signals.append({
                        'type': 'ma',
                        'name': '价格跌破MA20',
                        'strength': 1,
                        'bullish': False
                    })

            # 布林带信号
            boll_upper = indicators.get('boll_upper', 0)
            boll_lower = indicators.get('boll_lower', 0)

            if boll_upper > 0 and price >= boll_upper * 0.99:
                signals.append({
                    'type': 'boll',
                    'name': '触及布林上轨',
                    'strength': 1.5,
                    'bullish': False
                })
            elif boll_lower > 0 and price <= boll_lower * 1.01:
                signals.append({
                    'type': 'boll',
                    'name': '触及布林下轨',
                    'strength': 1.5,
                    'bullish': True
                })

            # KDJ信号
            kdj_k = indicators.get('kdj_k', 50)
            kdj_d = indicators.get('kdj_d', 50)
            kdj_j = indicators.get('kdj_j', 50)

            if kdj_k < 20 and kdj_d < 20:
                signals.append({
                    'type': 'kdj',
                    'name': f'KDJ低位(K:{kdj_k:.1f})',
                    'strength': 1,
                    'bullish': True
                })
            elif kdj_k > 80 and kdj_d > 80:
                signals.append({
                    'type': 'kdj',
                    'name': f'KDJ高位(K:{kdj_k:.1f})',
                    'strength': 1,
                    'bullish': False
                })

        except Exception as e:
            self.logger.error(f"获取技术信号失败: {str(e)}")

        return signals

    def _synthesize_signals(self, signals: List[Dict[str, Any]]) -> Tuple[str, float]:
        """
        综合所有信号

        Returns:
            (趋势方向, 强度)
        """
        if not signals:
            return '中性', 0

        bullish_strength = sum(s['strength'] for s in signals if s.get('bullish', False))
        bearish_strength = sum(s['strength'] for s in signals if not s.get('bullish', True))

        total_strength = bullish_strength - bearish_strength

        if total_strength >= 4:
            return '强烈看涨', total_strength
        elif total_strength >= 2:
            return '看涨', total_strength
        elif total_strength <= -4:
            return '强烈看跌', total_strength
        elif total_strength <= -2:
            return '看跌', total_strength
        else:
            return '中性', total_strength

    def batch_analyze(self, quotes: List[Dict[str, Any]],
                      historical_data: Dict[str, List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        """
        批量分析

        Args:
            quotes: 行情列表
            historical_data: {品种代码: 历史数据}

        Returns:
            分析结果列表
        """
        results = []

        for quote in quotes:
            symbol = quote.get('symbol', '')
            historical = historical_data.get(symbol, []) if historical_data else []

            analysis = self.analyze_quote(quote, historical)
            results.append(analysis)

        return results
