# -*- coding: utf-8 -*-
"""
真实期货数据源接入模块
支持AKShare、Tushare等权威数据源
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from abc import ABC, abstractmethod


class BaseDataSource(ABC):
    """数据源基类"""

    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        self.is_connected = False

    @abstractmethod
    def connect(self) -> bool:
        """连接数据源"""
        pass

    @abstractmethod
    def get_realtime_quotes(self, symbols: List[str] = None) -> List[Dict[str, Any]]:
        """获取实时行情"""
        pass

    @abstractmethod
    def get_historical_data(self, symbol: str, period: str = 'daily',
                           start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
        """获取历史数据"""
        pass

    def disconnect(self):
        """断开连接"""
        self.is_connected = False


class AKShareDataSource(BaseDataSource):
    """
    AKShare数据源
    中国最大的开源财经数据接口库
    GitHub: https://github.com/akfamily/akshare
    """

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self.ak = None
        self._symbol_map = self._build_symbol_map()

    def _build_symbol_map(self) -> Dict[str, str]:
        """构建期货代码映射"""
        return {
            # 上期所
            'CU0': 'cu2505', 'CU': 'cu',
            'AL0': 'al2505', 'AL': 'al',
            'ZN0': 'zn2505', 'ZN': 'zn',
            'PB0': 'pb2505', 'PB': 'pb',
            'NI0': 'ni2505', 'NI': 'ni',
            'SN0': 'sn2505', 'SN': 'sn',
            'AU0': 'au2506', 'AU': 'au',
            'AG0': 'ag2506', 'AG': 'ag',
            'RB0': 'rb2505', 'RB': 'rb',
            'HC0': 'hc2505', 'HC': 'hc',
            'WR0': 'wr2505', 'WR': 'wr',
            'SS0': 'ss2505', 'SS': 'ss',
            # 大商所
            'I0': 'i2505', 'I': 'i',
            'J0': 'j2505', 'J': 'j',
            'JM0': 'jm2505', 'JM': 'jm',
            'A0': 'a2505', 'A': 'a',
            'M0': 'm2505', 'M': 'm',
            'Y0': 'y2505', 'Y': 'y',
            'P0': 'p2505', 'P': 'p',
            'C0': 'c2505', 'C': 'c',
            'CS0': 'cs2505', 'CS': 'cs',
            'L0': 'l2505', 'L': 'l',
            'V0': 'v2505', 'V': 'v',
            'PP0': 'pp2505', 'PP': 'pp',
            'EG0': 'eg2505', 'EG': 'eg',
            'EB0': 'eb2505', 'EB': 'eb',
            'PG0': 'pg2505', 'PG': 'pg',
            'JD0': 'jd2505', 'JD': 'jd',
            'LH0': 'lh2505', 'LH': 'lh',
            'FB0': 'fb2505', 'FB': 'fb',
            'BB0': 'bb2505', 'BB': 'bb',
            'RR0': 'rr2505', 'RR': 'rr',
            'Y2505': 'y2505',
            # 郑商所
            'SR0': 'sr2505', 'SR': 'sr',
            'CF0': 'cf2505', 'CF': 'cf',
            'RM0': 'rm2505', 'RM': 'rm',
            'MA0': 'MA2505', 'MA': 'MA',
            'TA0': 'TA2505', 'TA': 'TA',
            'ZC0': 'ZC2505', 'ZC': 'ZC',
            'FG0': 'FG2505', 'FG': 'FG',
            'OI0': 'OI2505', 'OI': 'OI',
            'RS0': 'rs2505', 'RS': 'rs',
            'RI0': 'ri2505', 'RI': 'ri',
            'JR0': 'jr2505', 'JR': 'jr',
            'LR0': 'lr2505', 'LR': 'lr',
            'WH0': 'wh2505', 'WH': 'wh',
            'PM0': 'pm2505', 'PM': 'pm',
            'SF0': 'sf2505', 'SF': 'sf',
            'SM0': 'SM2505', 'SM': 'SM',
            'UR0': 'ur2505', 'UR': 'ur',
            'SA0': 'sa2505', 'SA': 'sa',
            'PK0': 'pk2505', 'PK': 'pk',
            'AP0': 'AP2505', 'AP': 'AP',
            'CJ0': 'cj2505', 'CJ': 'cj',
            'CY0': 'CY2505', 'CY': 'CY',
            'PX0': 'px2505', 'PX': 'px',
            # 能源中心
            'SC0': 'sc2504', 'SC': 'sc',
            'LU0': 'lu2504', 'LU': 'lu',
            'NR0': 'nr2505', 'NR': 'nr',
            'BC0': 'bc2504', 'BC': 'bc',
            # 中金所
            'IF0': 'IF2503', 'IF': 'IF',
            'IH0': 'IH2503', 'IH': 'IH',
            'IC0': 'IC2503', 'IC': 'IC',
            'IM0': 'IM2503', 'IM': 'IM',
            'T0': 'T2506', 'T': 'T',
            'TF0': 'TF2506', 'TF': 'TF',
            'TS0': 'TS2506', 'TS': 'TS',
            'TL0': 'TL2506', 'TL': 'TL',
        }

    def connect(self) -> bool:
        """连接AKShare"""
        try:
            import akshare as ak
            self.ak = ak
            self.is_connected = True
            self.logger.info("AKShare连接成功")
            return True
        except ImportError:
            self.logger.error("AKShare未安装，请运行: pip install akshare")
            return False
        except Exception as e:
            self.logger.error(f"AKShare连接失败: {str(e)}")
            return False

    def get_realtime_quotes(self, symbols: List[str] = None) -> List[Dict[str, Any]]:
        """获取实时行情"""
        if not self.is_connected:
            if not self.connect():
                return []

        quotes = []

        try:
            # 获取所有期货实时行情
            self.logger.info("正在获取期货实时行情...")

            # 获取上期所行情
            try:
                df_shfe = self.ak.futures_sina_sfh_hist(symbol="SHFE")
                if not df_shfe.empty:
                    quotes.extend(self._parse_sina_df(df_shfe, '上期所'))
            except Exception as e:
                self.logger.warning(f"获取上期所行情失败: {str(e)}")

            # 获取大商所行情
            try:
                df_dce = self.ak.futures_sina_sfh_hist(symbol="DCE")
                if not df_dce.empty:
                    quotes.extend(self._parse_sina_df(df_dce, '大商所'))
            except Exception as e:
                self.logger.warning(f"获取大商所行情失败: {str(e)}")

            # 获取郑商所行情
            try:
                df_czce = self.ak.futures_sina_sfh_hist(symbol="CZCE")
                if not df_czce.empty:
                    quotes.extend(self._parse_sina_df(df_czce, '郑商所'))
            except Exception as e:
                self.logger.warning(f"获取郑商所行情失败: {str(e)}")

            # 获取中金所行情
            try:
                df_cffex = self.ak.futures_sina_sfh_hist(symbol="CFFEX")
                if not df_cffex.empty:
                    quotes.extend(self._parse_sina_df(df_cffex, '中金所'))
            except Exception as e:
                self.logger.warning(f"获取中金所行情失败: {str(e)}")

            # 获取能源中心行情
            try:
                df_ine = self.ak.futures_sina_sfh_hist(symbol="INE")
                if not df_ine.empty:
                    quotes.extend(self._parse_sina_df(df_ine, '能源中心'))
            except Exception as e:
                self.logger.warning(f"获取能源中心行情失败: {str(e)}")

            self.logger.info(f"共获取 {len(quotes)} 条实时行情")

        except Exception as e:
            self.logger.error(f"获取实时行情失败: {str(e)}")

        return quotes

    def _parse_sina_df(self, df: pd.DataFrame, exchange: str) -> List[Dict[str, Any]]:
        """解析新浪期货数据框"""
        quotes = []

        try:
            for _, row in df.iterrows():
                try:
                    quote = {
                        'symbol': str(row.get('symbol', '')),
                        'name': str(row.get('name', '')),
                        'price': float(row.get('close', row.get('price', 0))),
                        'open': float(row.get('open', 0)),
                        'high': float(row.get('high', 0)),
                        'low': float(row.get('low', 0)),
                        'close': float(row.get('close', 0)),
                        'volume': float(row.get('volume', 0)),
                        'open_interest': float(row.get('hold', 0)),
                        'turnover': float(row.get('amount', 0)),
                        'exchange': exchange,
                        'source': 'AKShare',
                        'timestamp': datetime.now()
                    }

                    # 计算涨跌
                    if quote['close'] > 0 and quote['open'] > 0:
                        quote['change'] = quote['close'] - quote['open']
                        quote['change_percent'] = (quote['change'] / quote['open']) * 100
                    else:
                        quote['change'] = 0
                        quote['change_percent'] = 0

                    # 涨跌状态
                    if quote['change'] > 0:
                        quote['status'] = '上涨'
                    elif quote['change'] < 0:
                        quote['status'] = '下跌'
                    else:
                        quote['status'] = '平盘'

                    quotes.append(quote)

                except Exception as e:
                    self.logger.warning(f"解析单条数据失败: {str(e)}")
                    continue

        except Exception as e:
            self.logger.error(f"解析数据框失败: {str(e)}")

        return quotes

    def get_historical_data(self, symbol: str, period: str = 'daily',
                           start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
        """获取历史数据"""
        if not self.is_connected:
            if not self.connect():
                return []

        try:
            # 映射品种代码
            ak_symbol = self._symbol_map.get(symbol, symbol)

            # 获取历史数据
            df = self.ak.futures_sina_sfh_hist(symbol=ak_symbol)

            if df.empty:
                self.logger.warning(f"未获取到 {symbol} 的历史数据")
                return []

            # 过滤日期范围
            if start_date:
                df = df[df['date'] >= start_date]
            if end_date:
                df = df[df['date'] <= end_date]

            # 转换为列表
            historical = []
            for _, row in df.iterrows():
                historical.append({
                    'symbol': symbol,
                    'date': str(row.get('date', '')),
                    'open': float(row.get('open', 0)),
                    'high': float(row.get('high', 0)),
                    'low': float(row.get('low', 0)),
                    'close': float(row.get('close', 0)),
                    'volume': float(row.get('volume', 0)),
                    'open_interest': float(row.get('hold', 0)),
                    'turnover': float(row.get('amount', 0))
                })

            self.logger.info(f"获取 {symbol} 历史数据 {len(historical)} 条")
            return historical

        except Exception as e:
            self.logger.error(f"获取历史数据失败: {str(e)}")
            return []

    def get_futures_list(self) -> List[Dict[str, Any]]:
        """获取所有期货品种列表"""
        if not self.is_connected:
            if not self.connect():
                return []

        futures_list = []

        try:
            # 获取期货列表
            df = self.ak.futures_sina_list()

            for _, row in df.iterrows():
                futures_list.append({
                    'symbol': str(row.get('symbol', '')),
                    'name': str(row.get('name', '')),
                    'exchange': str(row.get('exchange', ''))
                })

        except Exception as e:
            self.logger.error(f"获取期货列表失败: {str(e)}")

        return futures_list


class TushareDataSource(BaseDataSource):
    """
    Tushare数据源
    需要API Token，有免费额度
    官网: https://tushare.pro
    """

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self.ts = None
        self.token = self.config.get('token', '')

    def connect(self) -> bool:
        """连接Tushare"""
        try:
            import tushare as ts
            self.ts = ts

            if self.token:
                ts.set_token(self.token)
                self.pro = ts.pro_api()
                self.is_connected = True
                self.logger.info("Tushare连接成功")
                return True
            else:
                self.logger.warning("Tushare Token未配置")
                return False

        except ImportError:
            self.logger.error("Tushare未安装，请运行: pip install tushare")
            return False
        except Exception as e:
            self.logger.error(f"Tushare连接失败: {str(e)}")
            return False

    def get_realtime_quotes(self, symbols: List[str] = None) -> List[Dict[str, Any]]:
        """获取实时行情"""
        if not self.is_connected:
            if not self.connect():
                return []

        quotes = []

        try:
            # 获取期货实时行情
            df = self.pro.fut_daily(trade_date=datetime.now().strftime('%Y%m%d'))

            for _, row in df.iterrows():
                quote = {
                    'symbol': str(row.get('ts_code', '')),
                    'name': str(row.get('name', '')),
                    'price': float(row.get('close', 0)),
                    'open': float(row.get('open', 0)),
                    'high': float(row.get('high', 0)),
                    'low': float(row.get('low', 0)),
                    'close': float(row.get('close', 0)),
                    'volume': float(row.get('vol', 0)),
                    'open_interest': float(row.get('oi', 0)),
                    'change': float(row.get('change', 0)),
                    'change_percent': float(row.get('pct_chg', 0)),
                    'exchange': str(row.get('exchange', '')),
                    'source': 'Tushare',
                    'timestamp': datetime.now()
                }
                quotes.append(quote)

        except Exception as e:
            self.logger.error(f"获取实时行情失败: {str(e)}")

        return quotes

    def get_historical_data(self, symbol: str, period: str = 'daily',
                           start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
        """获取历史数据"""
        if not self.is_connected:
            if not self.connect():
                return []

        try:
            df = self.pro.fut_daily(ts_code=symbol, start_date=start_date, end_date=end_date)

            historical = []
            for _, row in df.iterrows():
                historical.append({
                    'symbol': symbol,
                    'date': str(row.get('trade_date', '')),
                    'open': float(row.get('open', 0)),
                    'high': float(row.get('high', 0)),
                    'low': float(row.get('low', 0)),
                    'close': float(row.get('close', 0)),
                    'volume': float(row.get('vol', 0)),
                    'open_interest': float(row.get('oi', 0))
                })

            return historical

        except Exception as e:
            self.logger.error(f"获取历史数据失败: {str(e)}")
            return []


class ExchangeDataSource(BaseDataSource):
    """
    交易所官方数据源
    直接从交易所获取数据
    """

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self.exchange_urls = {
            'SHFE': 'https://www.shfe.com.cn',
            'DCE': 'http://www.dce.com.cn',
            'CZCE': 'http://www.czce.com.cn',
            'CFFEX': 'http://www.cffex.com.cn',
            'INE': 'https://www.ine.com.cn'
        }

    def connect(self) -> bool:
        """连接交易所"""
        import requests
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.is_connected = True
        return True

    def get_realtime_quotes(self, symbols: List[str] = None) -> List[Dict[str, Any]]:
        """获取实时行情（从交易所日行情）"""
        quotes = []

        # 上期所日行情
        try:
            url = f"{self.exchange_urls['SHFE']}/data/dailydata/kxqm"
            response = self.session.get(url, timeout=10)
            # 解析响应数据...
        except Exception as e:
            self.logger.warning(f"获取上期所数据失败: {str(e)}")

        # 其他交易所类似处理...

        return quotes

    def get_historical_data(self, symbol: str, period: str = 'daily',
                           start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
        """获取历史数据"""
        return []


class MultiSourceDataManager:
    """多数据源管理器"""

    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.sources = {}
        self.priority_order = ['AKShare', 'Tushare', 'Exchange']
        self.logger = logging.getLogger(self.__class__.__name__)

    def add_source(self, name: str, source: BaseDataSource):
        """添加数据源"""
        self.sources[name] = source
        self.logger.info(f"添加数据源: {name}")

    def get_realtime_quotes(self, symbols: List[str] = None) -> List[Dict[str, Any]]:
        """从所有数据源获取实时行情"""
        all_quotes = []

        for source_name in self.priority_order:
            if source_name not in self.sources:
                continue

            source = self.sources[source_name]
            try:
                quotes = source.get_realtime_quotes(symbols)
                if quotes:
                    all_quotes.extend(quotes)
                    self.logger.info(f"{source_name} 获取 {len(quotes)} 条行情")
            except Exception as e:
                self.logger.warning(f"{source_name} 获取行情失败: {str(e)}")

        # 去重
        unique_quotes = {}
        for quote in all_quotes:
            symbol = quote.get('symbol', '')
            if symbol and symbol not in unique_quotes:
                unique_quotes[symbol] = quote

        return list(unique_quotes.values())

    def get_historical_data(self, symbol: str, period: str = 'daily',
                           start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
        """获取历史数据"""
        for source_name in self.priority_order:
            if source_name not in self.sources:
                continue

            source = self.sources[source_name]
            try:
                data = source.get_historical_data(symbol, period, start_date, end_date)
                if data:
                    return data
            except Exception as e:
                self.logger.warning(f"{source_name} 获取历史数据失败: {str(e)}")

        return []


def create_real_data_manager(config: Dict[str, Any] = None) -> MultiSourceDataManager:
    """创建真实数据管理器"""
    manager = MultiSourceDataManager(config)

    # 添加AKShare数据源（主要）
    akshare_source = AKShareDataSource(config)
    if akshare_source.connect():
        manager.add_source('AKShare', akshare_source)

    # 添加Tushare数据源（备选，需要token）
    if config and config.get('tushare_token'):
        tushare_source = TushareDataSource({'token': config.get('tushare_token')})
        if tushare_source.connect():
            manager.add_source('Tushare', tushare_source)

    # 添加交易所数据源
    exchange_source = ExchangeDataSource(config)
    if exchange_source.connect():
        manager.add_source('Exchange', exchange_source)

    return manager
