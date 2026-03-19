# -*- coding: utf-8 -*-
"""
真实期货数据源接入模块 - 更新版
支持AKShare、Tushare等权威数据源
"""

import logging
import pandas as pd
import numpy as np
import requests
import json
import re
from typing import Dict, List, Any, Optional
from datetime import datetime
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


class DirectAPIDataSource(BaseDataSource):
    """
    直接API数据源
    直接访问东方财富、新浪等API获取数据
    """

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self.session = None

    def connect(self) -> bool:
        """建立连接"""
        try:
            self.session = requests.Session()
            self.session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })
            self.is_connected = True
            self.logger.info("API数据源连接成功")
            return True
        except Exception as e:
            self.logger.error(f"API数据源连接失败: {str(e)}")
            return False

    def get_realtime_quotes(self, symbols: List[str] = None) -> List[Dict[str, Any]]:
        """获取实时行情"""
        if not self.is_connected:
            if not self.connect():
                return []

        all_quotes = []

        # 1. 东方财富API
        all_quotes.extend(self._fetch_eastmoney())

        # 2. 新浪API
        all_quotes.extend(self._fetch_sina())

        return all_quotes

    def _fetch_eastmoney(self) -> List[Dict[str, Any]]:
        """从东方财富获取数据"""
        quotes = []

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
                'fs': 'm:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23',  # 全部市场
                'fields': 'f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152',
                '_': str(int(datetime.now().timestamp() * 1000))
            }

            response = self.session.get(url, params=params, timeout=15)
            if response.status_code == 200:
                # 解析JSONP响应
                json_str = re.sub(r'^jQuery\d+_\d+\(|\);?$', '', response.text.strip())
                data = json.loads(json_str)

                if 'data' in data and 'diff' in data['data']:
                    for item in data['data']['diff']:
                        try:
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
                                'turnover': float(item[9]) if len(item) > 9 else 0,
                                'source': '东方财富',
                                'timestamp': datetime.now()
                            }

                            # 添加交易所信息
                            exchange_code = item[12] if len(item) > 12 else ''
                            quote['exchange'] = self._get_exchange_name(exchange_code)

                            # 添加买卖价
                            quote['bid1'] = float(item[15]) if len(item) > 15 else 0
                            quote['ask1'] = float(item[16]) if len(item) > 16 else 0

                            # 添加涨跌状态
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

                    self.logger.info(f"东方财富: 获取 {len(quotes)} 条数据")
            else:
                self.logger.warning(f"东方财富请求失败: {response.status_code}")

        except Exception as e:
            self.logger.error(f"东方财富API错误: {str(e)}")

        return quotes

    def _fetch_sina(self) -> List[Dict[str, Any]]:
        """从新浪获取数据"""
        quotes = []

        try:
            # 主力合约列表
            main_symbols = [
                # 上期所
                'CU0', 'AL0', 'ZN0', 'PB0', 'NI0', 'SN0', 'RB0', 'HC0', 'WR0', 'SS0', 'AU0', 'AG0',
                # 大商所
                'I0', 'J0', 'JM0', 'A0', 'M0', 'Y0', 'P0', 'C0', 'CS0', 'L0', 'V0', 'PP0', 'EB0',
                'EG0', 'PG0', 'JD0', 'LH0', 'FB0', 'BB0', 'RR0',
                # 郑商所
                'SR0', 'CF0', 'RM0', 'MA0', 'TA0', 'ZC0', 'FG0', 'OI0', 'RS0', 'RI0', 'JR0', 'LR0',
                'WH0', 'PM0', 'SF0', 'SM0', 'UR0', 'SA0', 'PK0', 'AP0', 'CJ0',
                # 能源中心
                'SC0', 'LU0', 'NR0', 'BC0',
                # 中金所
                'IF0', 'IH0', 'IC0', 'IM0', 'T0', 'TF0', 'TS0', 'TL0'
            ]

            # 批量获取
            batch_size = 50
            for i in range(0, len(main_symbols), batch_size):
                batch = main_symbols[i:i+batch_size]
                symbols_str = ','.join(batch)

                try:
                    url = f"https://hq.sinajs.cn/list={symbols_str}"
                    response = self.session.get(url, timeout=10)

                    if response.status_code == 200:
                        # 解析响应
                        for symbol in batch:
                            pattern = rf'hq_str_{symbol.replace("0", "")}="([^"]+)"'
                            match = re.search(pattern, response.text)

                            if match:
                                content = match.group(1)
                                parts = content.split(',')

                                if len(parts) >= 12:
                                    try:
                                        quote = {
                                            'symbol': symbol,
                                            'name': parts[0].strip(),
                                            'open': float(parts[1]) if parts[1] else 0,
                                            'high': float(parts[2]) if parts[2] else 0,
                                            'low': float(parts[3]) if parts[3] else 0,
                                            'price': float(parts[4]) if parts[4] else 0,
                                            'close': float(parts[4]) if parts[4] else 0,
                                            'volume': float(parts[7]) if parts[7] else 0,
                                            'open_interest': float(parts[9]) if len(parts) > 9 else 0,
                                            'change': float(parts[10]) if len(parts) > 10 else 0,
                                            'change_percent': float(parts[11]) if len(parts) > 11 else 0,
                                            'source': '新浪期货',
                                            'timestamp': datetime.now()
                                        }

                                        # 添加交易所信息
                                        quote['exchange'] = self._get_exchange_by_symbol(symbol)

                                        # 添加涨跌状态
                                        if quote['change'] > 0:
                                            quote['status'] = '上涨'
                                        elif quote['change'] < 0:
                                            quote['status'] = '下跌'
                                        else:
                                            quote['status'] = '平盘'

                                        quotes.append(quote)
                                    except ValueError:
                                        continue

                except Exception as e:
                    self.logger.warning(f"新浪批量请求失败: {str(e)}")
                    continue

            self.logger.info(f"新浪期货: 获取 {len(quotes)} 条数据")

        except Exception as e:
            self.logger.error(f"新浪API错误: {str(e)}")

        return quotes

    def _get_exchange_name(self, code: str) -> str:
        """根据交易所代码获取名称"""
        exchange_map = {
            '0': '上期所',
            '1': '大商所',
            '2': '郑商所',
            '3': '大商所',
            '4': '上期所',
            '5': '中金所'
        }
        return exchange_map.get(code, '未知')

    def _get_exchange_by_symbol(self, symbol: str) -> str:
        """根据品种代码获取交易所"""
        first_char = symbol[0] if symbol else ''

        if first_char in ['CU', 'AL', 'ZN', 'PB', 'NI', 'SN', 'RB', 'HC', 'WR', 'SS']:
            return '上期所'
        elif first_char in ['I', 'J', 'JM', 'A', 'M', 'Y', 'P', 'C', 'CS', 'L', 'V', 'PP', 'EB', 'EG', 'PG', 'JD', 'LH', 'FB', 'BB', 'RR']:
            return '大商所'
        elif first_char in ['SR', 'CF', 'RM', 'MA', 'TA', 'ZC', 'FG', 'OI', 'RS', 'RI', 'JR', 'LR', 'WH', 'PM', 'SF', 'SM', 'UR', 'SA', 'PK', 'AP', 'CJ']:
            return '郑商所'
        elif first_char in ['IF', 'IH', 'IC', 'IM', 'T', 'TF', 'TS', 'TL']:
            return '中金所'
        elif first_char in ['SC', 'LU', 'NR', 'BC']:
            return '能源中心'
        else:
            return '未知'


class AKShareDataSource(BaseDataSource):
    """
    AKShare数据源
    使用futures_zh_realtime等函数
    """

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self.ak = None

    def connect(self) -> bool:
        """连接AKShare"""
        try:
            import akshare as ak
            self.ak = ak
            self.is_connected = True
            self.logger.info("AKShare连接成功")
            return True
        except ImportError:
            self.logger.error("AKShare未安装")
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
            # 使用futures_zh_realtime获取实时数据
            df = self.ak.futures_zh_realtime()

            for _, row in df.iterrows():
                quote = {
                    'symbol': str(row.get('symbol', '')),
                    'name': str(row.get('name', '')),
                    'price': float(row.get('close', row.get('trade', 0))),
                    'open': float(row.get('open', 0)),
                    'high': float(row.get('high', 0)),
                    'low': float(row.get('low', 0)),
                    'close': float(row.get('close', 0)),
                    'volume': float(row.get('volume', 0)),
                    'open_interest': float(row.get('position', 0)),
                    'change': 0,
                    'change_percent': float(row.get('changepercent', 0)),
                    'exchange': str(row.get('exchange', '')),
                    'source': 'AKShare',
                    'timestamp': datetime.now()
                }

                # 计算涨跌额
                quote['change'] = quote['price'] * quote['change_percent'] / 100

                # 涨跌状态
                if quote['change'] > 0:
                    quote['status'] = '上涨'
                elif quote['change'] < 0:
                    quote['status'] = '下跌'
                else:
                    quote['status'] = '平盘'

                quotes.append(quote)

            self.logger.info(f"AKShare: 获取 {len(quotes)} 条数据")

        except Exception as e:
            self.logger.error(f"AKShare获取失败: {str(e)}")

        return quotes


def create_real_data_manager(config: Dict[str, Any] = None) -> DirectAPIDataSource:
    """创建真实数据管理器"""
    # 优先使用直接API（更稳定）
    manager = DirectAPIDataSource(config)
    manager.connect()
    return manager
