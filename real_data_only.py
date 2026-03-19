# -*- coding: utf-8 -*-
"""
真实期货数据源 - 仅使用真实数据
支持AKShare、Lightpanda等多种数据源，自动切换
"""

import logging
import requests
import json
import re
import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed


class RealFuturesDataManager:
    """
    真实期货数据管理器
    支持多数据源自动切换，确保始终获取真实数据
    """

    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)

        # 数据源优先级
        self.sources = [
            'akshare',
            'eastmoney_api',
            'sina_api',
            'hexun_api',
            'lightpanda'
        ]

        # 会话
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        })

        # AKShare实例
        self.akshare = None

        # Lightpanda实例
        self.lightpanda_available = False

    def _init_akshare(self) -> bool:
        """初始化AKShare"""
        try:
            import akshare as ak
            self.akshare = ak
            self.logger.info("AKShare初始化成功")
            return True
        except ImportError:
            self.logger.warning("AKShare未安装")
            return False
        except Exception as e:
            self.logger.warning(f"AKShare初始化失败: {str(e)}")
            return False

    def _init_lightpanda(self) -> bool:
        """初始化Lightpanda"""
        try:
            # 检查是否安装了playwright（用于无头浏览器）
            import subprocess
            result = subprocess.run(['python3', '-c', 'import playwright'],
                                  capture_output=True, timeout=5)
            if result.returncode == 0:
                self.lightpanda_available = True
                self.logger.info("Lightpanda(Playwright)可用")
                return True
        except:
            pass
        return False

    def get_all_realtime_quotes(self) -> List[Dict[str, Any]]:
        """
        获取所有真实期货行情
        尝试所有数据源直到获取到数据
        """
        all_quotes = []
        successful_sources = []

        # 1. 尝试AKShare
        quotes = self._get_from_akshare()
        if quotes:
            all_quotes.extend(quotes)
            successful_sources.append('AKShare')

        # 2. 尝试东方财富API
        quotes = self._get_from_eastmoney()
        if quotes:
            all_quotes.extend(quotes)
            successful_sources.append('东方财富API')

        # 3. 尝试新浪API
        quotes = self._get_from_sina()
        if quotes:
            all_quotes.extend(quotes)
            successful_sources.append('新浪API')

        # 4. 如果以上都不够，使用Lightpanda爬取
        if len(all_quotes) < 20:
            self.logger.info("API数据不足，使用Lightpanda爬取...")
            quotes = self._get_with_lightpanda()
            if quotes:
                all_quotes.extend(quotes)
                successful_sources.append('Lightpanda')

        # 去重
        unique_quotes = self._deduplicate_quotes(all_quotes)

        self.logger.info(f"数据源: {', '.join(successful_sources)} | 获取品种: {len(unique_quotes)}")

        return unique_quotes

    def _get_from_akshare(self) -> List[Dict[str, Any]]:
        """从AKShare获取数据"""
        if not self.akshare:
            if not self._init_akshare():
                return []

        quotes = []

        try:
            # 获取实时行情
            df = self.akshare.futures_zh_realtime()

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
                    'source': 'AKShare',
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

            self.logger.info(f"AKShare: {len(quotes)}条")

        except Exception as e:
            self.logger.warning(f"AKShare失败: {str(e)}")

        return quotes

    def _get_from_eastmoney(self) -> List[Dict[str, Any]]:
        """从东方财富API获取数据"""
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
                                'source': '东方财富',
                                'timestamp': datetime.now()
                            }

                            # 交易所
                            exchange_code = item[12] if len(item) > 12 else ''
                            quote['exchange'] = self._get_exchange_name_eastmoney(exchange_code)

                            # 买卖价
                            quote['bid1'] = float(item[15]) if len(item) > 15 else 0
                            quote['ask1'] = float(item[16]) if len(item) > 16 else 0

                            # 状态
                            if quote['change'] > 0:
                                quote['status'] = '上涨'
                            elif quote['change'] < 0:
                                quote['status'] = '下跌'
                            else:
                                quote['status'] = '平盘'

                            quotes.append(quote)
                        except:
                            continue

                    self.logger.info(f"东方财富: {len(quotes)}条")

        except Exception as e:
            self.logger.warning(f"东方财富API失败: {str(e)}")

        return quotes

    def _get_from_sina(self) -> List[Dict[str, Any]]:
        """从新浪API获取数据"""
        quotes = []

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

        try:
            # 批量获取
            batch_size = 30
            for i in range(0, len(main_symbols), batch_size):
                batch = main_symbols[i:i+batch_size]
                symbols_str = ','.join([s.replace('0', '') for s in batch])

                try:
                    url = f"https://hq.sinajs.cn/list={symbols_str}"
                    response = self.session.get(url, timeout=10)

                    if response.status_code == 200:
                        for symbol in batch:
                            pattern = rf'hq_str_{symbol.replace("0", "")}="([^"]+)"'
                            match = re.search(pattern, response.text)

                            if match:
                                content = match.group(1)
                                parts = content.split(',')

                                if len(parts) >= 12 and parts[0]:
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

                                        # 交易所
                                        quote['exchange'] = self._get_exchange_by_symbol(symbol)

                                        # 状态
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

            self.logger.info(f"新浪: {len(quotes)}条")

        except Exception as e:
            self.logger.warning(f"新浪API失败: {str(e)}")

        return quotes

    def _get_with_lightpanda(self) -> List[Dict[str, Any]]:
        """使用Lightpanda(Playwright)爬取数据"""
        quotes = []

        try:
            # 检查playwright是否可用
            if not self.lightpanda_available:
                if not self._init_lightpanda():
                    return []

            from playwright.sync_api import sync_playwright

            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()

                # 爬取东方财富
                try:
                    self.logger.info("使用Lightpanda爬取东方财富...")
                    page.goto('https://futures.eastmoney.com/', timeout=30000)
                    page.wait_for_timeout(3000)

                    # 尝试获取数据
                    # 这里需要根据实际页面结构调整选择器
                    data = page.evaluate('''() => {
                        // 尝试从页面获取数据
                        if (window.quotesData) {
                            return window.quotesData;
                        }
                        return null;
                    }''')

                    if data:
                        quotes.extend(self._parse_lightpanda_data(data, '东方财富'))

                except Exception as e:
                    self.logger.warning(f"Lightpanda爬取东方财富失败: {str(e)}")

                # 爬取新浪期货
                try:
                    self.logger.info("使用Lightpanda爬取新浪期货...")
                    page.goto('http://finance.sina.com.cn/futuremarket/', timeout=30000)
                    page.wait_for_timeout(3000)

                    # 获取数据
                    data = page.evaluate('''() => {
                        // 从新浪页面获取数据
                        const rows = document.querySelectorAll('table tr');
                        const results = [];
                        rows.forEach(row => {
                            const cols = row.querySelectorAll('td');
                            if (cols.length >= 6) {
                                results.push({
                                    symbol: cols[0]?.textContent,
                                    name: cols[1]?.textContent,
                                    price: cols[2]?.textContent,
                                    change: cols[3]?.textContent
                                });
                            }
                        });
                        return results;
                    }''')

                    if data:
                        quotes.extend(self._parse_lightpanda_sina_data(data))

                except Exception as e:
                    self.logger.warning(f"Lightpanda爬取新浪失败: {str(e)}")

                browser.close()

            self.logger.info(f"Lightpanda: {len(quotes)}条")

        except Exception as e:
            self.logger.warning(f"Lightpanda失败: {str(e)}")

        return quotes

    def _parse_lightpanda_data(self, data: Any, source: str) -> List[Dict[str, Any]]:
        """解析Lightpanda获取的数据"""
        quotes = []
        # 实现数据解析逻辑
        return quotes

    def _parse_lightpanda_sina_data(self, data: List[Dict]) -> List[Dict[str, Any]]:
        """解析Lightpanda获取的新浪数据"""
        quotes = []

        for item in data:
            try:
                if item.get('symbol') and item.get('price'):
                    quote = {
                        'symbol': item['symbol'],
                        'name': item.get('name', ''),
                        'price': self._clean_number(item['price']),
                        'change': self._clean_number(item.get('change', 0)),
                        'source': 'Lightpanda-新浪',
                        'timestamp': datetime.now()
                    }

                    # 计算涨跌幅
                    if quote['price'] > 0:
                        quote['change_percent'] = (quote['change'] / quote['price']) * 100

                    quotes.append(quote)
            except:
                continue

        return quotes

    def _deduplicate_quotes(self, quotes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """去重，优先保留数据更完整的记录"""
        unique = {}

        for quote in quotes:
            symbol = quote.get('symbol', '')
            if not symbol:
                continue

            # 如果已存在，比较数据完整性
            if symbol in unique:
                existing = unique[symbol]
                # 优先选择有更多字段的记录
                if len(quote) > len(existing):
                    unique[symbol] = quote
            else:
                unique[symbol] = quote

        return list(unique.values())

    def _get_exchange_name_eastmoney(self, code: str) -> str:
        """东方财富交易所代码映射"""
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
        if not symbol:
            return '未知'

        first_char = symbol[0]

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

    def _clean_number(self, value: Any) -> float:
        """清理数字"""
        if isinstance(value, (int, float)):
            return float(value)

        if isinstance(value, str):
            # 移除逗号、空格等
            cleaned = value.replace(',', '').replace(' ', '').strip()
            try:
                return float(cleaned)
            except:
                return 0.0

        return 0.0

    def get_historical_data(self, symbol: str, days: int = 100) -> List[Dict[str, Any]]:
        """
        获取历史数据
        尝试多个数据源
        """
        historical = []

        # 1. 尝试AKShare历史数据
        if self.akshare:
            try:
                df = self.akshare.futures_hist_em(
                    symbol=symbol,
                    period="daily",
                    start_date=(datetime.now().replace(day=1) - timedelta(days=days*2)).strftime('%Y%m%d'),
                    end_date=datetime.now().strftime('%Y%m%d')
                )

                if not df.empty:
                    for _, row in df.iterrows():
                        historical.append({
                            'symbol': symbol,
                            'date': str(row.get('date', '')),
                            'open': float(row.get('open', 0)),
                            'high': float(row.get('high', 0)),
                            'low': float(row.get('low', 0)),
                            'close': float(row.get('close', 0)),
                            'volume': float(row.get('volume', 0)),
                            'open_interest': float(row.get('open_interest', 0))
                        })

                    self.logger.info(f"获取{symbol}历史数据: {len(historical)}条")
                    return historical

            except Exception as e:
                self.logger.warning(f"AKShare历史数据失败: {str(e)}")

        # 2. 如果AKShare失败，使用模拟生成（基于当前价格的随机波动）
        self.logger.warning(f"历史数据获取失败，使用技术分析模拟")
        return []

    def close(self):
        """关闭资源"""
        if self.session:
            self.session.close()


def get_real_futures_quotes() -> List[Dict[str, Any]]:
    """
    获取真实期货行情（便捷函数）
    """
    manager = RealFuturesDataManager()
    return manager.get_all_realtime_quotes()


def get_real_historical_data(symbol: str, days: int = 100) -> List[Dict[str, Any]]:
    """
    获取真实历史数据（便捷函数）
    """
    manager = RealFuturesDataManager()
    return manager.get_historical_data(symbol, days)
