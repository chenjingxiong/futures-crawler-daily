# -*- coding: utf-8 -*-
"""
同花顺期货数据爬虫
爬取同花顺网站的期货行情数据
"""

import json
import re
from typing import Dict, List, Optional, Any
from datetime import datetime

from ..base_crawler import BaseCrawler, DataNormalizer


class TonghuashunCrawler(BaseCrawler):
    """同花顺期货爬虫"""

    # 同花顺期货分类
    CATEGORIES = {
        'metal': '金属',
        'energy': '能源',
        'chemical': '化工',
        'agriculture': '农产品',
        'financial': '金融'
    }

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.api_base = "https://d.10jqka.com.cn"

    def _get_api_url(self, category: str = 'all') -> str:
        """获取API URL"""
        urls = {
            'all': f"{self.api_base}/future/v2/real/quote_all",
            'metal': f"{self.api_base}/future/center/metal",
            'energy': f"{self.api_base}/future/center/energy",
            'chemical': f"{self.api_base}/future/center/chemical",
            'agriculture': f"{self.api_base}/future/center/agriculture",
            'financial': f"{self.api_base}/future/center/financial"
        }
        return urls.get(category, urls['all'])

    def _parse_quote_data(self, item: Dict) -> Optional[Dict[str, Any]]:
        """
        解析单条行情数据

        Args:
            item: API返回的数据项

        Returns:
            解析后的行情字典
        """
        try:
            quote = {
                'symbol': str(item.get('code', '')),
                'name': str(item.get('name', '')),
                'price': float(item.get('price', 0)),
                'open': float(item.get('open', 0)),
                'high': float(item.get('high', 0)),
                'low': float(item.get('low', 0)),
                'close': float(item.get('preClose', 0)),
                'volume': float(item.get('volume', 0)),
                'turnover': float(item.get('turnover', 0)),
                'open_interest': float(item.get('openInterest', 0)),
                'change': float(item.get('change', 0)),
                'change_percent': float(item.get('changePercent', 0)),
                'bid1': float(item.get('buy1', 0)),
                'ask1': float(item.get('sell1', 0)),
                'timestamp': datetime.now(),
            }

            # 添加涨跌状态
            if quote['change'] > 0:
                quote['status'] = '上涨'
            elif quote['change'] < 0:
                quote['status'] = '下跌'
            else:
                quote['status'] = '平盘'

            # 添加交易所信息
            quote['exchange'] = self._get_exchange_name(quote['symbol'])

            return quote

        except (ValueError, TypeError) as e:
            self.logger.warning(f"解析行情数据失败: {str(e)}")
            return None

    def _get_exchange_name(self, symbol: str) -> str:
        """根据品种代码获取交易所名称"""
        first_char = symbol[0] if symbol else ''

        if first_char in ['CU', 'AL', 'ZN', 'PB', 'NI', 'SN', 'RB', 'HC', 'WR']:
            return '上期所'
        elif first_char in ['A', 'B', 'M', 'Y', 'P', 'C', 'CS', 'L', 'V', 'PP', 'J', 'JM', 'I', 'JD', 'BB', 'FB', 'LH', 'PG', 'EB']:
            return '大商所'
        elif first_char in ['SR', 'CF', 'RM', 'MA', 'TA', 'OI', 'FG', 'RS', 'RI', 'WH', 'PM', 'JR', 'LR', 'ZC', 'SF', 'SM', 'CY', 'UR', 'SA', 'PK']:
            return '郑商所'
        elif first_char in ['IF', 'IH', 'IC', 'IM']:
            return '中金所'
        elif first_char in ['T', 'TF', 'TS', 'TL']:
            return '中金所'
        elif first_char in ['SC', 'LU', 'NR']:
            return '能源中心'
        elif first_char in ['SI', 'LC', 'PK']:
            return '广期所'
        else:
            return '未知'

    def crawl_realtime_quote(self, symbols: List[str] = None) -> List[Dict[str, Any]]:
        """
        爬取实时行情

        Args:
            symbols: 期货品种列表（可选）

        Returns:
            行情数据列表
        """
        self.logger.info("开始爬取同花顺实时行情...")

        all_quotes = []

        # 同花顺API可能需要特殊处理，这里提供两种方式

        # 方式1: 尝试API接口
        api_quotes = self._crawl_from_api(symbols)
        all_quotes.extend(api_quotes)

        # 方式2: 如果API失败，尝试网页抓取
        if not all_quotes:
            web_quotes = self._crawl_from_web(symbols)
            all_quotes.extend(web_quotes)

        # 标准化数据
        normalized = [DataNormalizer.normalize_quote(q, '同花顺') for q in all_quotes
                      if DataNormalizer.validate_data(q)]

        self.logger.info(f"同花顺爬取完成: 共 {len(normalized)} 条数据")
        return normalized

    def _crawl_from_api(self, symbols: List[str] = None) -> List[Dict[str, Any]]:
        """从API爬取数据"""
        quotes = []

        try:
            url = f"{self.api_base}/future/v2/real/quote_all"

            headers = {
                'Referer': 'https://q.10jqka.com.cn/futures/',
                'X-Requested-With': 'XMLHttpRequest'
            }

            response = self._request(url, headers=headers)

            if response:
                time.sleep(self._get_delay())

                data = response.json()

                # 解析返回数据
                if 'data' in data and 'quote' in data['data']:
                    for item in data['data']['quote']:
                        quote = self._parse_quote_data(item)
                        if quote:
                            # 过滤指定品种
                            if symbols is None or quote['symbol'] in symbols:
                                quotes.append(quote)

        except Exception as e:
            self.logger.warning(f"API爬取失败: {str(e)}")

        return quotes

    def _crawl_from_web(self, symbols: List[str] = None) -> List[Dict[str, Any]]:
        """从网页爬取数据"""
        quotes = []

        try:
            url = "https://q.10jqka.com.cn/futures/"

            response = self._request(url)

            if response:
                time.sleep(self._get_delay())

                soup = self._parse_html(response.text)

                # 查找行情表格
                # 同花顺的页面结构需要根据实际情况调整

                tables = soup.find_all('table')
                for table in tables:
                    rows = table.find_all('tr')[1:]  # 跳过表头

                    for row in rows:
                        cols = row.find_all('td')
                        if len(cols) >= 6:
                            try:
                                quote = {
                                    'symbol': cols[0].text.strip(),
                                    'name': cols[1].text.strip(),
                                    'price': self._clean_number(cols[2].text),
                                    'change_percent': self._clean_number(cols[3].text),
                                    'volume': self._clean_number(cols[4].text),
                                    'open_interest': self._clean_number(cols[5].text),
                                    'timestamp': datetime.now()
                                }

                                # 计算涨跌额
                                quote['change'] = quote['price'] * quote['change_percent'] / 100

                                if symbols is None or quote['symbol'] in symbols:
                                    quotes.append(quote)

                            except Exception as e:
                                self.logger.warning(f"解析行数据失败: {str(e)}")

        except Exception as e:
            self.logger.error(f"网页爬取失败: {str(e)}")

        return quotes

    def crawl_historical_data(self, symbol: str, period: str = 'daily',
                              start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
        """
        爬取历史数据

        Args:
            symbol: 期货品种
            period: 周期
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            历史数据列表
        """
        self.logger.info(f"爬取 {symbol} 历史数据...")

        # 同花顺历史K线接口
        url = f"{self.api_base}/future/kline"

        params = {
            'code': symbol,
            'period': self._get_period_code(period),
            'type': 'before'
        }

        try:
            response = self._request(url, params=params)
            if not response:
                return []

            time.sleep(self._get_delay())

            data = response.json()

            historical = []
            if 'data' in data:
                for item in data['data']:
                    historical.append({
                        'symbol': symbol,
                        'date': item.get('date', ''),
                        'open': float(item.get('open', 0)),
                        'high': float(item.get('high', 0)),
                        'low': float(item.get('low', 0)),
                        'close': float(item.get('close', 0)),
                        'volume': float(item.get('volume', 0)),
                        'open_interest': float(item.get('amount', 0))
                    })

            self.logger.info(f"获取 {symbol} 历史数据 {len(historical)} 条")
            return historical

        except Exception as e:
            self.logger.error(f"爬取历史数据失败: {str(e)}")
            return []

    def _get_period_code(self, period: str) -> str:
        """获取周期代码"""
        period_map = {
            '1min': '1',
            '5min': '5',
            '15min': '15',
            '30min': '30',
            '60min': '60',
            'daily': '101',
            'weekly': '102',
            'monthly': '103'
        }
        return period_map.get(period, '101')

    def crawl_hot_ranking(self) -> List[Dict[str, Any]]:
        """
        爬取热门排名

        Returns:
            热门排名列表
        """
        self.logger.info("爬取同花顺热门排名...")

        rankings = []

        try:
            url = f"{self.api_base}/future/rank"

            headers = {
                'Referer': 'https://q.10jqka.com.cn/futures/',
                'X-Requested-With': 'XMLHttpRequest'
            }

            response = self._request(url, headers=headers)

            if response:
                data = response.json()

                # 解析排名数据
                if 'data' in data:
                    for item in data['data']:
                        rankings.append({
                            'symbol': item.get('code', ''),
                            'name': item.get('name', ''),
                            'rank': item.get('rank', 0),
                            'score': float(item.get('score', 0))
                        })

        except Exception as e:
            self.logger.error(f"爬取热门排名失败: {str(e)}")

        return rankings

    def crawl_market_sentiment(self) -> Dict[str, Any]:
        """
        爬取市场情绪指标

        Returns:
            市场情绪数据
        """
        self.logger.info("爬取市场情绪...")

        sentiment = {
            'timestamp': datetime.now(),
            'source': '同花顺'
        }

        try:
            url = "https://q.10jqka.com.cn/futures/"

            response = self._request(url)

            if response:
                soup = self._parse_html(response.text)

                # 解析市场情绪指标
                # 需要根据实际页面结构提取

        except Exception as e:
            self.logger.error(f"爬取市场情绪失败: {str(e)}")

        return sentiment
