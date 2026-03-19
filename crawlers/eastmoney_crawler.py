# -*- coding: utf-8 -*-
"""
东方财富期货数据爬虫
爬取东方财富网的期货行情数据
"""

import json
import re
from typing import Dict, List, Optional, Any
from datetime import datetime

from ..base_crawler import BaseCrawler, DataNormalizer


class EastMoneyCrawler(BaseCrawler):
    """东方财富期货爬虫"""

    # 东方财富期货品种代码映射
    EXCHANGE_MAP = {
        'SHFE': '上期所',      # 上海期货交易所
        'DCE': '大商所',       # 大连商品交易所
        'CZCE': '郑商所',      # 郑州商品交易所
        'CFFEX': '中金所',     # 中国金融期货交易所
        'GFEX': '广期所',      # 广州期货交易所
        'INE': '能源中心'      # 上海国际能源交易中心
    }

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.api_base = "https://futsseapi.eastmoney.com/list"

    def _get_api_params(self, market: str = 'all') -> Dict[str, str]:
        """
        获取API参数

        Args:
            market: 市场类型

        Returns:
            API参数字典
        """
        return {
            'cb': 'jQuery',
            'ut': 'f1',
            'pn': '1',
            'pz': '500',
            'po': '1',
            'np': '1',
            'fltt': '2',
            'invt': '2',
            'fid': 'f3',
            'fs': self._get_market_filter(market),
            'fields': 'f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152',
            '_': str(int(datetime.now().timestamp() * 1000))
        }

    def _get_market_filter(self, market: str) -> str:
        """获取市场过滤条件"""
        filters = {
            'all': 'm:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23',
            'shfe': 'm:0+t:6',      # 上期所
            'dce': 'm:0+t:80',      # 大商所
            'czce': 'm:1+t:2',      # 郑商所
            'cffex': 'm:1+t:23',    # 中金所
        }
        return filters.get(market, filters['all'])

    def _parse_api_response(self, response_text: str) -> List[Dict[str, Any]]:
        """
        解析API响应

        Args:
            response_text: 响应文本

        Returns:
            解析后的数据列表
        """
        # 移除JSONP回调
        json_str = re.sub(r'^jQuery\d+_\d+\(|\);?$', '', response_text.strip())

        try:
            data = json.loads(json_str)
        except json.JSONDecodeError:
            self.logger.error(f"API响应解析失败")
            return []

        if not data or 'data' not in data or 'diff' not in data['data']:
            return []

        quotes = []
        for item in data['data']['diff']:
            quote = self._parse_quote_item(item)
            if quote and DataNormalizer.validate_data(quote):
                quotes.append(quote)

        return quotes

    def _parse_quote_item(self, item: List) -> Optional[Dict[str, Any]]:
        """
        解析单条行情数据

        Args:
            item: API返回的单条数据

        Returns:
            解析后的行情字典
        """
        try:
            # API字段映射 (根据索引)
            quote = {
                'symbol': str(item[12] + item[13]),  # 品种代码
                'name': item[14],                     # 品种名称
                'price': float(item[2]),              # 最新价
                'open': float(item[4]),               # 开盘价
                'high': float(item[5]),               # 最高价
                'low': float(item[6]),                # 最低价
                'close': float(item[3]),              # 昨收价
                'volume': float(item[7]),             # 成交量
                'open_interest': float(item[8]),      # 持仓量
                'change': float(item[31]),            # 涨跌额
                'change_percent': float(item[32]),    # 涨跌幅%
                'turnover': float(item[9]),           # 成交额
                'timestamp': datetime.now(),
            }

            # 添加交易所信息
            exchange_code = item[12]
            quote['exchange'] = self.EXCHANGE_MAP.get(exchange_code, '未知')

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

            return quote

        except (IndexError, ValueError, TypeError) as e:
            self.logger.warning(f"解析行情项失败: {str(e)}")
            return None

    def crawl_realtime_quote(self, symbols: List[str] = None) -> List[Dict[str, Any]]:
        """
        爬取实时行情

        Args:
            symbols: 期货品种列表（可选，不传则爬取全部）

        Returns:
            行情数据列表
        """
        self.logger.info("开始爬取东方财富实时行情...")

        # 获取所有市场数据
        all_quotes = []

        for market in ['all']:  # 可扩展为 ['shfe', 'dce', 'czce', 'cffex']
            try:
                params = self._get_api_params(market)
                response = self._request(
                    f"{self.api_base}",
                    params=params
                )

                if response:
                    time.sleep(self._get_delay())
                    quotes = self._parse_api_response(response.text)

                    # 过滤指定品种
                    if symbols:
                        quotes = [q for q in quotes if q['symbol'] in symbols]

                    all_quotes.extend(quotes)
                    self.logger.info(f"{market} 获取 {len(quotes)} 条数据")

            except Exception as e:
                self.logger.error(f"爬取 {market} 失败: {str(e)}")

        # 标准化数据
        normalized = [DataNormalizer.normalize_quote(q, '东方财富') for q in all_quotes]

        self.logger.info(f"东方财富爬取完成: 共 {len(normalized)} 条数据")
        return normalized

    def crawl_historical_data(self, symbol: str, period: str = 'daily',
                              start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
        """
        爬取历史数据

        Args:
            symbol: 期货品种
            period: 周期 (daily, weekly, monthly)
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            历史数据列表
        """
        self.logger.info(f"爬取 {symbol} 历史数据...")

        # 东方财富历史数据API
        url = "https://futsseapi.eastmoney.com/kline"

        # 解析品种代码
        if len(symbol) >= 2:
            base_symbol = symbol[:-2]  # 主合约代码
        else:
            base_symbol = symbol

        # 周期映射
        period_map = {
            'daily': '101',
            'weekly': '102',
            'monthly': '103'
        }
        klt = period_map.get(period, '101')

        params = {
            'ut': 'fa5fd1943c7b386f172d6893dbfba10b',
            'dpt': 'wzf',
            'cb': 'jsonp',
            'secid': f'0.{self._get_symbol_code(symbol)}',
            'fields1': 'f1,f2,f3,f4,f5,f6',
            'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
            'klt': klt,
            'fqt': '1',
            'end': '20500101',
            'lmt': '1000'
        }

        try:
            response = self._request(url, params=params)
            if not response:
                return []

            time.sleep(self._get_delay())

            # 解析响应
            json_str = re.sub(r'^jsonp\(|\);?$', '', response.text.strip())
            data = json.loads(json_str)

            if not data or 'data' not in data:
                return []

            # 解析K线数据
            historical = []
            for item in data['data']:
                historical.append({
                    'symbol': symbol,
                    'date': item[0],
                    'open': float(item[1]),
                    'high': float(item[2]),
                    'low': float(item[3]),
                    'close': float(item[4]),
                    'volume': float(item[5]),
                    'open_interest': float(item[6]) if len(item) > 6 else 0,
                    'turnover': float(item[7]) if len(item) > 7 else 0
                })

            self.logger.info(f"获取 {symbol} 历史数据 {len(historical)} 条")
            return historical

        except Exception as e:
            self.logger.error(f"爬取历史数据失败: {str(e)}")
            return []

    def _get_symbol_code(self, symbol: str) -> str:
        """
        获取品种代码映射

        Args:
            symbol: 品种代码

        Returns:
            映射后的代码
        """
        # 常见品种代码映射
        code_map = {
            'CU': '800', 'AL': '801', 'ZN': '802', 'PB': '803', 'NI': '804', 'SN': '805',
            'RB': '810', 'HC': '811', 'AU': '820', 'AG': '821',
            'I': '850', 'J': '860', 'JM': '861', 'A': '870', 'M': '871', 'Y': '872', 'P': '873',
            'C': '880', 'CS': '881', 'L': '890', 'V': '891', 'PP': '892', 'MA': '910',
            'IF': '8300', 'IH': '8301', 'IC': '8302', 'T': '8400', 'TF': '8401'
        }

        base = symbol[:2] if len(symbol) > 2 else symbol
        return code_map.get(base, '800')

    def crawl_futures_list(self) -> List[Dict[str, Any]]:
        """
        爬取期货品种列表

        Returns:
            品种列表
        """
        self.logger.info("爬取期货品种列表...")

        url = "https://futsseapi.eastmoney.com/list"

        params = {
            'cb': 'jQuery',
            'ut': 'f1',
            'pn': '1',
            'pz': '1000',
            'po': '1',
            'np': '1',
            'fltt': '2',
            'invt': '2',
            'fid': 'f3',
            'fs': 'm:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:1+t:81',
            'fields': 'f12,f13,f14',
            '_': str(int(datetime.now().timestamp() * 1000))
        }

        try:
            response = self._request(url, params=params)
            if not response:
                return []

            quotes = self._parse_api_response(response.text)

            # 提取唯一品种
            futures_list = {}
            for quote in quotes:
                base_symbol = quote['symbol'][:-2] if len(quote['symbol']) > 2 else quote['symbol']
                if base_symbol not in futures_list:
                    futures_list[base_symbol] = {
                        'symbol': base_symbol,
                        'name': quote['name'][:2] if quote['name'] else base_symbol,
                        'exchange': quote.get('exchange', '未知')
                    }

            result = list(futures_list.values())
            self.logger.info(f"获取期货品种列表: {len(result)} 个")
            return result

        except Exception as e:
            self.logger.error(f"爬取品种列表失败: {str(e)}")
            return []

    def crawl_market_summary(self) -> Dict[str, Any]:
        """
        爬取市场概况

        Returns:
            市场概况数据
        """
        self.logger.info("爬取市场概况...")

        quotes = self.crawl_realtime_quote()

        if not quotes:
            return {}

        # 统计分析
        rising = [q for q in quotes if q['change'] > 0]
        falling = [q for q in quotes if q['change'] < 0]
        flat = [q for q in quotes if q['change'] == 0]

        summary = {
            'total_count': len(quotes),
            'rising_count': len(rising),
            'falling_count': len(falling),
            'flat_count': len(flat),
            'limit_up_count': len([q for q in quotes if q['change_percent'] >= 0.09]),
            'limit_down_count': len([q for q in quotes if q['change_percent'] <= -0.09]),
            'top_gainers': sorted(quotes, key=lambda x: x['change_percent'], reverse=True)[:10],
            'top_losers': sorted(quotes, key=lambda x: x['change_percent'])[:10],
            'most_active': sorted(quotes, key=lambda x: x['volume'], reverse=True)[:10],
            'timestamp': datetime.now()
        }

        return summary
