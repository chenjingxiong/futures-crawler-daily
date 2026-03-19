# -*- coding: utf-8 -*-
"""
新浪期货数据爬虫
爬取新浪财经的期货行情数据
"""

import json
import re
from typing import Dict, List, Optional, Any
from datetime import datetime

from ..base_crawler import BaseCrawler, DataNormalizer


class SinaCrawler(BaseCrawler):
    """新浪期货爬虫"""

    # 新浪期货品种代码映射
    MARKET_MAP = {
        'SHFE': '金属',       # 上期所
        'DCE': '大连',        # 大商所
        'CZCE': '郑州',       # 郑商所
        'CFFEX': '股指',      # 中金所
    }

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.api_base = "https://hq.sinajs.cn"

    def _get_sina_symbol(self, symbol: str) -> str:
        """
        转换为新浪期货代码格式

        Args:
            symbol: 标准品种代码

        Returns:
            新浪代码格式
        """
        # 新浪期货代码格式: 品种+合约+月份
        # 例如: RB0 (螺纹钢主力), IF0 (股指主力)

        # 判断交易所
        if symbol.startswith(('CU', 'AL', 'ZN', 'PB', 'NI', 'SN', 'RB', 'HC', 'AU', 'AG', 'WR')):
            market = 'SHFE'
        elif symbol.startswith(('I', 'J', 'JM', 'A', 'M', 'Y', 'P', 'C', 'CS', 'L', 'V', 'PP', 'BB', 'FB', 'JD', 'LH')):
            market = 'DCE'
        elif symbol.startswith(('IF', 'IH', 'IC', 'IM')):
            market = 'CFFEX'
        else:
            market = 'CZCE'

        # 构建新浪代码
        base = symbol[:2] if len(symbol) > 2 else symbol

        # 主力合约标识
        if len(symbol) <= 2:
            suffix = '0'  # 主力合约
        else:
            suffix = symbol[2:]

        sina_code = f"{base}{suffix}"

        return sina_code

    def _parse_sina_quote(self, content: str, symbol: str) -> Optional[Dict[str, Any]]:
        """
        解析新浪行情数据

        Args:
            content: 新浪返回的数据内容
            symbol: 品种代码

        Returns:
            解析后的行情字典
        """
        try:
            # 新浪数据格式: 逗号分隔
            # 格式: 名字,开盘价,最高价,最低价,收盘价,买一价,卖一价,成交量,成交额,持仓量,涨跌,涨跌幅,日期,时间
            parts = content.strip().split(',')

            if len(parts) < 12:
                return None

            quote = {
                'symbol': symbol,
                'name': parts[0].strip('"'),
                'open': float(parts[1]) if parts[1] else 0,
                'high': float(parts[2]) if parts[2] else 0,
                'low': float(parts[3]) if parts[3] else 0,
                'close': float(parts[4]) if parts[4] else 0,
                'bid1': float(parts[5]) if parts[5] else 0,
                'ask1': float(parts[6]) if parts[6] else 0,
                'volume': float(parts[7]) if parts[7] else 0,
                'turnover': float(parts[8]) if parts[8] else 0,
                'open_interest': float(parts[9]) if parts[9] else 0,
                'change': float(parts[10]) if parts[10] else 0,
                'change_percent': float(parts[11]) if parts[11] else 0,
                'price': float(parts[4]) if parts[4] else 0,  # 使用收盘价作为最新价
                'timestamp': datetime.now(),
            }

            # 添加涨跌状态
            if quote['change'] > 0:
                quote['status'] = '上涨'
            elif quote['change'] < 0:
                quote['status'] = '下跌'
            else:
                quote['status'] = '平盘'

            return quote

        except (ValueError, IndexError) as e:
            self.logger.warning(f"解析新浪行情失败 {symbol}: {str(e)}")
            return None

    def crawl_realtime_quote(self, symbols: List[str] = None) -> List[Dict[str, Any]]:
        """
        爬取实时行情

        Args:
            symbols: 期货品种列表

        Returns:
            行情数据列表
        """
        self.logger.info("开始爬取新浪期货实时行情...")

        # 默认品种列表
        if symbols is None:
            symbols = [
                # 金属
                'CU0', 'AL0', 'ZN0', 'PB0', 'NI0', 'SN0', 'AU0', 'AG0',
                # 黑色
                'RB0', 'HC0', 'I0', 'J0', 'JM0',
                # 化工
                'RU0', 'BU0', 'FG0', 'MA0', 'PP0', 'L0', 'V0',
                # 农产品
                'A0', 'M0', 'Y0', 'P0', 'C0', 'CS0', 'JD0', 'LH0',
                # 金融
                'IF0', 'IH0', 'IC0', 'T0', 'TF0'
            ]

        all_quotes = []

        for symbol in symbols:
            try:
                sina_symbol = self._get_sina_symbol(symbol)
                url = f"{self.api_base}/list={sina_symbol}"

                response = self._request(url)
                if not response:
                    continue

                time.sleep(self._get_delay())

                # 解析响应
                # 格式: var hq_str_RB0="螺纹钢,3200,3250,...";
                match = re.search(r'="([^"]+)"', response.text)
                if not match:
                    continue

                content = match.group(1)
                quote = self._parse_sina_quote(content, symbol)

                if quote and DataNormalizer.validate_data(quote):
                    all_quotes.append(quote)

            except Exception as e:
                self.logger.error(f"爬取 {symbol} 失败: {str(e)}")

        # 标准化数据
        normalized = [DataNormalizer.normalize_quote(q, '新浪期货') for q in all_quotes]

        self.logger.info(f"新浪期货爬取完成: 共 {len(normalized)} 条数据")
        return normalized

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

        # 新浪历史数据接口
        # 注意: 新浪的历史数据接口可能变化，需要根据实际情况调整

        url = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php"

        sina_symbol = self._get_sina_symbol(symbol)

        params = {
            'Market': 'FF',
            'symbol': sina_symbol.replace('0', ''),
            'scale': '240',  # 日线
            'ma': 'no',
            'datalen': '1000'
        }

        try:
            response = self._request(url, params=params)
            if not response:
                return []

            time.sleep(self._get_delay())

            # 新浪返回的是JavaScript格式的数据
            # 需要根据实际返回格式解析
            data = json.loads(response.text)

            historical = []
            if isinstance(data, list):
                for item in data:
                    historical.append({
                        'symbol': symbol,
                        'date': item.get('day', ''),
                        'open': float(item.get('open', 0)),
                        'high': float(item.get('high', 0)),
                        'low': float(item.get('low', 0)),
                        'close': float(item.get('close', 0)),
                        'volume': float(item.get('volume', 0))
                    })

            self.logger.info(f"获取 {symbol} 历史数据 {len(historical)} 条")
            return historical

        except Exception as e:
            self.logger.error(f"爬取历史数据失败: {str(e)}")
            return []

    def crawl_market_overview(self) -> Dict[str, Any]:
        """
        爬取市场概览

        Returns:
            市场概览数据
        """
        self.logger.info("爬取新浪期货市场概览...")

        url = "http://finance.sina.com.cn/futuremarket/"

        try:
            response = self._request(url)
            if not response:
                return {}

            soup = self._parse_html(response.text)

            # 解析市场概览
            overview = {
                'timestamp': datetime.now(),
                'source': '新浪期货'
            }

            # 这里可以根据新浪期货页面的实际结构进行解析
            # 提取主要合约行情、市场指数等

            return overview

        except Exception as e:
            self.logger.error(f"爬取市场概览失败: {str(e)}")
            return {}

    def crawl_hot_contracts(self) -> List[Dict[str, Any]]:
        """
        爬取热门合约

        Returns:
            热门合约列表
        """
        self.logger.info("爬取热门合约...")

        # 新浪热门合约API
        url = "https://hq.sinajs.cn/list=hf_ALL"

        try:
            response = self._request(url)
            if not response:
                return []

            # 解析热门合约数据
            # 需要根据实际返回格式解析

            return []

        except Exception as e:
            self.logger.error(f"爬取热门合约失败: {str(e)}")
            return []
