# -*- coding: utf-8 -*-
"""
期货数据爬虫基础类
提供通用的爬虫功能和方法
"""

import os
import time
import random
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent


class BaseCrawler(ABC):
    """爬虫基类"""

    def __init__(self, config: Dict[str, Any]):
        """
        初始化爬虫

        Args:
            config: 配置字典
        """
        self.config = config
        self.name = config.get('name', 'Unknown')
        self.base_url = config.get('base_url', '')
        self.timeout = config.get('timeout', 30)
        self.retry = config.get('retry', 3)

        # 初始化日志
        self._setup_logging()

        # 初始化User-Agent轮换
        self.ua = UserAgent()

        # 创建Session
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        })

        # 统计信息
        self.stats = {
            'success': 0,
            'failed': 0,
            'total': 0
        }

    def _setup_logging(self):
        """设置日志"""
        log_config = self.config.get('logging', {})
        log_level = log_config.get('level', 'INFO')
        log_path = log_config.get('path', './logs')

        os.makedirs(log_path, exist_ok=True)

        log_file = os.path.join(log_path, f'{self.name}_{datetime.now().strftime("%Y%m%d")}.log')

        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(self.__class__.__name__)

    def _get_headers(self) -> Dict[str, str]:
        """获取随机请求头"""
        headers = {
            'User-Agent': self.ua.random,
            'Referer': self.base_url
        }
        return headers

    def _request(self, url: str, method: str = 'GET', **kwargs) -> Optional[requests.Response]:
        """
        发送HTTP请求（带重试）

        Args:
            url: 请求URL
            method: 请求方法
            **kwargs: 其他请求参数

        Returns:
            Response对象或None
        """
        for attempt in range(self.retry):
            try:
                # 更新headers
                headers = self._get_headers()
                if 'headers' in kwargs:
                    headers.update(kwargs['headers'])

                kwargs['headers'] = headers
                kwargs['timeout'] = kwargs.get('timeout', self.timeout)

                response = self.session.request(method, url, **kwargs)
                response.raise_for_status()
                response.encoding = 'utf-8'

                self.stats['success'] += 1
                return response

            except requests.RequestException as e:
                self.logger.warning(f"请求失败 (尝试 {attempt + 1}/{self.retry}): {url} - {str(e)}")
                if attempt < self.retry - 1:
                    time.sleep(random.uniform(1, 3))
                else:
                    self.stats['failed'] += 1
                    self.logger.error(f"请求彻底失败: {url}")
                    return None

    def _parse_html(self, html: str) -> BeautifulSoup:
        """
        解析HTML

        Args:
            html: HTML字符串

        Returns:
            BeautifulSoup对象
        """
        return BeautifulSoup(html, 'lxml')

    def _clean_number(self, value: str) -> float:
        """
        清理数字字符串

        Args:
            value: 数字字符串

        Returns:
            清理后的浮点数
        """
        if not value:
            return 0.0

        # 移除逗号、空格等
        cleaned = str(value).replace(',', '').replace(' ', '').strip()

        # 处理百分号
        if '%' in cleaned:
            cleaned = cleaned.replace('%', '')
            return float(cleaned) / 100

        try:
            return float(cleaned)
        except ValueError:
            return 0.0

    def _get_delay(self) -> float:
        """获取请求延迟时间"""
        interval = self.config.get('request_interval', 1)
        return random.uniform(interval * 0.5, interval * 1.5)

    @abstractmethod
    def crawl_realtime_quote(self, symbols: List[str] = None) -> List[Dict[str, Any]]:
        """
        爬取实时行情

        Args:
            symbols: 期货品种列表

        Returns:
            行情数据列表
        """
        pass

    @abstractmethod
    def crawl_historical_data(self, symbol: str, period: str = 'daily',
                              start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
        """
        爬取历史数据

        Args:
            symbol: 期货品种
            period: 周期 (daily, weekly, monthly)
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            历史数据列表
        """
        pass

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        self.stats['total'] = self.stats['success'] + self.stats['failed']
        return self.stats

    def close(self):
        """关闭爬虫"""
        self.session.close()
        self.logger.info(f"爬虫关闭: {self.name}")


class MultiSourceCrawler:
    """多数据源爬虫管理器"""

    def __init__(self, crawlers: List[BaseCrawler]):
        """
        初始化多源爬虫

        Args:
            crawlers: 爬虫实例列表
        """
        self.crawlers = crawlers
        self.logger = logging.getLogger(self.__class__.__name__)

    def crawl_all_sources(self, symbols: List[str] = None,
                         max_workers: int = 5) -> Dict[str, List[Dict[str, Any]]]:
        """
        并发爬取所有数据源

        Args:
            symbols: 期货品种列表
            max_workers: 最大并发数

        Returns:
            {源名称: 数据列表}
        """
        results = {}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_crawler = {
                executor.submit(crawler.crawl_realtime_quote, symbols): crawler
                for crawler in self.crawlers
            }

            for future in as_completed(future_to_crawler):
                crawler = future_to_crawler[future]
                try:
                    data = future.result()
                    if data:
                        results[crawler.name] = data
                        self.logger.info(f"{crawler.name} 爬取成功: {len(data)} 条数据")
                except Exception as e:
                    self.logger.error(f"{crawler.name} 爬取失败: {str(e)}")

        return results

    def merge_results(self, results: Dict[str, List[Dict[str, Any]]],
                     priority: List[str] = None) -> List[Dict[str, Any]]:
        """
        合并多源数据

        Args:
            results: 多源数据
            priority: 优先级顺序（源名称列表）

        Returns:
            合并后的数据列表
        """
        if not results:
            return []

        # 默认优先级
        if priority is None:
            priority = list(results.keys())

        # 按优先级合并数据
        merged = {}
        for source in priority:
            if source not in results:
                continue

            for item in results[source]:
                symbol = item.get('symbol', '')
                if symbol and symbol not in merged:
                    merged[symbol] = item
                    merged[symbol]['source'] = source

        return list(merged.values())

    def close_all(self):
        """关闭所有爬虫"""
        for crawler in self.crawlers:
            crawler.close()


class DataNormalizer:
    """数据标准化器"""

    @staticmethod
    def normalize_quote(data: Dict[str, Any], source: str) -> Dict[str, Any]:
        """
        标准化行情数据格式

        Args:
            data: 原始数据
            source: 数据源

        Returns:
            标准化后的数据
        """
        normalized = {
            'symbol': data.get('symbol', ''),
            'name': data.get('name', ''),
            'price': float(data.get('price', 0)),
            'change': float(data.get('change', 0)),
            'change_percent': float(data.get('change_percent', 0)),
            'volume': float(data.get('volume', 0)),
            'open_interest': float(data.get('open_interest', 0)),
            'open': float(data.get('open', 0)),
            'high': float(data.get('high', 0)),
            'low': float(data.get('low', 0)),
            'close': float(data.get('close', 0)),
            'timestamp': datetime.now(),
            'source': source
        }

        return normalized

    @staticmethod
    def validate_data(data: Dict[str, Any]) -> bool:
        """
        验证数据有效性

        Args:
            data: 待验证数据

        Returns:
            是否有效
        """
        # 必填字段检查
        required_fields = ['symbol', 'price']
        for field in required_fields:
            if field not in data or data[field] is None:
                return False

        # 价格合理性检查
        price = float(data.get('price', 0))
        if price <= 0 or price > 1000000:
            return False

        return True
