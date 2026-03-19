# -*- coding: utf-8 -*-
"""
期货数据爬虫和分析系统
"""

__version__ = '1.0.0'
__author__ = 'Claude Code'

from .base_crawler import BaseCrawler, MultiSourceCrawler, DataNormalizer
from .data_storage import DataStorage
from .technical_analysis import TechnicalAnalysis, TechnicalIndicators
from .recommendation_engine import RecommendationEngine, TradingSignal

__all__ = [
    'BaseCrawler',
    'MultiSourceCrawler',
    'DataNormalizer',
    'DataStorage',
    'TechnicalAnalysis',
    'TechnicalIndicators',
    'RecommendationEngine',
    'TradingSignal'
]
