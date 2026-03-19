#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
期货数据爬虫和分析系统 - 主程序
"""

import os
import sys
import argparse
import logging
from typing import Dict, List, Any
from datetime import datetime
import yaml

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from base_crawler import MultiSourceCrawler
from crawlers import EastMoneyCrawler, SinaCrawler, TonghuashunCrawler
from data_storage import DataStorage
from technical_analysis import TechnicalAnalysis
from recommendation_engine import RecommendationEngine


class FuturesCrawlerApp:
    """期货爬虫应用主类"""

    def __init__(self, config_path: str = None):
        """
        初始化应用

        Args:
            config_path: 配置文件路径
        """
        self.config = self._load_config(config_path)
        self._setup_logging()

        self.logger = logging.getLogger(self.__class__.__name__)

        # 初始化组件
        self.crawlers = self._init_crawlers()
        self.storage = DataStorage(self.config)
        self.analysis = TechnicalAnalysis(self.config)
        self.recommendation = RecommendationEngine(self.config)

    def _load_config(self, config_path: str = None) -> Dict[str, Any]:
        """加载配置文件"""
        if config_path is None:
            config_path = os.path.join(os.path.dirname(__file__), 'config', 'config.yaml')

        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        else:
            # 返回默认配置
            return {
                'sources': {
                    'eastmoney': {'name': '东方财富', 'base_url': 'https://futures.eastmoney.com'},
                    'sina': {'name': '新浪期货', 'base_url': 'http://finance.sina.com.cn/futuremarket'},
                    'tonghuashun': {'name': '同花顺', 'base_url': 'https://q.10jqka.com.cn/futures'}
                },
                'storage': {'type': 'both'},
                'logging': {'level': 'INFO'}
            }

    def _setup_logging(self):
        """设置日志"""
        log_config = self.config.get('logging', {})
        log_level = log_config.get('level', 'INFO')
        log_path = log_config.get('path', './logs')

        os.makedirs(log_path, exist_ok=True)

        log_file = os.path.join(log_path, f'futures_{datetime.now().strftime("%Y%m%d")}.log')

        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )

    def _init_crawlers(self) -> List:
        """初始化爬虫"""
        crawlers = []
        sources = self.config.get('sources', {})

        if sources.get('eastmoney', {}).get('enabled', True):
            crawlers.append(EastMoneyCrawler(sources['eastmoney']))

        if sources.get('sina', {}).get('enabled', True):
            crawlers.append(SinaCrawler(sources['sina']))

        if sources.get('tonghuashun', {}).get('enabled', True):
            crawlers.append(TonghuashunCrawler(sources['tonghuashun']))

        return crawlers

    def crawl_realtime(self, symbols: List[str] = None, save: bool = True) -> List[Dict[str, Any]]:
        """
        爬取实时行情

        Args:
            symbols: 品种列表
            save: 是否保存数据

        Returns:
            行情数据列表
        """
        self.logger.info("=" * 50)
        self.logger.info("开始爬取实时行情...")
        self.logger.info("=" * 50)

        # 使用多源爬虫
        multi_crawler = MultiSourceCrawler(self.crawlers)
        all_results = multi_crawler.crawl_all_sources(symbols)

        # 合并结果
        priority = ['东方财富', '新浪期货', '同花顺']
        quotes = multi_crawler.merge_results(all_results, priority)

        self.logger.info(f"共获取 {len(quotes)} 条行情数据")

        # 保存数据
        if save and quotes:
            self.storage.save(quotes, data_type='quote', category='all')

        multi_crawler.close_all()
        return quotes

    def analyze_and_recommend(self, quotes: List[Dict[str, Any]] = None,
                             print_report: bool = True) -> Dict[str, Any]:
        """
        分析并生成推荐

        Args:
            quotes: 行情数据（如为None则自动爬取）
            print_report: 是否打印报告

        Returns:
            分析结果
        """
        if quotes is None:
            quotes = self.crawl_realtime()

        if not quotes:
            self.logger.warning("没有行情数据，跳过分析")
            return {}

        self.logger.info("=" * 50)
        self.logger.info("开始技术分析...")
        self.logger.info("=" * 50)

        # 技术分析
        analysis_results = self.analysis.batch_analyze(quotes)

        # 生成推荐
        self.logger.info("生成操作建议...")
        recommendations = self.recommendation.generate_batch_recommendations(
            quotes, analysis_results
        )

        # 生成市场汇总
        summary = self.recommendation.generate_market_summary(quotes, recommendations)

        # 打印报告
        if print_report:
            report = self.recommendation.format_report(quotes, recommendations, summary)
            print("\n" + report)

        return {
            'quotes': quotes,
            'analysis': analysis_results,
            'recommendations': [r.to_dict() for r in recommendations],
            'summary': summary
        }

    def run_daily_task(self):
        """执行每日任务"""
        self.logger.info("执行每日分析任务...")

        # 爬取并分析
        result = self.analyze_and_recommend()

        # 保存结果
        if result:
            # 保存推荐结果
            recs = result.get('recommendations', [])
            if recs:
                self.storage.save(recs, data_type='recommendation', category='all')

            # 保存市场汇总
            summary = result.get('summary', {})
            if summary:
                import json
                summary_path = os.path.join(
                    self.config.get('storage', {}).get('json', {}).get('path', './data/json'),
                    f'market_summary_{datetime.now().strftime("%Y%m%d")}.json'
                )
                os.makedirs(os.path.dirname(summary_path), exist_ok=True)
                with open(summary_path, 'w', encoding='utf-8') as f:
                    json.dump(summary, f, ensure_ascii=False, indent=2)

        self.logger.info("每日任务完成")

    def run_scheduled(self):
        """运行定时任务"""
        try:
            import schedule
            import time

            # 设置定时任务
            scheduler_config = self.config.get('scheduler', {})

            if scheduler_config.get('enabled', True):
                for task in scheduler_config.get('tasks', []):
                    if task.get('enabled', True):
                        cron_expr = task.get('cron', '')
                        task_name = task.get('name', 'unknown')

                        # 解析cron表达式并设置任务
                        # 这里使用简单的实现，完整cron解析需要额外库
                        if '*/10' in cron_expr:  # 每10分钟
                            schedule.every(10).minutes.do(self.crawl_realtime)
                        elif '15 15 * * 1-5' in cron_expr:  # 工作日15:15
                            schedule.every().monday.at("15:15").do(self.run_daily_task)
                            schedule.every().tuesday.at("15:15").do(self.run_daily_task)
                            schedule.every().wednesday.at("15:15").do(self.run_daily_task)
                            schedule.every().thursday.at("15:15").do(self.run_daily_task)
                            schedule.every().friday.at("15:15").do(self.run_daily_task)

                        self.logger.info(f"已设置定时任务: {task_name} ({cron_expr})")

            # 运行调度器
            self.logger.info("定时任务调度器已启动，按 Ctrl+C 退出...")
            while True:
                schedule.run_pending()
                time.sleep(1)

        except KeyboardInterrupt:
            self.logger.info("收到退出信号，关闭程序...")
        except ImportError:
            self.logger.warning("schedule库未安装，定时任务不可用")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='期货数据爬虫和分析系统')
    parser.add_argument('-c', '--config', help='配置文件路径')
    parser.add_argument('-m', '--mode', choices=['crawl', 'analyze', 'schedule', 'once'],
                       default='once', help='运行模式')
    parser.add_argument('-s', '--symbols', nargs='+', help='指定期货品种')
    parser.add_argument('--no-save', action='store_true', help='不保存数据')
    parser.add_argument('--quiet', action='store_true', help='静默模式')

    args = parser.parse_args()

    # 创建应用
    app = FuturesCrawlerApp(args.config)

    try:
        if args.mode == 'crawl':
            # 仅爬取
            quotes = app.crawl_realtime(symbols=args.symbols, save=not args.no_save)
            if not args.quiet:
                print(f"\n爬取完成，共获取 {len(quotes)} 条数据")

        elif args.mode == 'analyze':
            # 分析模式
            app.analyze_and_recommend()

        elif args.mode == 'schedule':
            # 定时模式
            app.run_scheduled()

        else:  # once
            # 默认：爬取并分析
            app.analyze_and_recommend()

    except Exception as e:
        logging.error(f"程序执行出错: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()
