# -*- coding: utf-8 -*-
"""
数据存储模块
支持CSV、JSON、数据库等多种存储方式
"""

import os
import json
import csv
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Float, DateTime, Integer, Text


Base = declarative_base()


class FuturesQuote(Base):
    """期货行情数据表"""
    __tablename__ = 'futures_quotes'

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), index=True, nullable=False)
    name = Column(String(50))
    price = Column(Float)
    change = Column(Float)
    change_percent = Column(Float)
    volume = Column(Float)
    open_interest = Column(Float)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    bid1 = Column(Float)
    ask1 = Column(Float)
    turnover = Column(Float)
    status = Column(String(10))
    exchange = Column(String(20))
    source = Column(String(20))
    timestamp = Column(DateTime, index=True)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'symbol': self.symbol,
            'name': self.name,
            'price': self.price,
            'change': self.change,
            'change_percent': self.change_percent,
            'volume': self.volume,
            'open_interest': self.open_interest,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'bid1': self.bid1,
            'ask1': self.ask1,
            'turnover': self.turnover,
            'status': self.status,
            'exchange': self.exchange,
            'source': self.source,
            'timestamp': self.timestamp.isoformat() if self.timestamp else None
        }


class FuturesHistorical(Base):
    """期货历史数据表"""
    __tablename__ = 'futures_historical'

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), index=True, nullable=False)
    date = Column(String(20), index=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    open_interest = Column(Float)
    turnover = Column(Float)
    period = Column(String(10))


class DataStorage:
    """数据存储基类"""

    def __init__(self, config: Dict[str, Any]):
        """
        初始化存储

        Args:
            config: 配置字典
        """
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self._setup_directories()

    def _setup_directories(self):
        """创建必要的目录"""
        storage_config = self.config.get('storage', {})

        # 创建CSV目录
        csv_path = storage_config.get('csv', {}).get('path', './data/csv')
        os.makedirs(csv_path, exist_ok=True)

        # 创建JSON目录
        json_path = storage_config.get('json', {}).get('path', './data/json')
        os.makedirs(json_path, exist_ok=True)

    def save(self, data: List[Dict[str, Any]], data_type: str = 'quote',
             category: str = 'all') -> bool:
        """
        保存数据

        Args:
            data: 数据列表
            data_type: 数据类型 (quote, historical)
            category: 数据分类

        Returns:
            是否成功
        """
        if not data:
            self.logger.warning("没有数据需要保存")
            return False

        storage_type = self.config.get('storage', {}).get('type', 'both')

        success = True

        try:
            if storage_type in ['csv', 'both']:
                if not self._save_csv(data, data_type, category):
                    success = False

            if storage_type in ['json', 'both']:
                if not self._save_json(data, data_type, category):
                    success = False

            if storage_type in ['database', 'both']:
                if not self._save_database(data, data_type):
                    success = False

        except Exception as e:
            self.logger.error(f"保存数据失败: {str(e)}")
            return False

        return success

    def _save_csv(self, data: List[Dict[str, Any]], data_type: str,
                  category: str) -> bool:
        """保存为CSV"""
        try:
            storage_config = self.config.get('storage', {})
            csv_config = storage_config.get('csv', {})

            csv_path = csv_config.get('path', './data/csv')
            filename_format = csv_config.get('filename_format', '{category}_{date}.csv')

            date_str = datetime.now().strftime('%Y%m%d')
            filename = filename_format.format(
                category=category,
                date=date_str
            )
            filepath = os.path.join(csv_path, filename)

            # 转换为DataFrame
            df = pd.DataFrame(data)

            # 如果文件存在，追加；否则创建新文件
            if os.path.exists(filepath):
                existing_df = pd.read_csv(filepath)
                df = pd.concat([existing_df, df], ignore_index=True)
                # 去重
                df = df.drop_duplicates(subset=['symbol', 'timestamp'], keep='last')

            df.to_csv(filepath, index=False, encoding='utf-8-sig')

            self.logger.info(f"CSV保存成功: {filepath} ({len(data)} 条)")
            return True

        except Exception as e:
            self.logger.error(f"CSV保存失败: {str(e)}")
            return False

    def _save_json(self, data: List[Dict[str, Any]], data_type: str,
                   category: str) -> bool:
        """保存为JSON"""
        try:
            storage_config = self.config.get('storage', {})
            json_config = storage_config.get('json', {})

            json_path = json_config.get('path', './data/json')
            filename_format = json_config.get('filename_format', '{category}_{date}.json')

            date_str = datetime.now().strftime('%Y%m%d')
            filename = filename_format.format(
                category=category,
                date=date_str
            )
            filepath = os.path.join(json_path, filename)

            # 转换datetime为字符串
            processed_data = []
            for item in data:
                processed_item = {}
                for key, value in item.items():
                    if isinstance(value, datetime):
                        processed_item[key] = value.isoformat()
                    else:
                        processed_item[key] = value
                processed_data.append(processed_item)

            # 如果文件存在，读取并合并
            existing_data = []
            if os.path.exists(filepath):
                with open(filepath, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)

            # 合并数据
            all_data = existing_data + processed_data

            # 去重
            seen = set()
            unique_data = []
            for item in all_data:
                key = (item.get('symbol'), item.get('timestamp'))
                if key not in seen:
                    seen.add(key)
                    unique_data.append(item)

            # 保存
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(unique_data, f, ensure_ascii=False, indent=2)

            self.logger.info(f"JSON保存成功: {filepath} ({len(data)} 条)")
            return True

        except Exception as e:
            self.logger.error(f"JSON保存失败: {str(e)}")
            return False

    def _save_database(self, data: List[Dict[str, Any]], data_type: str) -> bool:
        """保存到数据库"""
        try:
            storage_config = self.config.get('storage', {})
            db_config = storage_config.get('database', {})

            db_type = db_config.get('type', 'sqlite')

            # 创建数据库连接
            if db_type == 'sqlite':
                db_path = db_config.get('sqlite_path', './data/futures.db')
                os.makedirs(os.path.dirname(db_path), exist_ok=True)
                db_url = f'sqlite:///{db_path}'
            elif db_type == 'mysql':
                db_url = (f"mysql+pymysql://{db_config.get('mysql_user', 'root')}:"
                         f"{db_config.get('mysql_password', '')}@"
                         f"{db_config.get('mysql_host', 'localhost')}:"
                         f"{db_config.get('mysql_port', 3306)}/"
                         f"{db_config.get('mysql_database', 'futures_db')}")
            else:
                self.logger.warning(f"不支持的数据库类型: {db_type}")
                return False

            # 创建引擎
            engine = create_engine(db_url, echo=False)

            # 创建表
            Base.metadata.create_all(engine)

            # 创建Session
            SessionLocal = sessionmaker(bind=engine)
            session = SessionLocal()

            try:
                # 保存数据
                if data_type == 'quote':
                    for item in data:
                        quote = FuturesQuote(
                            symbol=item.get('symbol', ''),
                            name=item.get('name', ''),
                            price=float(item.get('price', 0)),
                            change=float(item.get('change', 0)),
                            change_percent=float(item.get('change_percent', 0)),
                            volume=float(item.get('volume', 0)),
                            open_interest=float(item.get('open_interest', 0)),
                            open=float(item.get('open', 0)),
                            high=float(item.get('high', 0)),
                            low=float(item.get('low', 0)),
                            close=float(item.get('close', 0)),
                            bid1=float(item.get('bid1', 0)),
                            ask1=float(item.get('ask1', 0)),
                            turnover=float(item.get('turnover', 0)),
                            status=item.get('status', ''),
                            exchange=item.get('exchange', ''),
                            source=item.get('source', ''),
                            timestamp=item.get('timestamp', datetime.now())
                        )
                        session.merge(quote)  # 使用merge避免重复

                elif data_type == 'historical':
                    for item in data:
                        hist = FuturesHistorical(
                            symbol=item.get('symbol', ''),
                            date=item.get('date', ''),
                            open=float(item.get('open', 0)),
                            high=float(item.get('high', 0)),
                            low=float(item.get('low', 0)),
                            close=float(item.get('close', 0)),
                            volume=float(item.get('volume', 0)),
                            open_interest=float(item.get('open_interest', 0)),
                            turnover=float(item.get('turnover', 0)),
                            period=item.get('period', 'daily')
                        )
                        session.merge(hist)

                session.commit()
                self.logger.info(f"数据库保存成功: {len(data)} 条")
                return True

            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()

        except Exception as e:
            self.logger.error(f"数据库保存失败: {str(e)}")
            return False

    def load(self, data_type: str = 'quote', symbol: str = None,
             start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
        """
        加载数据

        Args:
            data_type: 数据类型
            symbol: 品种代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            数据列表
        """
        storage_type = self.config.get('storage', {}).get('type', 'both')

        # 优先从数据库加载
        if storage_type in ['database', 'both']:
            try:
                return self._load_database(data_type, symbol, start_date, end_date)
            except Exception as e:
                self.logger.warning(f"数据库加载失败: {str(e)}")

        # 从CSV加载
        if storage_type in ['csv', 'both']:
            try:
                return self._load_csv(data_type, symbol, start_date, end_date)
            except Exception as e:
                self.logger.warning(f"CSV加载失败: {str(e)}")

        return []

    def _load_database(self, data_type: str, symbol: str = None,
                       start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
        """从数据库加载"""
        storage_config = self.config.get('storage', {})
        db_config = storage_config.get('database', {})

        db_type = db_config.get('type', 'sqlite')

        if db_type == 'sqlite':
            db_path = db_config.get('sqlite_path', './data/futures.db')
            db_url = f'sqlite:///{db_path}'
        elif db_type == 'mysql':
            db_url = (f"mysql+pymysql://{db_config.get('mysql_user', 'root')}:"
                     f"{db_config.get('mysql_password', '')}@"
                     f"{db_config.get('mysql_host', 'localhost')}:"
                     f"{db_config.get('mysql_port', 3306)}/"
                     f"{db_config.get('mysql_database', 'futures_db')}")
        else:
            return []

        engine = create_engine(db_url)
        Session = sessionmaker(bind=engine)
        session = Session()

        try:
            if data_type == 'quote':
                query = session.query(FuturesQuote)
                if symbol:
                    query = query.filter(FuturesQuote.symbol == symbol)
                if start_date:
                    query = query.filter(FuturesQuote.timestamp >= start_date)
                if end_date:
                    query = query.filter(FuturesQuote.timestamp <= end_date)

                results = query.all()
                return [r.to_dict() for r in results]

            elif data_type == 'historical':
                query = session.query(FuturesHistorical)
                if symbol:
                    query = query.filter(FuturesHistorical.symbol == symbol)
                if start_date:
                    query = query.filter(FuturesHistorical.date >= start_date)
                if end_date:
                    query = query.filter(FuturesHistorical.date <= end_date)

                results = query.all()
                return [r.to_dict() for r in results]

        finally:
            session.close()

        return []

    def _load_csv(self, data_type: str, symbol: str = None,
                  start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
        """从CSV加载"""
        storage_config = self.config.get('storage', {})
        csv_path = storage_config.get('csv', {}).get('path', './data/csv')

        # 查找匹配的CSV文件
        csv_files = list(Path(csv_path).glob('*.csv'))

        all_data = []
        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file)
                data = df.to_dict('records')

                # 过滤
                if symbol:
                    data = [d for d in data if d.get('symbol') == symbol]
                if start_date:
                    data = [d for d in data if d.get('timestamp', '') >= start_date]
                if end_date:
                    data = [d for d in data if d.get('timestamp', '') <= end_date]

                all_data.extend(data)

            except Exception as e:
                self.logger.warning(f"读取 {csv_file} 失败: {str(e)}")

        return all_data
