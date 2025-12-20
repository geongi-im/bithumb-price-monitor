import sqlite3
from datetime import datetime


class DatabaseUtil:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = None

    def connect(self):
        """데이터베이스 연결"""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        return True

    def close(self):
        """연결 종료"""
        if self.conn:
            self.conn.commit()
            self.conn.close()

    def table_exists(self, symbol):
        """
        테이블 존재 여부 확인

        Args:
            symbol: 'BTC', 'XRP', 'ETH'

        Returns:
            bool: 테이블 존재 여부
        """
        table_name = f"bp_price_{symbol.lower()}"
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT name FROM sqlite_master
            WHERE type='table' AND name=?
        ''', (table_name,))
        return cursor.fetchone() is not None

    def create_table(self, symbol):
        """
        종목별 테이블 생성

        Args:
            symbol: 'BTC', 'XRP', 'ETH'
        """
        table_name = f"bp_price_{symbol.lower()}"
        cursor = self.conn.cursor()

        # 테이블 생성
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                idx INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_price REAL NOT NULL,
                high_price REAL NOT NULL,
                low_price REAL NOT NULL,
                reg_date TEXT NOT NULL
            )
        ''')

        # 인덱스 생성
        cursor.execute(f'''
            CREATE INDEX IF NOT EXISTS idx_{symbol.lower()}_reg_date
            ON {table_name}(reg_date DESC)
        ''')

        self.conn.commit()

    def bulk_insert_candles(self, symbol, candles):
        """
        캔들 데이터 일괄 삽입 (초기 데이터 로딩용)

        Args:
            symbol: 'BTC', 'XRP', 'ETH'
            candles: 캔들 데이터 리스트
                [
                    {
                        'trade_price': float,
                        'high_price': float,
                        'low_price': float,
                        'candle_date_time_kst': 'YYYY-MM-DD HH:MM:SS'
                    },
                    ...
                ]
        """
        table_name = f"bp_price_{symbol.lower()}"
        cursor = self.conn.cursor()

        for candle in candles:
            cursor.execute(f'''
                INSERT INTO {table_name} (trade_price, high_price, low_price, reg_date)
                VALUES (?, ?, ?, ?)
            ''', (
                candle['trade_price'],
                candle['high_price'],
                candle['low_price'],
                candle['candle_date_time_kst']
            ))

        self.conn.commit()

    def save_price(self, symbol, price_data):
        """
        가격 데이터 저장

        Args:
            symbol: 'BTC', 'XRP', 'ETH'
            price_data: {
                'trade_price': float,
                'high_price': float,
                'low_price': float
            }
        """
        table_name = f"bp_price_{symbol.lower()}"
        reg_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        cursor = self.conn.cursor()
        cursor.execute(f'''
            INSERT INTO {table_name} (trade_price, high_price, low_price, reg_date)
            VALUES (?, ?, ?, ?)
        ''', (
            price_data['trade_price'],
            price_data['high_price'],
            price_data['low_price'],
            reg_date
        ))
        self.conn.commit()

    def get_today_high(self, symbol):
        """
        당일 최고가 조회 (DB 기준)

        Returns:
            float: 오늘 저장된 trade_price 중 최고값
            None: 데이터 없음
        """
        table_name = f"bp_price_{symbol.lower()}"
        today = datetime.now().strftime('%Y-%m-%d')

        cursor = self.conn.cursor()
        cursor.execute(f'''
            SELECT MAX(trade_price) as max_price
            FROM {table_name}
            WHERE DATE(reg_date) = ?
        ''', (today,))

        result = cursor.fetchone()
        return result['max_price'] if result and result['max_price'] else None

    def get_today_low(self, symbol):
        """
        당일 최저가 조회 (DB 기준)

        Returns:
            float: 오늘 저장된 trade_price 중 최저값
            None: 데이터 없음
        """
        table_name = f"bp_price_{symbol.lower()}"
        today = datetime.now().strftime('%Y-%m-%d')

        cursor = self.conn.cursor()
        cursor.execute(f'''
            SELECT MIN(trade_price) as min_price
            FROM {table_name}
            WHERE DATE(reg_date) = ?
        ''', (today,))

        result = cursor.fetchone()
        return result['min_price'] if result and result['min_price'] else None

    def get_period_high(self, symbol, days):
        """
        N일 기준 최고가 조회

        Args:
            symbol: 'BTC', 'XRP', 'ETH'
            days: 5, 20, 60, 120

        Returns:
            float: N일간 trade_price 중 최고값
            None: 데이터 없음
        """
        table_name = f"bp_price_{symbol.lower()}"

        cursor = self.conn.cursor()
        cursor.execute(f'''
            SELECT MAX(trade_price) as max_price
            FROM {table_name}
            WHERE DATE(reg_date) >= DATE('now', '-{days} days')
        ''')

        result = cursor.fetchone()
        return result['max_price'] if result and result['max_price'] else None
