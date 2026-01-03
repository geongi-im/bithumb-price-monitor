import sqlite3


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
                open_price REAL NOT NULL,
                close_price REAL NOT NULL,
                high_price REAL NOT NULL,
                low_price REAL NOT NULL,
                volume REAL NOT NULL,
                reg_date TEXT NOT NULL,
                UNIQUE(reg_date)
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
                        'opening_price': float,
                        'trade_price': float,
                        'high_price': float,
                        'low_price': float,
                        'candle_acc_trade_volume': float,
                        'candle_date_time_kst': 'YYYY-MM-DD HH:MM:SS'
                    },
                    ...
                ]
        """
        table_name = f"bp_price_{symbol.lower()}"
        cursor = self.conn.cursor()

        for candle in candles:
            # reg_date를 날짜만 추출 (YYYY-MM-DD)
            date_only = candle['candle_date_time_kst'][:10]

            cursor.execute(f'''
                INSERT INTO {table_name}
                (open_price, close_price, high_price, low_price, volume, reg_date)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                candle['opening_price'],
                candle['trade_price'],
                candle['high_price'],
                candle['low_price'],
                candle['candle_acc_trade_volume'],
                date_only
            ))

        self.conn.commit()

    def get_record_by_date(self, symbol, date):
        """
        특정 날짜의 레코드 조회

        Args:
            symbol: 'BTC', 'XRP', 'ETH'
            date: 'YYYY-MM-DD'

        Returns:
            {
                'idx': int,
                'open_price': float,
                'close_price': float,
                'high_price': float,
                'low_price': float,
                'volume': float,
                'reg_date': str
            }
            없으면 None
        """
        table_name = f"bp_price_{symbol.lower()}"
        cursor = self.conn.cursor()

        cursor.execute(f'''
            SELECT * FROM {table_name}
            WHERE reg_date = ?
        ''', (date,))

        result = cursor.fetchone()
        if result:
            return dict(result)
        return None

    def insert_candle(self, symbol, candle):
        """
        새로운 날짜 레코드 삽입

        Args:
            symbol: 'BTC', 'XRP', 'ETH'
            candle: 일간 캔들 데이터
        """
        table_name = f"bp_price_{symbol.lower()}"
        cursor = self.conn.cursor()

        date_only = candle['candle_date_time_kst'][:10]

        cursor.execute(f'''
            INSERT INTO {table_name}
            (open_price, close_price, high_price, low_price, volume, reg_date)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            candle['opening_price'],
            candle['trade_price'],
            candle['high_price'],
            candle['low_price'],
            candle['candle_acc_trade_volume'],
            date_only
        ))

        self.conn.commit()

    def update_candle(self, symbol, candle, date):
        """
        기존 레코드 업데이트

        Args:
            symbol: 'BTC', 'XRP', 'ETH'
            candle: 일간 캔들 데이터
            date: 'YYYY-MM-DD'
        """
        table_name = f"bp_price_{symbol.lower()}"
        cursor = self.conn.cursor()

        cursor.execute(f'''
            UPDATE {table_name}
            SET close_price = ?,
                high_price = ?,
                low_price = ?,
                volume = ?
            WHERE reg_date = ?
        ''', (
            candle['trade_price'],
            candle['high_price'],
            candle['low_price'],
            candle['candle_acc_trade_volume'],
            date
        ))

        self.conn.commit()

    def get_period_high(self, symbol, days):
        """
        N일 기준 최고가 조회

        Args:
            symbol: 'BTC', 'XRP', 'ETH'
            days: 5, 20, 60, 120

        Returns:
            float: N일간 high_price 중 최고값
            None: 데이터 없음
        """
        table_name = f"bp_price_{symbol.lower()}"

        cursor = self.conn.cursor()
        cursor.execute(f'''
            SELECT MAX(high_price) as max_price
            FROM {table_name}
            WHERE DATE(reg_date) >= DATE('now', '-{days} days')
        ''')

        result = cursor.fetchone()
        return result['max_price'] if result and result['max_price'] else None

    def get_period_low(self, symbol, days):
        """
        N일 기준 최저가 조회

        Args:
            symbol: 'BTC', 'XRP', 'ETH'
            days: 5, 20, 60, 120

        Returns:
            float: N일간 low_price 중 최저값
            None: 데이터 없음
        """
        table_name = f"bp_price_{symbol.lower()}"

        cursor = self.conn.cursor()
        cursor.execute(f'''
            SELECT MIN(low_price) as min_price
            FROM {table_name}
            WHERE DATE(reg_date) >= DATE('now', '-{days} days')
        ''')

        result = cursor.fetchone()
        return result['min_price'] if result and result['min_price'] else None

    def get_period_candles(self, symbol, days):
        """
        N일 기간의 캔들 데이터 조회 (차트 생성 및 이동평균 계산용)

        Args:
            symbol: 'BTC', 'XRP', 'ETH'
            days: 조회할 일수 (예: 300)

        Returns:
            list: 캔들 데이터 리스트 (오래된 순서)
                [
                    {
                        'opening_price': float,
                        'trade_price': float,
                        'high_price': float,
                        'low_price': float,
                        'candle_acc_trade_volume': float,
                        'candle_date_time_kst': 'YYYY-MM-DD 00:00:00'
                    },
                    ...
                ]
            빈 리스트: 데이터 없음
        """
        table_name = f"bp_price_{symbol.lower()}"
        cursor = self.conn.cursor()

        cursor.execute(f'''
            SELECT open_price, close_price, high_price, low_price, volume, reg_date
            FROM {table_name}
            WHERE DATE(reg_date) >= DATE('now', '-{days} days')
            ORDER BY reg_date ASC
        ''')

        results = cursor.fetchall()

        candles = []
        for row in results:
            candles.append({
                'opening_price': row['open_price'],
                'trade_price': row['close_price'],  # close_price를 trade_price로 매핑
                'high_price': row['high_price'],
                'low_price': row['low_price'],
                'candle_acc_trade_volume': row['volume'],
                'candle_date_time_kst': f"{row['reg_date']} 00:00:00"  # 날짜 형식 맞추기
            })

        return candles
