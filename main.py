import os
import sys
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import requests

from utils.logger_util import LoggerUtil
from utils.telegram_util import TelegramUtil
from utils.db_util import DatabaseUtil

# 환경변수 로드
load_dotenv()

# 경로 설정
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / 'data'
DB_PATH = DATA_DIR / 'bithumb_price_monitor.db'

# 디렉토리 생성
DATA_DIR.mkdir(parents=True, exist_ok=True)


def validate_env():
    """
    필수 환경변수 검증

    필수 환경변수:
    - TELEGRAM_BOT_TOKEN
    - TELEGRAM_CHAT_ID
    - MONITORED_SYMBOLS

    없으면 에러 발생 후 종료
    """
    required_vars = ['TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID', 'MONITORED_SYMBOLS']
    missing_vars = []

    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)

    if missing_vars:
        print(f"ERROR: 필수 환경변수가 설정되지 않았습니다: {', '.join(missing_vars)}")
        print(".env 파일에 다음 항목을 추가해주세요:")
        for var in missing_vars:
            print(f"   {var}=...")
        sys.exit(1)


def get_daily_candles(symbol, count=120, logger=None):
    """
    빗썸 일봉 캔들 데이터 조회

    Args:
        symbol: 'BTC', 'XRP', 'ETH'
        count: 조회할 캔들 개수 (기본 120일)
        logger: Logger 인스턴스

    Returns:
        list: 캔들 데이터 리스트
            [
                {
                    'trade_price': float,
                    'high_price': float,
                    'low_price': float,
                    'candle_date_time_kst': 'YYYY-MM-DD HH:MM:SS'
                },
                ...
            ]
        실패 시 None
    """
    url = f"https://api.bithumb.com/v1/candles/days"
    params = {
        'count': count,
        'market': f'KRW-{symbol}'
    }
    headers = {"accept": "application/json"}

    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        # 빗썸 API는 배열로 응답
        if not isinstance(data, list):
            if logger:
                logger.error(f"[{symbol}] 예상하지 못한 API 응답 형식: {type(data)}")
            return None

        candles = []
        for candle in data:
            candles.append({
                'trade_price': float(candle['trade_price']),
                'high_price': float(candle['high_price']),
                'low_price': float(candle['low_price']),
                'candle_date_time_kst': candle['candle_date_time_kst']
            })

        if logger:
            logger.info(f"[{symbol}] 일봉 캔들 {len(candles)}개 조회 완료")

        return candles

    except Exception as e:
        if logger:
            logger.error(f"[{symbol}] 일봉 캔들 조회 실패: {str(e)}")
        return None


def get_current_price(symbol, logger):
    """
    빗썸 API에서 현재가 정보 가져오기

    Args:
        symbol: 'BTC', 'XRP', 'ETH'
        logger: Logger 인스턴스

    Returns:
        {
            'trade_price': float,      # 현재가
            'high_price': float,       # 당일 고가
            'low_price': float        # 당일 저가
        }
        실패 시 None
    """
    url = f"https://api.bithumb.com/v1/ticker?markets=KRW-{symbol}"
    headers = {"accept": "application/json"}

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        # 빗썸 API는 배열로 응답하므로 첫 번째 요소 가져오기
        if not isinstance(data, list) or len(data) == 0:
            logger.error(f"[{symbol}] 예상하지 못한 API 응답 형식: {type(data)}")
            return None

        ticker = data[0]

        # API 응답 파싱
        return {
            'trade_price': float(ticker['trade_price']),
            'high_price': float(ticker['high_price']),
            'low_price': float(ticker['low_price'])
        }
    except Exception as e:
        logger.error(f"[{symbol}] API 호출 실패: {str(e)}")
        return None


def initialize_symbol_table(symbol, db, logger):
    """
    종목 테이블 초기화

    테이블이 없으면:
    1. 테이블 생성
    2. 120일치 일봉 캔들 데이터 조회
    3. DB에 일괄 삽입

    Args:
        symbol: 'BTC', 'XRP', 'ETH'
        db: DatabaseUtil 인스턴스
        logger: Logger 인스턴스
    """
    if not db.table_exists(symbol):
        logger.info(f"[{symbol}] 테이블이 존재하지 않습니다. 초기화를 시작합니다.")

        # 1. 테이블 생성
        db.create_table(symbol)
        logger.info(f"[{symbol}] 테이블 생성 완료: bp_price_{symbol.lower()}")

        # 2. 120일치 캔들 데이터 조회
        candles = get_daily_candles(symbol, count=120, logger=logger)

        if candles:
            # 3. DB에 일괄 삽입 (오래된 순서대로)
            candles.reverse()
            db.bulk_insert_candles(symbol, candles)
            logger.info(f"[{symbol}] 초기 데이터 {len(candles)}건 삽입 완료")
        else:
            logger.error(f"[{symbol}] 초기 데이터 로딩 실패")
    else:
        logger.info(f"[{symbol}] 테이블 존재 확인 완료")


def main():
    """메인 실행 함수"""

    # 1. 환경변수 검증
    validate_env()

    # 2. 초기화
    logger = LoggerUtil().get_logger()
    telegram = TelegramUtil()
    db = DatabaseUtil(DB_PATH)

    # 환경변수에서 모니터링 코인 가져오기
    monitored_symbols = os.getenv('MONITORED_SYMBOLS').split(',')
    monitored_symbols = [s.strip().upper() for s in monitored_symbols]

    logger.info("=== 빗썸 가격 모니터 시작 ===")
    logger.info(f"모니터링 대상: {', '.join(monitored_symbols)}")

    # 3. DB 연결
    db.connect()

    # 4. 각 종목 테이블 초기화 (없으면 생성 + 120일 데이터 로딩)
    for symbol in monitored_symbols:
        initialize_symbol_table(symbol, db, logger)

    # 5. 각 코인 처리
    for symbol in monitored_symbols:
        process_symbol(symbol, logger, telegram, db)

    # 6. 종료
    db.close()
    logger.info("=== 빗썸 가격 모니터 완료 ===")


def process_symbol(symbol, logger, telegram, db):
    """
    단일 종목 처리

    1. API에서 현재가 조회
    2. 당일 고가/저가 조회 (DB 기준, 저장 전)
    3. DB에 저장
    4. 현재가가 당일 고가/저가를 갱신했는지 확인
    5. 갱신 시 텔레그램 알림 (5일/20일/60일/120일 고가 포함)
    """

    logger.info(f"[{symbol}] 처리 시작")

    # 1. API 호출
    price_data = get_current_price(symbol, logger)
    if price_data is None:
        logger.warning(f"[{symbol}] API 호출 실패 - 건너뜀")
        return

    current_price = price_data['trade_price']
    logger.info(f"[{symbol}] 현재가: {current_price:,.0f}원")

    # 2. 당일 기존 고가/저가 조회 (저장 전)
    prev_today_high = db.get_today_high(symbol)
    prev_today_low = db.get_today_low(symbol)

    # 3. DB 저장
    db.save_price(symbol, price_data)

    # 4. 당일 고가/저가 갱신 여부 확인
    is_new_high = False
    is_new_low = False

    if prev_today_high is not None:
        # 기존 데이터가 있을 때만 비교
        if current_price > prev_today_high:
            is_new_high = True
            logger.info(f"[{symbol}] 당일 고가 갱신: {prev_today_high:,.0f} -> {current_price:,.0f}")

    if prev_today_low is not None:
        if current_price < prev_today_low:
            is_new_low = True
            logger.info(f"[{symbol}] 당일 저가 갱신: {prev_today_low:,.0f} -> {current_price:,.0f}")

    # 5. 알림 전송
    if is_new_high:
        send_alert(symbol, 'HIGH', current_price, db, telegram, logger)

    if is_new_low:
        send_alert(symbol, 'LOW', current_price, db, telegram, logger)


def send_alert(symbol, alert_type, current_price, db, telegram, logger):
    """
    텔레그램 알림 전송

    Args:
        symbol: 종목명
        alert_type: 'HIGH' 또는 'LOW'
        current_price: 현재가
        db: DatabaseUtil 인스턴스
        telegram: TelegramUtil 인스턴스
        logger: Logger 인스턴스
    """

    if alert_type == 'HIGH':
        alert_text = "🟥 당일 고가 갱신"
    else:
        alert_text = "🟦당일 저가 갱신"

    # 기간별 고가 조회
    high_5d = db.get_period_high(symbol, 5)
    high_20d = db.get_period_high(symbol, 20)
    high_60d = db.get_period_high(symbol, 60)
    high_120d = db.get_period_high(symbol, 120)

    # 기간별 최고가 포맷팅
    high_5d_str = f"{high_5d:,.0f}" if high_5d is not None else "N/A"
    high_20d_str = f"{high_20d:,.0f}" if high_20d is not None else "N/A"
    high_60d_str = f"{high_60d:,.0f}" if high_60d is not None else "N/A"
    high_120d_str = f"{high_120d:,.0f}" if high_120d is not None else "N/A"

    # 메시지 작성
    message = f"""
<b>{alert_text}</b>
<b>종목코드: {symbol}</b>
현재가: {current_price:,.0f}원
5일최고가: {high_5d_str}원
20일최고가: {high_20d_str}원
60일최고가: {high_60d_str}원
120일최고가: {high_120d_str}원

{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
""".strip()

    try:
        telegram.send_message(message)
        logger.info(f"[{symbol}] 알림 전송 완료")
    except Exception as e:
        logger.error(f"[{symbol}] 알림 전송 실패: {str(e)}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger = LoggerUtil().get_logger()
        logger.error(f"치명적 오류: {str(e)}", exc_info=True)
        sys.exit(1)
