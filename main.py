import os
import sys
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import requests
import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.font_manager as fm
import numpy as np

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
                'opening_price': float(candle['opening_price']),
                'high_price': float(candle['high_price']),
                'low_price': float(candle['low_price']),
                'trade_price': float(candle['trade_price']),
                'candle_acc_trade_volume': float(candle['candle_acc_trade_volume']),
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


def create_chart(symbol, candles, logger, hlines_data):
    """
    차트 이미지 생성 (yy-mm-dd 포맷, 한국어 지원, 상단 밀착 타이틀, 기간별 라벨 추가)
    Args:
        hlines_data: [(price, label), ...] 필수 파라미터
    """
    try:
        # 이전 차트 파일 정리 (해당 symbol의 png 파일 삭제)
        DATA_DIR = Path('data')
        for old_chart in DATA_DIR.glob(f"chart_{symbol}_*.png"):
            try:
                os.remove(old_chart)
                logger.info(f"[{symbol}] 이전 차트 파일 삭제: {old_chart.name}")
            except Exception as e:
                logger.warning(f"[{symbol}] 이전 차트 파일 삭제 실패: {old_chart.name}, {str(e)}")
        
        # 데이터프레임 변환
        df = pd.DataFrame(candles)
        
        # 컬럼명 매핑 (빗썸 -> mplfinance)
        mapping = {
            'candle_date_time_kst': 'Date',
            'opening_price': 'Open',
            'high_price': 'High',
            'low_price': 'Low',
            'trade_price': 'Close',
            'candle_acc_trade_volume': 'Volume'
        }
        df = df.rename(columns={k: v for k, v in mapping.items() if k in df.columns})
        
        # 인덱스 설정
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        df.sort_index(inplace=True)

        # 폰트 및 스타일 설정 (Noto Sans KR 사용)
        FONT_PATH = PROJECT_ROOT / 'fonts' / 'NotoSansKR-Regular.ttf'
        
        # 폰트를 matplotlib 폰트 매니저에 등록
        fm.fontManager.addfont(str(FONT_PATH))
        font_prop = fm.FontProperties(fname=str(FONT_PATH))
        font_name = font_prop.get_name()
        
        plt.rcParams['font.family'] = font_name
        plt.rcParams['axes.unicode_minus'] = False
        
        # 차트 스타일 설정
        mc = mpf.make_marketcolors(up='red', down='blue', inherit=True)
        s = mpf.make_mpf_style(
            marketcolors=mc, 
            gridstyle='--', 
            y_on_right=True,
            rc={'font.family': font_name, 'axes.unicode_minus': False}
        )

        DATA_DIR = Path('data')
        save_path = DATA_DIR / f"chart_{symbol}_{datetime.now().strftime('%H%M%S')}.png"

        # 차트 그리기
        hlines_values = [h[0] for h in hlines_data]
        
        fig, axes = mpf.plot(
            df,
            type='candle',
            volume=True,
            style=s,
            ylabel='', # Y축 라벨 제거
            ylabel_lower='', # 거래량 라벨 제거
            datetime_format='%y-%m-%d',
            hlines=dict(hlines=hlines_values, colors=['#FFC300','#FF5733','#C70039','#900C3F'], linestyle='--', linewidths=1.0) if hlines_values else None,
            returnfig=True,
            figratio=(10, 7)
        )
        
        plt.rcParams['axes.formatter.useoffset'] = False
        
        axes[0].set_title(f"{symbol}/KRW 차트", fontsize=10, fontweight='bold', pad=8)
        
        def conditional_formatter(x, p):
            if abs(x) >= 10000:
                return f'{int(x/1000):,}K'
            return f'{int(x):,}'

        for ax in fig.get_axes():
            # 10^6 제거
            ax.yaxis.get_offset_text().set_visible(False)
            ax.yaxis.get_offset_text().set_text("")
            ax.xaxis.get_offset_text().set_visible(False)
            ax.xaxis.get_offset_text().set_text("")
            ax.yaxis.set_major_formatter(mticker.FuncFormatter(conditional_formatter))

        from matplotlib.text import Text
        for obj in fig.findobj(Text):
            text = obj.get_text()
            if text and '10' in text and ('^' in text or '×' in text or 'e' in text):
                obj.set_visible(False)

        # X축 레이아웃 설정
        if len(axes) > 0:
            for ax in fig.get_axes():
                ax.set_xlim(-0.5, len(df)-0.5)
                
                total_days = len(df)
                num_ticks = 5
                tick_indices = [int(i) for i in np.linspace(0, total_days - 1, num_ticks)]
                
                ax.set_xticks(tick_indices)
                ax.set_xticklabels([df.index[i].strftime('%y-%m-%d') for i in tick_indices])
                
                for label in ax.get_xticklabels():
                    label.set_fontsize(7.5)
                    label.set_rotation(0)
                    label.set_horizontalalignment('center')
                ax.tick_params(axis='x', pad=5)

        # 가로선 라벨 추가 (우측 끝)
        if hlines_data:
            # mpf.plot(returnfig=True)에서 axes[0]는 메인 차트 영역임
            main_ax = axes[0]
            for val, label in hlines_data:
                main_ax.text(len(df)-0.5, val, f' {label}', va='center', ha='left', fontsize=8, color='#C70039', fontweight='bold', clip_on=False)

        # 여백 조정: 전체 이미지 상단 여백을 줄이고 타이틀과 차트를 밀착
        fig.subplots_adjust(top=0.93, bottom=0.10, left=0.08, right=0.92)

        # 이미지 저장
        fig.savefig(str(save_path), dpi=100, bbox_inches='tight', pad_inches=0.05)
        plt.close(fig)

        logger.info(f"[{symbol}] 차트 생성 완료: {save_path.name}")
        return str(save_path)
    except Exception as e:
        logger.error(f"[{symbol}] 차트 생성 실패: {str(e)}")
        raise


def send_alert(symbol, alert_type, current_price, db, telegram, logger):
    """
    텔레그램 알림 전송 (텍스트 + 차트)
    """

    if alert_type == 'HIGH':
        alert_text = "🟥 당일 고가 갱신"
        price_5d = db.get_period_high(symbol, 5)
        price_20d = db.get_period_high(symbol, 20)
        price_60d = db.get_period_high(symbol, 60)
        price_120d = db.get_period_high(symbol, 120)
        period_label = "최고가"
    else:
        alert_text = "🟦 당일 저가 갱신"
        price_5d = db.get_period_low(symbol, 5)
        price_20d = db.get_period_low(symbol, 20)
        price_60d = db.get_period_low(symbol, 60)
        price_120d = db.get_period_low(symbol, 120)
        period_label = "최저가"

    # 기간별 가격 포맷팅
    price_5d_str = f"{price_5d:,.0f}" if price_5d is not None else "N/A"
    price_20d_str = f"{price_20d:,.0f}" if price_20d is not None else "N/A"
    price_60d_str = f"{price_60d:,.0f}" if price_60d is not None else "N/A"
    price_120d_str = f"{price_120d:,.0f}" if price_120d is not None else "N/A"

    # 메시지 작성
    message = f"""
<b>{alert_text}</b>
<b>종목코드: {symbol}</b>
현재가: {current_price:,.0f}원
5일{period_label}: {price_5d_str}원
20일{period_label}: {price_20d_str}원
60일{period_label}: {price_60d_str}원
120일{period_label}: {price_120d_str}원

{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
""".strip()

    try:
        # 차트 생성 (최근 120일 데이터 기준)
        candles = get_daily_candles(symbol, count=120, logger=logger)
        chart_path = None
        if candles:
            candles.reverse() # 오래된 순으로
            
            # 수평선 데이터 준비 (기간별 고가/저가)
            hlines_data = []
            if price_5d: hlines_data.append((price_5d, "5일"))
            if price_20d: hlines_data.append((price_20d, "20일"))
            if price_60d: hlines_data.append((price_60d, "60일"))
            if price_120d: hlines_data.append((price_120d, "120일"))
            
            chart_path = create_chart(symbol, candles, logger, hlines_data=hlines_data)

            if chart_path:
                telegram.send_photo(chart_path, caption=message)
        else:
            telegram.send_message(message)
            
        logger.info(f"[{symbol}] 알림 전송 완료")
    except Exception as e:
        error_msg = f"⚠️ [{symbol}] 알림 전송 중 오류 발생: {str(e)}"
        logger.error(error_msg)
        try:
            telegram.send_test_message(error_msg)
        except:
            pass


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger = LoggerUtil().get_logger()
        logger.error(f"치명적 오류: {str(e)}", exc_info=True)
        
        # 테스트 채널로 오류 메시지 전송
        try:
            telegram = TelegramUtil()
            error_msg = f"🚨 치명적 오류 발생\n\n{str(e)}\n\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            telegram.send_test_message(error_msg)
        except:
            pass
        
        sys.exit(1)
