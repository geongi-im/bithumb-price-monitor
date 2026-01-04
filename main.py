import os
import sys
import glob
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

# 로거 세팅
logger = LoggerUtil().get_logger()

# 경로 설정
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = f"{PROJECT_ROOT}/data"
DB_PATH = f"{DATA_DIR}/bithumb_price_monitor.db"

# 디렉토리 생성
os.makedirs(DATA_DIR, exist_ok=True)

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


def get_daily_candles(symbol, count=120):
    """
    빗썸 일봉 캔들 데이터 조회 (다중 호출 지원)

    Args:
        symbol: 'BTC', 'XRP', 'ETH'
        count: 조회할 캔들 개수 (200 이상도 가능, 자동 다중 호출)

    Returns:
        list: 캔들 데이터 리스트 (최신→과거 순서)
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
        실패 시 None
    """
    import time

    url = f"https://api.bithumb.com/v1/candles/days"
    headers = {"accept": "application/json"}

    all_candles = []
    remaining_count = count
    to_timestamp = None  # 첫 호출은 None (최신 데이터)

    try:
        while remaining_count > 0:
            # 이번 배치 크기 (최대 200)
            batch_size = min(remaining_count, 200)

            # 파라미터 설정
            params = {
                'count': batch_size,
                'market': f'KRW-{symbol}'
            }

            # 2차 호출부터 to 파라미터 추가
            if to_timestamp:
                params['to'] = to_timestamp

            # API 호출
            logger.info(f"[{symbol}] API 호출: count={batch_size}, to={to_timestamp or '최신'}")
            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()

            # 응답 검증
            if not isinstance(data, list):
                logger.error(f"[{symbol}] 예상하지 못한 응답: {type(data)}")
                return None

            # 더 이상 데이터 없으면 종료
            if len(data) == 0:
                logger.warning(f"[{symbol}] 과거 데이터 없음. 총 {len(all_candles)}개 수집")
                break

            # 캔들 데이터 변환
            batch_candles = []
            for candle in data:
                batch_candles.append({
                    'opening_price': float(candle['opening_price']),
                    'high_price': float(candle['high_price']),
                    'low_price': float(candle['low_price']),
                    'trade_price': float(candle['trade_price']),
                    'candle_acc_trade_volume': float(candle['candle_acc_trade_volume']),
                    'candle_date_time_kst': candle['candle_date_time_kst']
                })

            # 배치 추가
            all_candles.extend(batch_candles)

            # 다음 호출을 위한 to 파라미터 설정
            # 마지막(가장 오래된) 캔들의 timestamp 사용
            oldest_candle = data[-1]
            to_timestamp = oldest_candle['candle_date_time_kst']

            # 남은 개수 갱신
            remaining_count -= len(batch_candles)

            # API Rate Limit 대응 (0.5초 대기)
            if remaining_count > 0:
                time.sleep(0.5)
                logger.info(f"[{symbol}] 다음 배치 대기... (남은: {remaining_count}개)")

        logger.info(f"[{symbol}] 일봉 캔들 {len(all_candles)}개 조회 완료")
        return all_candles

    except Exception as e:
        logger.error(f"[{symbol}] 일봉 캔들 조회 실패: {str(e)}")
        # 부분 데이터라도 반환
        return all_candles if len(all_candles) > 0 else None


def get_latest_daily_candle(symbol):
    """
    오늘 일간 캔들 데이터 조회 (1개)

    Args:
        symbol: 'BTC', 'XRP', 'ETH'

    Returns:
        {
            'opening_price': float,
            'trade_price': float,
            'high_price': float,
            'low_price': float,
            'candle_acc_trade_volume': float,
            'candle_date_time_kst': 'YYYY-MM-DD HH:MM:SS'
        }
        실패 시 None
    """
    url = "https://api.bithumb.com/v1/candles/days"
    params = {
        'count': 1,
        'market': f'KRW-{symbol}'
    }
    headers = {"accept": "application/json"}

    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        # 빗썸 API는 배열로 응답
        if not isinstance(data, list) or len(data) == 0:
            logger.error(f"[{symbol}] 예상하지 못한 API 응답 형식: {type(data)}")
            return None

        candle = data[0]

        return {
            'opening_price': float(candle['opening_price']),
            'trade_price': float(candle['trade_price']),
            'high_price': float(candle['high_price']),
            'low_price': float(candle['low_price']),
            'candle_acc_trade_volume': float(candle['candle_acc_trade_volume']),
            'candle_date_time_kst': candle['candle_date_time_kst']
        }
    except Exception as e:
        logger.error(f"[{symbol}] 일간 캔들 조회 실패: {str(e)}")
        return None


def initialize_symbol_table(symbol, db):
    """
    종목 테이블 초기화

    테이블이 없으면:
    1. 테이블 생성
    2. N일치 일봉 캔들 데이터 조회
    3. DB에 일괄 삽입

    Args:
        symbol: 'BTC', 'XRP', 'ETH'
        db: DatabaseUtil 인스턴스
    """
    if not db.table_exists(symbol):
        logger.info(f"[{symbol}] 테이블이 존재하지 않습니다. 초기화를 시작합니다.")

        # 1. 테이블 생성
        db.create_table(symbol)
        logger.info(f"[{symbol}] 테이블 생성 완료: bp_price_{symbol.lower()}")

        # 2. 1년치(365일) 캔들 데이터 조회
        candles = get_daily_candles(symbol, count=365)

        if candles:
            # 3. DB에 일괄 삽입 (오래된 순서대로)
            candles.reverse()
            db.bulk_insert_candles(symbol, candles)
            logger.info(f"[{symbol}] 초기 데이터 {len(candles)}건 삽입 완료")
        else:
            logger.error(f"[{symbol}] 초기 데이터 로딩 실패")
    else:
        logger.info(f"[{symbol}] 테이블 존재 확인 완료")

def process_symbol(symbol, telegram, db):
    """
    단일 종목 처리 (UPSERT 방식)

    1. 일간 캔들 API 호출 (count=1) → 오늘 캔들 데이터
    2. DB에서 오늘 날짜 레코드 조회
    3. 레코드 없으면: INSERT (새로운 날짜)
    4. 레코드 있으면:
       - UPDATE 전 고가/저가 비교
       - 갱신 시 알림 전송
       - UPDATE 실행
    """
    logger.info(f"[{symbol}] 처리 시작")

    # 1. 일간 캔들 API 호출
    candle = get_latest_daily_candle(symbol)
    if candle is None:
        logger.warning(f"[{symbol}] API 호출 실패 - 건너뜀")
        return

    current_price = candle['trade_price']
    logger.info(f"[{symbol}] 현재가: {current_price:,.0f}원")

    # 2. 오늘 날짜 레코드 조회
    today_date = datetime.now().strftime('%Y-%m-%d')
    existing_record = db.get_record_by_date(symbol, today_date)

    # 3. INSERT or UPDATE
    if existing_record is None:
        # INSERT: 오늘 첫 실행
        db.insert_candle(symbol, candle)
        logger.info(f"[{symbol}] 신규 레코드 삽입 (날짜: {today_date})")
    else:
        # UPDATE: 고가/저가 갱신 체크 후 업데이트
        is_new_high = current_price > existing_record['high_price']
        is_new_low = current_price < existing_record['low_price']

        if is_new_high:
            logger.info(f"[{symbol}] 당일 고가 갱신: {existing_record['high_price']:,.0f} -> {current_price:,.0f}")
            send_alert(symbol, 'HIGH', current_price, db, telegram)

        if is_new_low:
            logger.info(f"[{symbol}] 당일 저가 갱신: {existing_record['low_price']:,.0f} -> {current_price:,.0f}")
            send_alert(symbol, 'LOW', current_price, db, telegram)

        # 레코드 업데이트
        db.update_candle(symbol, candle, today_date)
        logger.info(f"[{symbol}] 레코드 업데이트 (종가: {current_price:,.0f}원)")

def create_chart(symbol, candles):
    """
    차트 이미지 생성 (yy-mm-dd 포맷, 한국어 지원, 상단 밀착 타이틀, 기간별 이동평균선 추가)
    Args:
        symbol: 종목코드
        candles: 캔들 데이터 리스트 (최소 120개 이상 권장 for MA)
    """
    try:
        # 이전 차트 파일 정리 (해당 symbol의 png 파일 삭제)
        data_dir = f"{PROJECT_ROOT}/data"
        for old_chart in glob.glob(f"{data_dir}/chart_{symbol}_*.png"):
            try:
                os.remove(old_chart)
                logger.info(f"[{symbol}] 이전 차트 파일 삭제: {os.path.basename(old_chart)}")
            except Exception as e:
                logger.warning(f"[{symbol}] 이전 차트 파일 삭제 실패: {os.path.basename(old_chart)}, {str(e)}")
        
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

        # 이동평균선 계산 (5, 20, 60, 120)
        ma_colors = {
            '5일': '#2ca02c',   # Green
            '20일': '#d62728',  # Red
            '60일': '#ff7f0e',  # Orange
            '120일': '#9467bd'  # Purple
        }
        
        df['MA5'] = df['Close'].rolling(window=5).mean()
        df['MA20'] = df['Close'].rolling(window=20).mean()
        df['MA60'] = df['Close'].rolling(window=60).mean()
        df['MA120'] = df['Close'].rolling(window=120).mean()

        # 최근 120일 데이터만 슬라이싱 (계산 후 자르기)
        df = df.iloc[-120:]

        # 폰트 및 스타일 설정 (Noto Sans KR 사용)
        FONT_PATH = f"{PROJECT_ROOT}/fonts/NotoSansKR-Regular.ttf"
        
        # 폰트를 matplotlib 폰트 매니저에 등록
        fm.fontManager.addfont(FONT_PATH)
        font_prop = fm.FontProperties(fname=FONT_PATH)
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

        data_dir = f"{PROJECT_ROOT}/data"
        save_path = f"{data_dir}/chart_{symbol}_{datetime.now().strftime('%y%m%d_%H%M%S')}.png"

        # 추가 플롯 (이동평균선)
        ap = [
            mpf.make_addplot(df['MA5'], color=ma_colors['5일'], width=1.0),
            mpf.make_addplot(df['MA20'], color=ma_colors['20일'], width=1.0),
            mpf.make_addplot(df['MA60'], color=ma_colors['60일'], width=1.0),
            mpf.make_addplot(df['MA120'], color=ma_colors['120일'], width=1.0)
        ]

        # 차트 그리기
        fig, axes = mpf.plot(
            df,
            type='candle',
            volume=True,
            style=s,
            addplot=ap,
            ylabel='', # Y축 라벨 제거
            ylabel_lower='', # 거래량 라벨 제거
            datetime_format='%y-%m-%d',
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

        # 현재 일시 표시 (우측 상단)
        current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        axes[0].text(0.99, 1.01, current_time_str, transform=axes[0].transAxes, 
                     ha='right', va='bottom', fontsize=8)

        # 하단 범례 추가 (fig.legend 사용)
        from matplotlib.lines import Line2D
        legend_elements = [
            Line2D([0], [0], color=color, lw=2, label=label) 
            for label, color in ma_colors.items()
        ]
        
        # 범례를 하단 중앙에 배치
        fig.legend(handles=legend_elements, loc='lower center', 
                   bbox_to_anchor=(0.55, 0.08), ncol=4, frameon=False, prop={'size': 9, 'weight': 'bold'})

        # 여백 조정: 하단 여백을 충분히 주어 범례 공간 확보
        fig.subplots_adjust(top=0.90, bottom=0.15, left=0.08, right=0.92)

        # 이미지 저장
        fig.savefig(save_path, dpi=100, bbox_inches='tight', pad_inches=0.05)
        plt.close(fig)

        logger.info(f"[{symbol}] 차트 생성 완료: {os.path.basename(save_path)}")
        return save_path
    except Exception as e:
        logger.error(f"[{symbol}] 차트 생성 실패: {str(e)}")
        raise

def format_percent_diff(current_price, period_price):
    """
    현재가 대비 기간별 가격의 퍼센트 차이 계산
    
    Args:
        current_price: 현재가
        period_price: 기간별 고가/저가 (None 가능)
    
    Returns:
        str: " (+2.50%)" 또는 " (-3.75%)" 또는 ""
    """
    if period_price is None or period_price == 0:
        return ""
    
    diff_amount = current_price - period_price
    
    # 차이가 0이면 표시 안 함
    if diff_amount == 0:
        return ""
    
    percent = (diff_amount / period_price) * 100
    
    if diff_amount > 0:
        return f" (+{percent:.2f}%)"
    else:
        return f" ({percent:.2f}%)"

def send_alert(symbol, alert_type, current_price, db, telegram):
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

    # 퍼센트 차이 계산
    diff_5d = format_percent_diff(current_price, price_5d)
    diff_20d = format_percent_diff(current_price, price_20d)
    diff_60d = format_percent_diff(current_price, price_60d)
    diff_120d = format_percent_diff(current_price, price_120d)

    # 메시지 작성
    message = f"""
<b>{alert_text}</b>
<b>종목코드: {symbol}</b>
현재가: {current_price:,.0f}원
5일{period_label}: {price_5d_str}원{diff_5d}
20일{period_label}: {price_20d_str}원{diff_20d}
60일{period_label}: {price_60d_str}원{diff_60d}
120일{period_label}: {price_120d_str}원{diff_120d}

{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
""".strip()

    try:
        # 차트 생성 (DB에서 최근 365일 데이터 조회 - 120일 이동평균선 계산용)
        candles = db.get_period_candles(symbol, days=365)
        chart_path = None
        if candles:
            chart_path = create_chart(symbol, candles)

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


def main():
    """메인 실행 함수"""

    # 1. 환경변수 검증
    validate_env()

    # 2. 초기화
    telegram = TelegramUtil()
    db = DatabaseUtil(DB_PATH)

    # 환경변수에서 모니터링 코인 가져오기
    monitored_symbols = os.getenv('MONITORED_SYMBOLS').split(',')
    monitored_symbols = [s.strip().upper() for s in monitored_symbols]

    logger.info("=== 빗썸 가격 모니터 시작 ===")
    logger.info(f"모니터링 대상: {', '.join(monitored_symbols)}")

    # 3. DB 연결
    db.connect()

    # 4. 각 종목 테이블 초기화 (없으면 생성 + N일 데이터 로딩)
    for symbol in monitored_symbols:
        initialize_symbol_table(symbol, db)

    # 5. 각 코인 처리
    for symbol in monitored_symbols:
        process_symbol(symbol, telegram, db)

    # 6. 종료
    db.close()
    logger.info("=== 빗썸 가격 모니터 완료 ===")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"치명적 오류: {str(e)}", exc_info=True)
        
        try:
            telegram = TelegramUtil()
            error_msg = f"🚨 치명적 오류 발생\n\n{str(e)}\n\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            telegram.send_test_message(error_msg)
        except:
            pass
        
        sys.exit(1)
