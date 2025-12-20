# 빗썸 가격 모니터

빗썸 거래소의 가상화폐 가격을 1분 단위로 모니터링하여, 당일 고가/저가가 갱신될 때 텔레그램으로 자동 알림을 전송하는 시스템입니다.

## 주요 기능

- 1분 단위 가격 모니터링 (Crontab 스케줄러)
- 당일 고가 갱신 시 텔레그램 알림
- 당일 저가 갱신 시 텔레그램 알림
- 5일/20일/60일/120일 기간별 최고가 정보 제공
- 종목별 독립 테이블 관리 (SQLite)
- 최초 실행 시 120일치 과거 데이터 자동 로딩

## 기술 스택

- **Language**: Python 3.x
- **Database**: SQLite3
- **API**: Bithumb Public API (v1)
- **Notification**: Telegram Bot API
- **Scheduler**: Crontab (Linux/Ubuntu)

## 시스템 요구사항

- Python 3.8 이상
- Linux/Ubuntu (Crontab 지원 환경)
- 인터넷 연결

## 설치 방법

### 1. 저장소 클론

```bash
git clone <repository-url> bithumb-price-monitor
cd bithumb-price-monitor
```

### 2. 의존성 설치

```bash
pip install -r requirements.txt
```

### 3. 환경변수 설정

`.env.sample` 파일을 `.env`로 복사하고 설정:

```bash
cp .env.sample .env
```

`.env` 파일 편집:

```env
# 텔레그램 봇 설정
TELEGRAM_BOT_TOKEN=your_telegram_bot_token_here
TELEGRAM_CHAT_ID=your_telegram_chat_id_here

# 모니터링 대상 코인 (쉼표로 구분)
MONITORED_SYMBOLS=BTC,XRP,ETH
```

**필수 환경변수:**
- `TELEGRAM_BOT_TOKEN`: 텔레그램 봇 토큰
- `TELEGRAM_CHAT_ID`: 텔레그램 채팅 ID
- `MONITORED_SYMBOLS`: 모니터링할 코인 목록 (쉼표로 구분)

### 4. 수동 실행 테스트

```bash
python main.py
```

최초 실행 시:
- 종목별 테이블 자동 생성 (`bp_price_btc`, `bp_price_xrp`, `bp_price_eth`)
- 120일치 과거 데이터 자동 로딩
- 데이터베이스 생성: `data/bithumb_price_monitor.db`

## 프로젝트 구조

```
bithumb-price-monitor/
├── main.py                           # 메인 실행 파일
├── requirements.txt                  # Python 패키지 의존성
├── README.md                         # 프로젝트 문서 (이 파일)
├── .env                              # 환경변수 설정 (gitignore)
├── .env.sample                       # 환경변수 샘플
├── .gitignore                        # Git 제외 파일 목록
├── .claude/
│   └── CLAUDE.md                     # 프로젝트 상세 문서
├── utils/
│   ├── telegram_util.py             # 텔레그램 알림 유틸리티
│   ├── logger_util.py               # 로깅 유틸리티
│   └── db_util.py                   # 데이터베이스 유틸리티
├── data/
│   └── bithumb_price_monitor.db     # SQLite 데이터베이스
└── logs/
    ├── YYYY-MM-DD_log.log           # 일별 애플리케이션 로그
    └── cron.log                     # Crontab 실행 로그
```

## 데이터베이스 스키마

각 종목(BTC, XRP, ETH)마다 별도 테이블 생성:

```sql
CREATE TABLE bp_price_btc (
    idx INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_price REAL NOT NULL,           -- 현재가
    high_price REAL NOT NULL,            -- 당일 고가
    low_price REAL NOT NULL,             -- 당일 저가
    reg_date TEXT NOT NULL               -- 등록 시간 (YYYY-MM-DD HH:MM:SS)
);

CREATE INDEX idx_btc_reg_date ON bp_price_btc(reg_date DESC);
```

## 텔레그램 알림 예시

### 🟥 당일 고가 갱신

```
🟥 당일 고가 갱신
종목코드: BTC
현재가: 131,500,000원
5일: 132,000,000원
20일: 135,000,000원
60일: 140,000,000원
120일: 145,000,000원

2025-12-20 14:23:45
```

### 🟦 당일 저가 갱신

```
🟦 당일 저가 갱신
종목코드: XRP
현재가: 2,850원
5일: 2,900원
20일: 3,000원
60일: 3,200원
120일: 3,500원

2025-12-20 14:24:12
```

## 알림 로직

### 당일 고가/저가 갱신 판단

1. DB에서 오늘 저장된 최고가/최저가 조회
2. API에서 현재가 조회
3. 현재가를 DB에 저장
4. 현재가 > 오늘 최고가 → 🟥 당일 고가 갱신 알림
5. 현재가 < 오늘 최저가 → 🟦 당일 저가 갱신 알림

### 중복 알림 방지

- 같은 가격대에서는 재알림하지 않음
- 가격이 실제로 갱신될 때만 알림 전송

**예시:**
- 13:00 - 135M (당일 고가 갱신, 알림 전송)
- 13:01 - 134M (갱신 아님, 알림 없음)
- 13:02 - 135M (갱신 아님, 알림 없음)
- 13:03 - 136M (당일 고가 갱신, 알림 전송)

## 코인 추가/제거

### 모니터링 코인 추가

`.env` 파일 수정:

```env
MONITORED_SYMBOLS=BTC,XRP,ETH,SOL,DOGE
```

## API 명세

### Bithumb Public API v1

#### 현재가 조회
```
GET https://api.bithumb.com/v1/ticker?markets=KRW-{symbol}
```

#### 일봉 캔들 조회
```
GET https://api.bithumb.com/v1/candles/days?count=120&market=KRW-{symbol}
```
