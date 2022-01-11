import pandas as pd
from pykrx import stock
import requests
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import numpy as np
import warnings
warnings.filterwarnings( 'ignore' )
from io import BytesIO


def get_daily_price(ticker, start_date, end_date) :
    df_prc = pd.DataFrame()
    for i in range(0, len(ticker)):
        try:
            df_prc_temp = pd.DataFrame(stock.get_market_ohlcv_by_date(start_date, end_date, ticker.iloc[i], adjusted=True)['종가'])
            df_prc_temp.columns = [ticker.iloc[i].종목명]
            df_prc = pd.concat([df_prc, df_prc_temp], axis=1)
        except:
            pass
    df_prc = df_prc.reset_index(drop = False)
    return df_prc


def get_bdate_info(start_date, end_date) :
    date = pd.DataFrame(stock.get_previous_business_days(fromdate=start_date, todate=end_date)).rename(columns={0:'일자'})
    prevbdate = date.shift(1).rename(columns={'일자':'전영업일자'})
    date = pd.concat([date,prevbdate],axis =1).dropna()
    return date

def get_prc_adj (start_date):
    query_str_parms = {
    'locale': 'ko_KR',
    'mktId': 'ALL',
    'strtDd': start_date,
    'endDd': start_date,
    'adjStkPrc_check': 'Y',
    'adjStkPrc': '2',
    'share': '1',
    'money': '1',
    'csvxls_isNo': 'false',
    'name': 'fileDown',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT01602'
        }
    headers = {
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0'
        }
    r = requests.get('http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd', query_str_parms, headers=headers)
    form_data = {
        'code': r.content
        }
    r = requests.post('http://data.krx.co.kr/comm/fileDn/download_excel/download.cmd', form_data, headers=headers)
    df = pd.read_excel(BytesIO(r.content))
    for i in range(0, len(df.종목코드)):
        df.종목코드.iloc[i] = str(df.종목코드[i]).zfill(6)
    df = df[['종목코드','종료일 종가']].rename(columns={'종목코드':'티커','종료일 종가':datetime.strftime(datetime.strptime(start_date, "%Y%m%d"),"%Y-%m-%d")})
    return df


def get_daily_prc_adj(start_date, end_date):
    bdate = get_bdate_info(start_date, end_date)
    df = get_prc_adj(datetime.strftime(bdate.iloc[0].일자, "%Y%m%d"))
    for i in range(1,len(bdate)):
        df_temp = get_prc_adj(datetime.strftime(bdate.iloc[i].일자, "%Y%m%d"))
        df = pd.merge(df, df_temp, on ='티커', how = 'outer')
    df_T = df.T
    df_T.columns = list(df['티커'])
    return df_T

def get_stock_num(start_date,end_date) :
    bdate = get_bdate_info(start_date, end_date)
    df = stock.get_market_cap_by_ticker(bdate.iloc[0].일자).reset_index()[['티커','상장주식수']].rename(columns={'상장주식수':bdate.iloc[0].일자})
    for i in range(1,len(bdate)):
        df_temp = stock.get_market_cap_by_ticker(bdate.iloc[i].일자).reset_index()[['티커','상장주식수']].rename(columns={'상장주식수':bdate.iloc[i].일자})
        df = pd.merge(df, df_temp, on ='티커', how = 'outer')
    df_T = df.T
    df_T.columns = list(df['티커'])
    return df_T


def get_daily_siga(ticker, start_date, end_date) :
    df_siga = pd.DataFrame()
    for i in range(0,len(ticker)):
        try :
            df_siga_temp = pd.DataFrame(stock.get_market_cap_by_date(start_date, end_date, ticker.iloc[i].티커)['시가총액'])
            df_siga_temp.columns = [ticker.iloc[i].종목명]
            df_siga = pd.concat([df_siga,df_siga_temp], axis= 1)
        except : pass
    df_siga = df_siga.reset_index(drop = False)
    return df_siga


def get_bdate_info(start_date, end_date) :
    date = pd.DataFrame(stock.get_previous_business_days(fromdate=start_date, todate=end_date)).rename(columns={0:'일자'})
    prevbdate = date.shift(1).rename(columns={'일자':'전영업일자'})
    date = pd.concat([date,prevbdate],axis =1).dropna()
    return date

def get_ticker_prc(ticker,krcode,start_date,end_date):
    query_str_parms = {
    'tboxisuCd_finder_stkisu0_1': ticker,
    'isuCd': krcode,
    'isuCd2': krcode,
    'codeNmisuCd_finder_stkisu0_1': '',
    'param1isuCd_finder_stkisu0_1': 'ALL',
    'strtDd': start_date,
    'endDd': end_date,
    'share': '1',
    'money': '1',
    'csvxls_isNo': 'false',
    'name': 'fileDown',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT01701'
        }
    headers = {
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0'
        }
    r = requests.get('http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd', query_str_parms, headers=headers)
    form_data = {
        'code': r.content
        }
    r = requests.post('http://data.krx.co.kr/comm/fileDn/download_excel/download.cmd', form_data, headers=headers)
    df = pd.read_excel(BytesIO(r.content))
    for i in range(0,len(df.일자)) :
        df.일자.iloc[i] = datetime.strftime(datetime.strptime(df['일자'][i], "%Y/%m/%d"),"%Y%m%d")
    return df

def get_sector(stddate):
    sector = {1010: '에너지',
              1510: '소재',
              2010: '자본재',
              2020: '상업서비스와공급품',
              2030: '운송',
              2510: '자동차와부품',
              2520: '내구소비재와의류',
              2530: '호텔,레스토랑,레저 등',
              2550: '소매(유통)',
              2560: '교육서비스',
              3010: '식품과기본식료품소매',
              3020: '식품,음료,담배',
              3030: '가정용품과개인용품',
              3510: '건강관리장비와서비스',
              3520: '제약과생물공학',
              4010: '은행',
              4020: '증권',
              4030: '다각화된금융',
              4040: '보험',
              4050: '부동산',
              4510: '소프트웨어와서비스',
              4520: '기술하드웨어와장비',
              4530: '반도체와반도체장비',
              4535: '전자와 전기제품',
              4540: '디스플레이',
              5010: '전기통신서비스',
              5020: '미디어와엔터테인먼트',
              5510: '유틸리티'}
    df = pd.DataFrame(columns=['티커', '종목명', '섹터', '세부섹터', 'mktval', 'wgt'])
    for i, sec_code in enumerate(sector.keys()):
        response = requests.get('http://www.wiseindex.com/Index/GetIndexComponets?ceil_yn=0&''dt=' + stddate + '&sec_cd=G' + str(sec_code))
        if (response.status_code == 200):
            json_list = response.json()
            for json in json_list['list']:
                티커 = json['CMP_CD']
                종목명 = json['CMP_KOR']
                섹터 = json['SEC_NM_KOR']
                세부섹터 = json['IDX_NM_KOR'][5:]
                mktval = json['MKT_VAL']
                wgt = json['WGT']
                df = df.append(
                    {'티커': 티커, '종목명': 종목명, '섹터': 섹터, '세부섹터': 세부섹터, 'mktval': mktval,'wgt': wgt}, ignore_index=True)
    return df

def get_ticker_rtn(stddate) :
    sector = get_sector(stddate)
    date = [stddate
        , stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(stddate, "%Y%m%d") - timedelta(days=8),"%Y%m%d"))
        , stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(stddate, "%Y%m%d") - relativedelta(months=1),"%Y%m%d"))
        , stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(stddate, "%Y%m%d") - relativedelta(months=3),"%Y%m%d"))
        , stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(stddate, "%Y%m%d") - relativedelta(months=6),"%Y%m%d"))
        , stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(stddate, "%Y%m%d") - relativedelta(years=1),"%Y%m%d"))]
    ticker = stock.get_market_cap_by_ticker(date[0]).reset_index()
    ticker_w1 = stock.get_market_cap_by_ticker(date[1]).reset_index()[['티커','종가']].rename(columns={'종가':'w1_종가'})
    ticker_m1 = stock.get_market_cap_by_ticker(date[2]).reset_index()[['티커','종가']].rename(columns={'종가':'m1_종가'})
    ticker_m3 = stock.get_market_cap_by_ticker(date[3]).reset_index()[['티커','종가']].rename(columns={'종가':'m3_종가'})
    ticker_m6 = stock.get_market_cap_by_ticker(date[4]).reset_index()[['티커','종가']].rename(columns={'종가':'m6_종가'})
    ticker_y1 = stock.get_market_cap_by_ticker(date[5]).reset_index()[['티커','종가']].rename(columns={'종가':'y1_종가'})
    ticker = pd.merge(ticker, ticker_w1, how= 'outer', on = '티커')
    ticker = pd.merge(ticker, ticker_m1, how= 'outer', on = '티커')
    ticker = pd.merge(ticker, ticker_m3, how= 'outer', on = '티커')
    ticker = pd.merge(ticker, ticker_m6, how= 'outer', on = '티커')
    ticker = pd.merge(ticker, ticker_y1, how= 'outer', on = '티커').dropna()
    ticker['1W'] = ticker['종가'] / ticker['w1_종가'] - 1
    ticker['1M'] = ticker['종가'] / ticker['m1_종가'] - 1
    ticker['3M'] = ticker['종가'] / ticker['m3_종가'] - 1
    ticker['6M'] = ticker['종가'] / ticker['m6_종가'] - 1
    ticker['1Y'] = ticker['종가'] / ticker['y1_종가'] - 1
    ticker = ticker[['티커', '종가','시가총액','1W','1M','3M','6M','1Y']]
    ticker_rtn = pd.merge(sector[['티커','종목명','섹터','세부섹터']], ticker, how = 'inner', on ='code').sort_values(by= '1W', ascending = False)
    ticker_rtn = ticker_rtn[['티커','종목명','섹터','세부섹터','종가','시가총액','1W','1M','3M','6M','1Y']]
    return ticker_rtn
