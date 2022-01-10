import pandas as pd
from pykrx import stock
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
import numpy as np
import warnings
warnings.filterwarnings( 'ignore' )
from io import BytesIO
import FinanceDataReader as fdr


def get_daily_universe(universe) :
    rebal_date = list(universe.날짜.unique())
    df = pd.DataFrame()
    for s in range(1, len(rebal_date)):
        bdate = stock.get_previous_business_days(
            fromdate=datetime.strftime(pd.to_datetime(rebal_date[s - 1]), "%Y%m%d"),
            todate=datetime.strftime(pd.to_datetime(rebal_date[s]), "%Y%m%d"))
        pf_info = universe[universe.날짜 == rebal_date[s - 1]]
        df_pf = pd.DataFrame()
        for i in range(1, len(bdate)):
            date_list = pd.DataFrame(np.repeat(bdate[i], len(pf_info))).rename(columns={0: '일자'})
            ohlcv = stock.get_market_ohlcv_by_ticker(datetime.strftime(bdate[i], "%Y%m%d"), market='ALL').reset_index()[
                ['티커', '시가총액', '등락률']]
            df_pf_temp = pd.concat([date_list, pf_info[['티커', '종목명']].reset_index(drop=True)], axis=1)
            df_pf_temp = pd.merge(df_pf_temp, ohlcv, on='티커', how='inner')
            df_pf_temp = df_pf_temp[['일자', '티커', '종목명', '시가총액', '등락률']]
            df_pf = pd.concat([df_pf, df_pf_temp], axis=0).reset_index(drop=True)
        df = pd.concat([df, df_pf], axis=0).reset_index(drop=True)
    return df


def get_etf_pdf(stddate):
    query_str_parms = {
        'tboxisuCd_finder_secuprodisu1_0': '385720/TIMEFOLIO Kstock액티브',
        'isuCd': 'KR7385720008',
        'isuCd2': '385720',
        'codeNmisuCd_finder_secuprodisu1_0': 'TIMEFOLIO Kstock액티브',
        'param1isuCd_finder_secuprodisu1_0': '',
        'trdDd': stddate,
        'share': '1',
        'money': '1',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT05001'
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
    df['날짜'] = datetime.strptime(stddate, "%Y%m%d")
    for i in range(0,len(df.종목코드)) :
        df.종목코드.iloc[i] = str(df.종목코드[i]).zfill(6)
    df = df[['날짜','종목코드','구성종목명','주식수(계약수)','시가총액']].rename(columns={'종목코드':'티커','구성종목명':'종목명','주식수(계약수)':'수량'})
    return df

def get_etf_prc(stddate):
    query_str_parms = {
        'trdDd': stddate,
        'share': '1',
        'money': '1',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT04301'
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
    df['날짜'] = datetime.strptime(stddate, "%Y%m%d")
    df['전일종가'] = df['종가'] - df['대비']
    for i in range(0,len(df.종목코드)) :
        df.종목코드.iloc[i] = str(df.종목코드[i]).zfill(6)
    df = df[['날짜','종목코드','종목명','종가','전일종가','대비','등락률','시가총액']].rename(columns={'종목코드':'티커','시가총액':'상장시가총액'})
    return df

def get_kospi_prc(stddate):
    query_str_parms = {
        'tboxindIdx_finder_equidx0_0': '코스피',
        'indIdx': '1',
        'indIdx2': '001',
        'codeNmindIdx_finder_equidx0_0': '코스피',
        'param1indIdx_finder_equidx0_0': '',
        'trdDd': stddate,
        'money': '3',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT00601'
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
    df['날짜'] = datetime.strptime(stddate, "%Y%m%d")
    for i in range(0,len(df.종목코드)) :
        df.종목코드.iloc[i] = str(df.종목코드[i]).zfill(6)
    df = df[['날짜','종목코드','종목명','종가','대비','등락률','상장시가총액']].rename(columns={'종목코드':'티커'})
    return df

def get_kosdaq_prc(stddate):
    query_str_parms = {
        'tboxindIdx_finder_equidx0_0': '코스닥',
        'indIdx': '2',
        'indIdx2': '001',
        'codeNmindIdx_finder_equidx0_0': '코스닥',
        'param1indIdx_finder_equidx0_0': '',
        'trdDd': stddate,
        'money': '3',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT00601'
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
    df['날짜'] = datetime.strptime(stddate, "%Y%m%d")
    for i in range(0,len(df.종목코드)) :
        df.종목코드.iloc[i] = str(df.종목코드[i]).zfill(6)
    df = df[['날짜','종목코드','종목명','종가','대비','등락률','상장시가총액']].rename(columns={'종목코드':'티커'})
    return df

bdate =stock.get_previous_business_days(fromdate="20210526", todate="20220103")

def calc_idxrtn_by_date(date):
    df_idx = pd.DataFrame()
    for i in range(0,len(date)) :
        stdmon = datetime.strftime(date.iloc[i].전영업일자, "%Y%m")
        mapmon_list = pd.read_excel('C:/Users/ysj/Desktop/bnk50_universe.xlsx', sheet_name = 'MapMonth', dtype = {'StdMonth':str,'MapMonth':str})
        mapmon = str(int(mapmon_list[mapmon_list['StdMonth'] == (stdmon)]['MapMonth']))
        bnk50_universe = pd.read_excel('C:/Users/ysj/Desktop/bnk50_universe.xlsx', sheet_name = mapmon, dtype = {'티커':str})[['종목코드','티커','종목명','섹터','세부섹터']]
        bnkcore50_universe = pd.read_excel('C:/Users/ysj/Desktop/bnkcore50_universe.xlsx', sheet_name = mapmon, dtype = {'티커':str})[['종목코드','티커','종목명','섹터','세부섹터']]
        ohlcv = stock.get_market_ohlcv_by_ticker(datetime.strftime(date.iloc[i].전영업일자, "%Y%m%d"),market = 'ALL').reset_index()[['티커','시가총액','등락률']]
        bnk50_universe = pd.merge(bnk50_universe, ohlcv, how ='inner',on ='티커')
        bnkcore50_universe = pd.merge(bnkcore50_universe, ohlcv, how ='inner',on ='티커')
        bnk50_universe['PF수익률'] = bnk50_universe['등락률'] * bnk50_universe['시가총액'] / bnk50_universe['시가총액'].sum()
        bnkcore50_universe['PF수익률'] = bnkcore50_universe['등락률'] * bnkcore50_universe['시가총액'] / bnkcore50_universe['시가총액'].sum()
        bnk50_idx = pd.DataFrame({'날짜' : date.iloc[i].일자, '지수명' : 'BNK50', '종목수' : bnk50_universe['종목코드'].count(),  '시가총액' :  bnk50_universe['시가총액'].sum(), '수익률' : bnk50_universe['PF수익률'].sum()} , index =[0])
        bnkcore50_idx = pd.DataFrame({'날짜' : date.iloc[i].일자, '지수명' : 'BNKCORE50', '종목수' : bnkcore50_universe['종목코드'].count(),  '시가총액' :  bnkcore50_universe['시가총액'].sum(), '수익률' : bnkcore50_universe['PF수익률'].sum()} , index =[0])
        df_idx_temp = pd.concat([bnk50_idx, bnkcore50_idx]).reset_index(drop=True)
        df_idx = pd.concat([df_idx,df_idx_temp])
    return df_idx

def get_etf_rtn(etf_info, stddate) :
    date = [stddate
        , stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(stddate, "%Y%m%d") - timedelta(days=8),"%Y%m%d"))
        , stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(stddate, "%Y%m%d") - relativedelta(months=1),"%Y%m%d"))
        , stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(stddate, "%Y%m%d") - relativedelta(months=3),"%Y%m%d"))
        , stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(stddate, "%Y%m%d") - relativedelta(months=6),"%Y%m%d"))
        , stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(stddate, "%Y%m%d") - relativedelta(years=1),"%Y%m%d"))]
    etf = stock.get_etf_ohlcv_by_ticker(date[0]).reset_index()
    etf_w1 = stock.get_etf_ohlcv_by_ticker(date[1]).reset_index()[['티커','종가']].rename(columns={'종가':'w1_종가'})
    etf_m1 = stock.get_etf_ohlcv_by_ticker(date[2]).reset_index()[['티커','종가']].rename(columns={'종가':'m1_종가'})
    etf_m3 = stock.get_etf_ohlcv_by_ticker(date[3]).reset_index()[['티커','종가']].rename(columns={'종가':'m3_종가'})
    etf_m6 = stock.get_etf_ohlcv_by_ticker(date[4]).reset_index()[['티커','종가']].rename(columns={'종가':'m6_종가'})
    etf_y1 = stock.get_etf_ohlcv_by_ticker(date[5]).reset_index()[['티커','종가']].rename(columns={'종가':'y1_종가'})
    etf = pd.merge(etf, etf_w1, how= 'outer', on = '티커')
    etf = pd.merge(etf, etf_m1, how= 'outer', on = '티커')
    etf = pd.merge(etf, etf_m3, how= 'outer', on = '티커')
    etf = pd.merge(etf, etf_m6, how= 'outer', on = '티커')
    etf = pd.merge(etf, etf_y1, how= 'outer', on = '티커').dropna()
    etf['1W'] = etf['종가'] / etf['w1_종가'] - 1
    etf['1M'] = etf['종가'] / etf['m1_종가'] - 1
    etf['3M'] = etf['종가'] / etf['m3_종가'] - 1
    etf['6M'] = etf['종가'] / etf['m6_종가'] - 1
    etf['1Y'] = etf['종가'] / etf['y1_종가'] - 1
    etf = etf[['티커', '종가','NAV','1W','1M','3M','6M','1Y']].rename(columns={'티커':'종목코드'})
    etf_rtn = pd.merge(etf_info[['기초시장','기초자산','기초자산상세','종목코드','ETF명']], etf, how = 'inner', on ='종목코드').sort_values(by= '1W', ascending = False)
    etf_rtn = etf_rtn[['기초시장','기초자산','기초자산상세','종목코드','ETF명','종가','NAV','1W','1M','3M','6M','1Y']]
    return etf_rtn


def get_top_pick(start_date, end_date, target_list, df, tgt_n) :
    etf = pd.DataFrame()
    for i in range(0, len(target_list)):
        etf_temp = pd.DataFrame(fdr.DataReader(target_list[i], start_date, end_date)['Close'])
        etf_temp.columns = df[df['종목코드'] == target_list[i]]['기초지수명']
        etf = pd.concat([etf, etf_temp], axis=1)
    month_list = etf.index.map(lambda x: datetime.strftime(x, '%Y-%m')).unique()
    rebal_date = pd.DataFrame()
    for m in month_list:
        rebal_date = rebal_date.append(
            etf[etf.index.map(lambda x: datetime.strftime(x, '%Y-%m')) == m].iloc[-1])
    rebal_date = rebal_date / rebal_date.shift(1) - 1
    rebal_date = rebal_date.fillna(-1)[1:len(rebal_date)]
    signal = pd.DataFrame((rebal_date.rank(axis=1, ascending=False) <= tgt_n).applymap(lambda x: '1' if x == True else '0'))
    df_etf = pd.DataFrame(index=signal.index, columns=list(range(1, tgt_n + 1)))
    df_rtn = pd.DataFrame(index=signal.index, columns=list(range(1, tgt_n + 1)))
    for s in range(0, len(signal)):
        if len(signal.columns[signal.iloc[s] == '1'].tolist()) != tgt_n:
            df_etf.iloc[s] = signal.columns[signal.iloc[s] == '1'].tolist() + ['Nan'] * (tgt_n - len(signal.columns[signal.iloc[s] == '1'].tolist()))
            df_rtn.iloc[s] = rebal_date[signal.columns[signal.iloc[s] == '1']].iloc[s].tolist() + [-1] * (tgt_n - len(signal.columns[signal.iloc[s] == '1'].tolist()))
        else:
            df_etf.iloc[s] = signal.columns[signal.iloc[s] == '1']
            df_rtn.iloc[s] = rebal_date[signal.columns[signal.iloc[s] == '1']].iloc[s]
    df_rtn_t = pd.DataFrame(columns=signal.index)
    df_etf_t = pd.DataFrame(columns=signal.index)
    for i in range(0, len(df_rtn)):
        df_rtn_t.iloc[:, i] = df_rtn.T.sort_values(by=df_rtn.T.columns[i], ascending=False).iloc[:, i].reset_index(drop=True)
        df_etf_t.iloc[:, i] = df_etf.iloc[i][df_rtn.T.sort_values(by=df_rtn.T.columns[i], ascending=False).iloc[:, i].index].reset_index(drop=True)
    df_rtn_t.columns = df_rtn_t.columns.strftime('%Y%m%d')
    df_etf_t.columns = df_etf_t.columns.strftime('%Y%m%d')
    df_all = pd.concat([df_etf_t, df_rtn_t])
    return df_all, df_etf_t, df_rtn_t

def get_pdf_data(df_theme, ticker_df, stddate) :
    pdf = pd.DataFrame()
    for i in range(0,len(df_theme.종목코드)) :
        df_theme.종목코드.iloc[i] = str(df_theme.종목코드.iloc[i]).zfill(6)
        pdf_temp = stock.get_etf_portfolio_deposit_file(str(df_theme.종목코드.iloc[i]), stddate).reset_index()
        try :
            pdf_temp = pdf_temp.rename(columns={'티커':'code'})
            pdf_temp = pdf_temp[['code','시가총액']].dropna(axis=1)
            pdf_temp['etf_code'] = df_theme.종목코드.iloc[i]
            pdf = pd.concat([pdf, pdf_temp])
        except Exception as e:
            print(i, ' 번 째 오류 발생 : ', df_theme.종목코드.iloc[i], ' 오류:', str(e))
    pdf = pd.merge(pdf, ticker_df[['code','name','sector_l']], how = 'outer', on ='code')
    pdf = pdf.rename(columns={'etf_code':'종목코드','sector_l':'섹터'})
    etf_pdf = df_theme[['자산','기초시장','기초자산','기초자산상세', '종목코드','ETF명','기초지수명', '키워드','CU','상장좌수']].drop_duplicates()
    etf_pdf = pd.merge(pdf, etf_pdf, how = 'outer', on ='종목코드').dropna()
    etf_pdf['시가총액_adj'] = etf_pdf['시가총액'] * etf_pdf['상장좌수'] / etf_pdf['CU']
    return etf_pdf
