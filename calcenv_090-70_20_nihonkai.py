import math
import csv
import pygrib
import numpy as np
import pandas as pd
import xarray as xr
import cfgrib
import matplotlib.pyplot as plt
import matplotlib as mpl
import matplotlib.ticker as mticker
import japanize_matplotlib
from mpl_toolkits.axes_grid1 import make_axes_locatable
import cartopy.crs as ccrs
import cartopy.util as cutil
import cartopy.feature as cfeature
from cartopy.mpl.ticker import LongitudeFormatter, LatitudeFormatter
from metpy.units import units
import metpy.calc as mpcalc
import metpy.constants as c
from datetime import datetime, timedelta
from dateutil import relativedelta
from struct import pack, unpack, calcsize, iter_unpack
from PIL import Image
import os
from time import sleep
from multiprocessing import Pool, cpu_count,Manager,Value,Array, Process #並列処理モジュール
from tqdm import tqdm
#from new_colors import * #自作モジュール
import warnings
from datetime import datetime
#警告非表示
# warnings.simplefilter('ignore')

# import warnings
warnings.filterwarnings('ignore')

#描 画 領 域 の 設 定
#日本
#latd=22.50 ; latu=45.00 ; lonl=121.00 ; lonr=149.00

#東北地方
lat_d, lat_u = 36.0, 44.0
lon_l, lon_r = 136.0, 145.0

#自作関数のインポート
#水蒸気供給
#水蒸気フラックス計算のための関数
def moisture_flux(Ds,path,YYYY,MM):

    if (YYYY < 2021) or (YYYY == 2021 and MM <= 6): #2021年7月以前/mnt/hail1/RRJ-Conv/prs0にあるもの　SPFHを読むがこれは実際には混合比
                q = Ds['q']#比湿と書かれているが実際には混合比

    else: #2022以降 /mnt/hail1/RRJ-Conv/prs　にあるもの　MIXRを読み込む（nameがないため番号指定）
                ds_q = xr.open_dataset(
                path,
                engine="cfgrib",
                backend_kwargs={
                    "filter_by_keys": {
                        "parameterCategory": 1,
                        "parameterNumber": 2,  #MIXR
                        }
                    }
                )

                #unknownなのでリネーム
                ds_q = ds_q.rename({"unknown": "q"})
                q = ds_q["q"]

                                                                
                                                                    
    Wu = Ds['u']#東西風
    Wv = Ds['v']#南北風
    temp = Ds['t']#気温
    lev = Ds['isobaricInhPa']#等圧面

    #水蒸気量の計算
    dd =  lev / (2.87 * temp)#乾燥空気の密度
    # kg/kg
    q0 = q #混合比
    # g/kg
    q1 = q0 * 1000
    # g /m3
    q = q1 * dd#水蒸気密度

    uv = mpcalc.wind_speed(Wu[2],Wv[2]) #[2]=.isel(isobaricInhPa=2)
    qV = q[2]*uv
    qV = qV*units['g/m**3']#水蒸気フラックス

    return qV

   #時間繰り、日にち繰りの関数
#forはあくまで外でやる　YYYY MM DDをチェックして変換
def add_hours(YYYY, MM,  DD,  HH):
    #月末を指定
    month_end = {1:31, 2:28, 3: 31, 4:30, 5:31, 6:30, 7:31, 8:31, 9:30, 10:31, 11:30, 12:31}

    if HH == 24: #足し算によって24を超えてしまう場合
        HH = 0
        DD += 1
    
    elif HH > 24:
        HH = HH - 24
        DD += 1

    if HH < 0: #-3時間の処理を入れたせいでマイナス側になる場合
        HH = HH + 24
        DD -= 1

    if DD == 0: #1日の時に-になった場合の対処
        MM = MM - 1
        DD = month_end[MM]

    if DD > month_end[MM]:
        DD = 1
        MM += 1

    if MM > 12:
        MM = 1
        YYYY + 1 == 1

    return YYYY, MM, DD, HH


#ここから本処理
def run_composite():
    #read csvfile
    csvfile = '/mnt/jet12/makoto/extract_senjo/ext_sun_edd_a/csv/total_4-10_2000-2024_ratio2.0_090-70_onlynihonkai.csv'
    df = pd.read_csv(csvfile, usecols=["hrid", "dtst", "dten","nt"],dtype={"dtst":str, "dten":str, "nt":int})

    #reinit array
    sum_ave_qV = np.zeros((577, 721))
    sum_ave_U = np.zeros((577, 721))
    sum_ave_V = np.zeros((577, 721))
    sum_ave_MSLP = np.zeros((577, 721))

    #loop
    for i in range(0,31):
        row = df.iloc[i]
        hrid = row["hrid"] #ID
        dtst = row["dtst"] #開始時間
        dten = row["dten"] #終了時間
        nt  =  row["nt"]   #継続時間

        #convert int
        hh = int(str(dtst)[-2:])
        hh = hh - 4 #ループの都合上-1
        dd = int(str(dtst)[6:8])
        mm = int(str(dtst)[4:6])
        yyyy = int(str(dtst)[:4])

        print(dd, type(dd))
        print(hh, type(hh))

        nt = int(row["nt"])
        nt_add = nt + 4
        print(nt, type(nt))

        #reinit array
        array_qV = np.zeros((577, 721)) #水蒸気フラックスの配列
        array_U = np.zeros((577, 721)) #東西風の配列
        array_V = np.zeros((577, 721)) #南北風の配列
        array_MSLP = np.zeros((577,721)) #海面気圧の配列

        #dtenが2023年7月1日以降であれば以下の処理は行わず次の行へ 
        YYYY_en = int(str(dten)[:4])
        MM_en   = int(str(dten)[4:6])
        DD_en   = int(str(dten)[6:8])

        current_date = datetime(YYYY_en, MM_en, DD_en)
        threshold = datetime(2023, 7, 1)
        
        if current_date >= threshold:
            print("skip this rain event because there's no RRJ file")
            continue

        #loop 1 case
        for i in range(nt+4):
            hh = hh + 1
            YYYY, MM, DD, HH = add_hours(yyyy,mm,dd,hh)

            #0
            MM = f"{MM:02d}"
            DD = f"{DD:02d}"
            HH = f"{HH:02d}"

            #3d
            #2021年7月4日12:00以降は格納ディレクトリが異なるので注意
            if YYYY < 2021:
                if MM in ["07", "08", "09", "10"]:
                    datadir = f'/mnt/hail1/regional_RA/GRIB2/prs0/{YYYY}/ctrl/fcst_prs_{YYYY}{MM}{DD}{HH}00.grib2'

                else:
                    YYYY_tag = YYYY - 1
                    datadir = f'/mnt/hail1/regional_RA/GRIB2/prs0/{YYYY_tag}/ctrl/fcst_prs_{YYYY}{MM}{DD}{HH}00.grib2'  
             
            else:
                if MM in ["07", "08", "09", "10"]:
                     datadir = f'/mnt/hail1/regional_RA/GRIB2/prs/{YYYY}/ctrl/fcst_prs_{YYYY}{MM}{DD}{HH}00.grib2'

                else:
                    if YYYY == 2021:
                        YYYY_tag =  YYYY -1
                        datadir = f'/mnt/hail1/regional_RA/GRIB2/prs0/{YYYY_tag}/ctrl/fcst_prs_{YYYY}{MM}{DD}{HH}00.grib2'

                    elif YYYY == 2023:
                        datadir = f'/mnt/hail1/regional_RA/GRIB2/prs/2022/ctrl/fcst_prs_{YYYY}{MM}{DD}{HH}00.grib2'

                    else:
                         YYYY_tag = YYYY - 1
                         datadir = f'/mnt/hail1/regional_RA/GRIB2/prs/{YYYY_tag}/ctrl/fcst_prs_{YYYY}{MM}{DD}{HH}00.grib2'

            print(datadir)

            #2d
            if MM in ["07", "08", "09", "10"]:
                datadir2 = f'/mnt/hail1/regional_RA/GRIB2/sfc/{YYYY}/ctrl/fcst_sfc_{YYYY}{MM}{DD}{HH}00.grib2'

            else:
                YYYY_tag = YYYY - 1
                datadir2 = f'/mnt/hail1/regional_RA/GRIB2/sfc/{YYYY_tag}/ctrl/fcst_sfc_{YYYY}{MM}{DD}{HH}00.grib2'

            print(datadir2)

            #3d
            grb_file_xr = xr.open_dataset(datadir, engine='cfgrib')
            Ds = grb_file_xr #.isel(isobaricInhPa=2)
            grb_file_xr.close()

            #2d
            Ds2 = xr.open_dataset(
                datadir2,
                engine="cfgrib",
                backend_kwargs={'filter_by_keys': {'typeOfLevel': 'meanSea'}}
                )


            hh = int(hh)

            dten_1 = f"{YYYY}{MM}{DD}{HH}"
            print(dten_1)
            
            #sea pressure
            mslp = Ds2['prmsl'].values / 100  # Pa → hPa
            array_MSLP += mslp

            #水蒸気フラックス
            #YYYY MM DD HH が dtenと一致するまで積算を繰り返す

            YYYY = int(YYYY)
            MM   = int(MM)

            qV=moisture_flux(Ds,datadir,YYYY,MM)
            array_qV += qV   

            #風速分布
            u = Ds['u']
            v = Ds['v']

            U = u.sel(isobaricInhPa=950)
            V = v.sel(isobaricInhPa=950)

            array_U += U.values
            array_V += V.values

            #0
            DD = int(DD)
            MM = int(MM)
            HH = int(HH)

            MM = f"{MM:02d}"
            DD = f"{DD:02d}"
            HH = f"{HH:02d}"
            
            #1 case average
            if  dten_1 == dten:
                ave_qV = array_qV / nt_add #平均 3時間分ふえてるのでnt +3 でわる
                ave_U = array_U / nt_add
                ave_V = array_V / nt_add

                ave_MSLP = array_MSLP / nt_add
                print("matched dten, breaking loop", flush=True)
                break

        sum_ave_qV += ave_qV #各事例の平均を足し合わせる
        sum_ave_U += ave_U
        sum_ave_V += ave_V
        sum_ave_MSLP += ave_MSLP
         

        pass

    np.save("data/sum_ave_qV_090-70_20_nihonkai.npy", sum_ave_qV)
    np.save("data/sum_ave_U_090-70_20_nihonkai.npy", sum_ave_U)
    np.save("data/sum_ave_V_090-70_20_nihonkai.npy", sum_ave_V)
    np.save("data/sum_ave_MSLP_090-70_20_nihonkai.npy", sum_ave_MSLP)

if __name__ == "__main__":
    run_composite()



