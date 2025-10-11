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
import metpy.calc as mpcalc
from metpy.calc import cape_cin, dewpoint_from_relative_humidity, parcel_profile
from metpy.units import units

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
def moisture_flux(Ds,path,YYYY):

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
#cape計算test

# === 設定 ===
YYYY, MM, DD, HH = 2011, 8, 26, 17
MM = f"{MM:02d}"; DD = f"{DD:02d}"; HH = f"{HH:02d}"

# === ファイルパス選択 ===
if YYYY < 2021:
        if MM in ["07", "08", "09", "10"]:
            datadir = f'/mnt/hail1/regional_RA/GRIB2/prs0/{YYYY}/ctrl/fcst_prs_{YYYY}{MM}{DD}{HH}00.grib2'
        else:
            YYYY -= 1
            datadir = f'/mnt/hail1/regional_RA/GRIB2/prs0/{YYYY}/ctrl/fcst_prs_{YYYY}{MM}{DD}{HH}00.grib2'
else:
        if MM in ["07", "08", "09", "10"]:
            datadir = f'/mnt/hail1/regional_RA/GRIB2/prs/{YYYY}/ctrl/fcst_prs_{YYYY}{MM}{DD}{HH}00.grib2'
        else:
            if YYYY == 2021:
                 YYYY -= 1
                 datadir = f'/mnt/hail1/regional_RA/GRIB2/prs0/{YYYY}/ctrl/fcst_prs_{YYYY}{MM}{DD}{HH}00.grib2'
            elif YYYY == 2023:
                 datadir = f'/mnt/hail1/regional_RA/GRIB2/prs/2022/ctrl/fcst_prs_{YYYY}{MM}{DD}{HH}00.grib2'
            else:
                 YYYY -= 1 
                 
                 datadir = f'/mnt/hail1/regional_RA/GRIB2/prs/{YYYY}/ctrl/fcst_prs_{YYYY}{MM}{DD}{HH}00.grib2'
print("open:",datadir)

# === データ読み込み ===
Ds = xr.open_dataset(datadir, engine='cfgrib')
prs  = Ds['isobaricInhPa'].values * units.hPa
temp = Ds['t']

ds_depr = xr.open_dataset(datadir, engine='cfgrib',
    backend_kwargs={"filter_by_keys": {"parameterCategory": 0, "parameterNumber": 7}}
    )
depr = ds_depr.rename({"unknown": "depr"})["depr"]
depr.attrs['units'] = 'K'

# === CAPE/CIN計算 ===
ny, nx = temp.shape[1], temp.shape[2]
cape_vals = np.full((ny, nx), np.nan)
cin_vals  = np.full((ny, nx), np.nan)

for iy in tqdm(range(0,ny,40), desc="CAPE計算中"):
    for ix in range(0,nx,40):
        try:
            T = temp.isel(y=iy, x=ix).metpy.convert_units('K').values * units.kelvin
            Td = (T - depr.isel(y=iy, x=ix).values * units.kelvin).to('degC')
            T = T.to('degC')

            mask = (~np.isnan(T)) & (~np.isnan(Td)) & (~np.isnan(prs.m))
            if np.count_nonzero(mask) < 5:
                continue

            prof = parcel_profile(prs, T[0], Td[0]).to('degC')
            if np.any(np.isnan(prof)):
                continue

            cape, cin = mpcalc.cape_cin(prs[mask], T[mask], Td[mask], prof[mask])
            cape_vals[iy, ix] = cape.m
            cin_vals[iy, ix] = cin.m

        except Exception as e:
            continue

# === 結果保存 ===
np.save('data/cape_2018071106.npy', cape_vals)
np.save('data/cin_2018071106.npy',  cin_vals)

print("計算完了・保存しました。")

