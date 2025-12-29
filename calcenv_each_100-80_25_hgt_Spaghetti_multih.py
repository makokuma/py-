#各事例それぞれコンポジットして図として出力させる

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
from matplotlib.lines import Line2D

import importlib
import  plot_module #自作モジュール
importlib.reload(plot_module)

#警告非表示
# warnings.simplefilter('ignore')

# import warnings
warnings.filterwarnings('ignore')

#描 画 領 域 の 設 定
#日本
#latd=22.50 ; latu=45.00 ; lonl=121.00 ; lonr=149.00

#東北地方
lat_d, lat_u = 19.7, 48.8
lon_l, lon_r = 107.5, 156.1

def add_hours(YYYY, MM,  DD,  HH):
     month_end = {1:31, 2:28, 3: 31, 4:30, 5:31, 6:30, 7:31, 8:31, 9:30, 10:31, 11:30, 12:31}
     if HH == 24: #足し算によって24を超えてしまう場合
         HH = 0
         DD += 1

     elif HH > 24:
        HH = HH -24
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
area = 'nihonkai'
num_hPa = 300
csvfile = f'/mnt/jet12/makoto/extract_senjo/ext_sun/csv/100-80/{area}_new_100-80_25.csv'
df = pd.read_csv(csvfile, usecols=["hrid", "dtst", "dten","nt"],dtype={"dtst":str, "dten":str, "nt":int})

# ---- 事例ループの外に図を1個だけ作る ----
#proj = ccrs.PlateCarree()
#fig, ax = plt.subplots(figsize=(12, 8), subplot_kw={'projection': proj})

proj = ccrs.PlateCarree()
#fig, (ax, ax_tmp) = plt.subplots(
#    1, 2,
#    figsize=(12, 8),
#    subplot_kw={'projection': proj}
#)
#ax_tmp.set_visible(False)

fig = plt.figure(figsize=(12, 8))
ax = fig.add_subplot(111, projection=proj)
ax_tmp = fig.add_subplot(111, projection=proj)
ax_tmp.set_axis_off()

# ベースの地図だけ最初に描く
plot_module.plot_map(ax, lon_l, lon_r, lat_d, lat_u, color='black')

for i in range(0,len(df)):
    row = df.iloc[i]
    hrid = row["hrid"] #ID
    dtst = row["dtst"] #開始時間
    dten = row["dten"] #終了時間
    nt  =  row["nt"]   #継続時間

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

    #reinit
    array_hgt = np.zeros((577, 721)) #HGTの配列
    array_MSLP = np.zeros((577,721)) #海面気圧の配列

    #dtenが2023年7月1日以降であれば以下の処理は行わず次の行へ fcst_prs_202307010000.grib2以降はない
    YYYY_en = int(str(dten)[:4])
    MM_en   = int(str(dten)[4:6])
    DD_en   = int(str(dten)[6:8])

    current_date = datetime(YYYY_en, MM_en, DD_en)
    threshold = datetime(2023, 7, 1)

    if current_date >= threshold:
        print("skip this rain event because there's no RRJ file")
        continue

    for i in range(nt+4):
        hh = hh + 1
        YYYY, MM, DD, HH = add_hours(yyyy,mm,dd,hh)

        #ゼロ埋め
        MM = f"{MM:02d}"
        DD = f"{DD:02d}"
        HH = f"{HH:02d}"

        print(YYYY)
        print(MM)
        print(DD)
        print(HH)
        
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



        #read data
        #3d
        grb_file_xr = xr.open_dataset(datadir, engine='cfgrib')
        Ds = grb_file_xr#.isel(isobaricInhPa=2)
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

        #hgt
        hgt = Ds['gh']
        HGT = hgt.sel(isobaricInhPa=num_hPa)
        array_hgt += HGT

        #sea pressure
        mslp = Ds2['prmsl'].values / 100  # Pa → hPa
        array_MSLP += mslp

        if  dten_1 == dten:
            ave_hgt = array_hgt / nt_add
            ave_MSLP = array_MSLP / nt_add
            print("matched dten, breaking loop", flush=True)
            

            #data plot and savefig
            # 緯度経度
            lat = Ds['latitude'].values
            lon = Ds['longitude'].values

#            proj = ccrs.PlateCarree()
#            fig, ax = plt.subplots(figsize=(12,8), subplot_kw={'projection': proj})

            #東北上空で検出された高度でプロットする 

            lat_min, lat_max = 39.0, 40.5   # 秋田含む
            lon_min, lon_max = 140.0, 141.0

            levels = np.arange(8500, 9800, 1)  # m

            south_lat = {}
           # north_lat = {}
           # for lev in levels:
          #      ncol_before = len(ax.collections)

           #     cs = ax_tmp.contour(lon, lat, ave_hgt, levels=[lev])
           #     min_lat=-999
                #max_lat=-999

           #     for seg in cs.allsegs[0]:
           #         lat_seg = seg[:,1]
           #         lon_seg = seg[:,0]

            box_mask = (
                    (lat >= lat_min) & (lat <= lat_max) &
                    (lon >= lon_min) & (lon <= lon_max)
                )

            gh_box = ave_hgt.values[box_mask]

            gh_min = np.nanmin(gh_box)
            gh_max = np.nanmax(gh_box)

                    #max north min south
            #        if np.any(mask):
               #       min_lat = min(min_lat, lat_seg[mask].min())

          #      del ax.collections[ncol_before:]

                #if min_lat > -999:
                 #   south_lat[lev] = min_lat
 
            ax_tmp.cla()
            ax_tmp.set_axis_off()
            #selected_height = max(north_lat, key=north_lat.get)
            #selected_height = min(south_lat, key=south_lat.get)
            selected_height_low = gh_min
            selected_height_high = gh_max

            #N30以下は描画しない 
            mask = lat < 30.0
            hgt_plot = ave_hgt.values.copy()
            hgt_plot[mask] = np.nan

            ax.contour(
                lon, lat, hgt_plot,
                levels=[selected_height_high],
                colors='red',
                linewidths=1.5,
                transform=ccrs.PlateCarree()
            )



            # -----  スパゲティ図として等高度線を重ね書き -----
            #red 8900m
#            ax.contour(
#                lon,
#                lat,
#                ave_hgt,
#                levels=[9300],
#                colors='red',
#                linewidths=1.5,
#                transform=ccrs.PlateCarree(),
#                label=f"{hrid}"
#            )

#            #blue 8800m 
#            ax.contour(
#                lon,
#                lat,
#                ave_hgt,
#                levels=[9400],
#                colors='blue',
#                linewidths=1.5,
#                transform=ccrs.PlateCarree(),
#                label=f"{hrid}"
#            )

#            #green 5700m
#            ax.contour(
#                lon,
#                lat,
#                ave_hgt,
#                levels=[9500],
#                colors='green',
#                linewidths=1.5,
#                transform=ccrs.PlateCarree(),
#                label=f"{hrid}"
#            )

            ax.set_title(f'Senjo-kousuitai 100-80_25 {num_hPa}hPa 15cases',fontsize=22)

#            legend_lines = [
#                Line2D([0], [0], color='red', linewidth=2),
#                Line2D([0], [0], color='blue', linewidth=2),
#                Line2D([0], [0], color='green', linewidth=2)
#            ]

            #legend_labels = ['9300 m', '9400 m', '9500 m']

#            ax.legend(
#               legend_lines,
#               legend_labels,
#               loc='upper right',
#               fontsize=14,
#               frameon=True
#            )

#            plot_module.plot_map(ax,lon_l,lon_r,lat_d,lat_u,color='black')
#            plot_module.plot_shaded(ax,lon,lat,ave_hgt,cmap='coolwarm',vmin=5600,vmax=5900,lint=40, labelname='m')
#           plot_module.plot_contour(ax, lon, lat, ave_MSLP, 980, 1020, 1, color='black', thick=1.5)

            array_hgt = np.zeros((577, 721))
            array_MSLP = np.zeros((577, 721))
            break

#savefig
figpath = f'/mnt/jet12/makoto/extract_senjo/environment/png/100-80_ratio25/hgt/Spaghetti_composite_flux_{area}_100-80_25_{num_hPa}hPa_multih.png'
print('saved image')
plt.savefig(figpath)
plt.show()
