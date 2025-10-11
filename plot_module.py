import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.ticker as mticker
import numpy as np

# =====================================================
# 基本の地図
# =====================================================
def plot_map(ax, lon_min, lon_max, lat_min, lat_max, color='black'):
    
    """
    ax: matplotlib axis (Cartopy projection)
    lon_min, lon_max, lat_min, lat_max: 描画範囲
    color: 枠線や国境線の色
    """
    countries_10m = cfeature.NaturalEarthFeature(
        'cultural', 'admin_1_states_provinces_lines',
        '10m', edgecolor=color, facecolor='none'

    )

    ax.set_extent([lon_min, lon_max, lat_min, lat_max])
    ax.coastlines(resolution='10m', lw=0.5)
    ax.add_feature(countries_10m, linestyle='dotted', linewidth=0.5, alpha=1.0)
    gl = ax.gridlines(ccrs.PlateCarree(), draw_labels=True,
       linewidth=1, linestyle=':')
    gl.xlocator = mticker.FixedLocator(np.arange(-180, 181, 1))
    gl.ylocator = mticker.FixedLocator(np.arange(-90, 91, 1))
    gl.top_labels = False
    gl.right_labels = False
    return ax

# =====================================================
# 1. 塗りつぶし（例: 水蒸気フラックス）
# =====================================================
def plot_shaded(ax, lon, lat, data, cmap='YlGnBu', vmin=None, vmax=None, lint=None, labelname=''):
    """
    ax        : 既存のaxis（Cartopy）
    lon, lat  : 座標
    data      : 描画データ
    cmap      : カラーマップ名またはオブジェクト
    vmin,vmax : カラーバー範囲
    lint      : 等値間隔
    labelname : カラーバータイトル
    """

    if lint is not None:
        levels = np.arange(vmin, vmax + lint, lint)

    else:
        levels = np.linspace(np.nanmin(data), np.nanmax(data), 10)

    im = ax.contourf(
        lon, lat, data,
        levels=levels, cmap=cmap,
        transform=ccrs.PlateCarree(),
        extend='both'

    )

    cbar = plt.colorbar(
        im, ax=ax, orientation='horizontal',
        pad=0.05, shrink=0.62,
        extendfrac='auto', extendrect=True,
        drawedges=True, ticks=levels
    )

    cbar.set_label(labelname)
    return im

# =====================================================
# 2. コンター（例: 気圧線）
# =====================================================

def plot_contour(ax, lon, lat, data, vmin, vmax, lint, color='black', thick=1.0):
    """
    ax     : axis
    lon,lat: 座標
    data   : 描画データ
    vmin,vmax,lint: 等値線範囲と間隔
    color  : 線の色
    thick  : 線の太さ
    """

    levels = np.arange(vmin, vmax + lint, lint)
    cs = ax.contour(
        lon, lat, data,
        levels=levels, colors=color,
        transform=ccrs.PlateCarree(), linewidths=thick
    )
    ax.clabel(cs, inline=False, fontsize=8, fmt="%d", inline_spacing=0)
    return cs

# =====================================================
# 3. ベクトル（例: 風）
# =====================================================
def plot_vector(ax, lon, lat, data_x, data_y, skip=5, size=0.003, color='black', base=10, unit='m/s'):
    """
    ax        : axis
    lon, lat  : 座標
    data_x,y  : ベクトル成分
    skip      : 簡略化間隔
    size      : 矢印太さ
    color     : 矢印の色
    base      : 基準値
    unit      : 単位
    """

    q = ax.quiver(
        lon[::skip, ::skip], lat[::skip, ::skip],
        data_x[::skip, ::skip], data_y[::skip, ::skip],
        color=color, scale=200, alpha=0.9, width=size,
        transform=ccrs.PlateCarree()
    )
    ax.quiverkey(q, 0.1, -0.15, base, f'{base} {unit}', labelpos='E')
    return q
