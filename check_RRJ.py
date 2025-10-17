#cfgribでRRJファイルの中身を確認
import xarray as xr
import cfgrib

datadir = '/mnt/hail1/regional_RA/GRIB2/prs0/2020/ctrl/fcst_prs_202107040500.grib2' 

grb_file_xr = xr.open_dataset(datadir, engine='cfgrib')
xr.set_options(display_max_rows=200, display_width=200)
Ds = grb_file_xr#.isel(isobaricInhPa=2)
grb_file_xr.close()

#xr.set_options(display_max_rows=None, display_width=None)
#print(Ds)

for var in Ds.data_vars:
    attrs = Ds[var].attrs
    longname = attrs.get('long_name', attrs.get('longname', '(long_nameなし)'))
    units = attrs.get('units', '(単位なし)')
    print(f"{var:10s} | long_name: {longname} | units: {units}")

