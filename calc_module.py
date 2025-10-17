#コンポジット計算用のモジュール置き場

#prs --> num list
def prs_num(prs):
    prs_map = {
        1000: 0,
        975: 1,
        950: 2,
        925: 3,
        900: 4,
        875: 5,
        850: 6,
        800: 7,
        750: 8,
        700: 9,
        600: 10,
        500: 11,
        400: 12,
        300: 13,
        200: 14,
        100: 15,
         70: 16
    }

    return prs_num.get(prs, 999) #対応なしなら999

#calc saturated water vapor pressure
def e_sat(Ds,prs,path,YYYY):
    num = prs_num(prs) #指定した気圧面を数字変換

    #read data
    #混合比
    if (YYYY < 2021) or (YYYY == 2021 and MM <= 6): #2021年7月以前/mnt/hail1/RRJ-Conv/prs0にあるもの　SPFHを読むがこれは実際には混合比
        q = Ds['q']#比湿と書かれているが実際には水蒸気混合比

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
    q = ds_q["q"] #水蒸気混合比

    temp = Ds['t'] #温度
    lev = Ds['isobaricInhPa']#等圧面

    #calc water vapar
    eta = 287 / 461  #R_d / R_v
    e = q * lev / q + eta #水蒸気圧

    #calc saturated water vapor pressure
    esat = 6.112 * math.exp(17.67 * (temp - 237.15) / ((temp - 273.15) + 243.5))
    
    esat_prs = esat[num]
    return(esat_prs)

#calc Equivalent potential temperature

def theta_sat(Ds,prs,path,YYYY):
    num = prs_num(prs) #指定した気圧面を数字変換

    #read data
    temp = Ds['t'] #温度
    lev = Ds['isobaricInhPa']#等圧面
    esat = e_sat(Ds,prs,path,YYYY) #飽和水蒸気圧

    #calc saturated mixing ratio
    w_s = 0.622 * esat / lev[num]

    #calc potential temperature
    gamma = 287 / 1004
    p_0 = 1000 #lowest level
    theta = temp[num] * ( p_0 / lev)**gamma #温位

    #Equivalent potential temperature
    L = 2.5 * 10**6 #J/kg
    theta_sat = theta math.exp(L * w_s / 1004 * temp[num]

    return(theta_sat)

#calc rh (6-condition)
def calc_rh(Ds,prs,path,YYYY):
    
    num = prs_num(prs) #指定した気圧面を数字変換

    #read data
    #混合比
    if (YYYY < 2021) or (YYYY == 2021 and MM <= 6): #2021年7月以前/mnt/hail1/RRJ-Conv/prs0にあるもの　SPFHを読むがこれは実際には混合比
        q = Ds['q']#比湿と書かれているが実際には水蒸気混合比

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
    q = ds_q["q"] #水蒸気混合比

    temp = Ds['t'] #温度
    lev = Ds['isobaricInhPa']#等圧面

    #calc water vapar
    eta = 287 / 461  #R_d / R_v
    e = q * lev / q + eta #水蒸気圧

    #calc saturated water vapor pressure
    esat = 6.112 * math.exp(17.67 * (temp - 237.15) / ((temp - 273.15) + 243.5))
    
    esat_prs = esat[num]

    rh = 100 * (e / esat)
    rh_data = rh[num]
    return(rh)

#calc Geometric vertical velocity horizontal  average (6-condition)
def calc_w_ave(Ds,YYYY):
    import numpy as np
    
    num = prs_num(prs) #指定した気圧面を数字変換

    #read data
    w = Ds['wz'] #鉛直速度 m/s

    #400km horizontal average
    nx,ny - w.shape
    spec = 5 #格子間隔
    dxy = int(400 / spec / 2)
    w_ave = np.zeros_like(w)
    
    for i in range(nx)
        for j in range(ny)

            i_min = max(i- dxy, 0)
            i_max = min(i + dxy + 1, ny)
            j_min = max(j- dxy, 0)
            j_max = min(j + dxy + 1, ny)

            #calc ave
            w_local = w[i_min:imax, j_min:j_max]
            w_ave[i, j] = np.mean(w_local)

    return w_ave

        

