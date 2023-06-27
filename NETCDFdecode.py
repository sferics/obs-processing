import netCDF4 as nc
from netCDF4 import Dataset

#https://unidata.github.io/netcdf4-python/#tutorial
#https://towardsdatascience.com/read-netcdf-data-with-python-901f7ff61648

data = Dataset("KNMI/KMDS__OPER_P___10M_OBS_L2_202306270920.nc", "r", format="NETCDF4")

print(data)
print(data["station"])

data.close()
