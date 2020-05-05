﻿# intake-qcodes

Intake driver for qcodes data. Includes support for pandas.DataFrame and xarray.Dataset loading.  
  
Lazy-loading (Dask) is not yet supported. But, some effort has been made to reduce the number of calls to the database. The built-in Intake caching _should_ work.
