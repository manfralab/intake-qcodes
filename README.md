# intake-qcodes



Intake driver for qcodes data. Includes support for pandas.DataFrame and xarray.Dataset loading.  
  
Lazy-loading (Dask) is not yet supported. But, some effort has been made to reduce the number of calls to the database. The built-in Intake caching _should_ work.

The `hvplot` and `bokeh` requirements are a bit strict because of some bokeh 2.0.0 updates. I'm sure there is a smart way to fix that. For now the requirements are exact.
