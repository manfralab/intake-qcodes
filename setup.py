from setuptools import setup, find_packages

setup(
    name="intake-qcodes",
    version="0.1",
    description="intake driver for qcodes data",
    url="t",
    classifiers=[
        "Intended Audience :: Science/Research",
        "Programming Language :: Python :: 3 :: Only",
        "Licence :: MIT Licence",
        "Topic :: Scientific/Engineering",
    ],
    license="MIT",
    packages=find_packages(),
    python_requires=">=3",
    install_requires=[
        'qcodes',
        'xarray',
        'pandas',
        'intake',
        'panel>=0.8.0',
        'hvplot==0.5.2',
        'bokeh==1.4.0',
        'qcodes>=0.12',
    ]
)
