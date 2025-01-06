# Estimating offline Statistics From Online Data Sources

This repository containst the code for the paper `Estimating offline Statistics From Online Data Sources`.

## Setup

This project builds on python 3.10. It is recommended to create a virtural environment. The setup for running pyqgis scripts is slightly different and described in the QGIS section.

## Data

### Offline Data
We provide the offline data collected for various billion dollar disasters in 2017. The data is collected from SHELDUS and aggregated at a county level. The cost estimates for property or crop damage are CPI-adjusted to 2017.

### QGIS 
Most of the data for offline statistics at a county-level is given as shapefiles. While QGIS is a suitable GIS tool to read and operate on shape files, we use python API provided, PyQGIS to perform some operations. 

Note that only to run PyQGIS scripts in the `qgis` folder, it is recommended to use the python that is bundled with the installed qgis version. For instance on QGIS3-LTR on MacOS installed with MacPorts, `/opt/local/bin/python3.12 pyqgis_test.py` would run seamlessly, than using your virtual environment python interpreter.


