# EUBUCCO height-data quality

## Overview 
This repo investigates height data within the EUBUCCO database, with the aim of removing buildings with bad heights to improve energy models derived from the data. The following table describes the notebook and the steps taken in the analysis.


| Notebook | Purpose |
|---- |---- |
| [01a_extract-sql.ipynb](./01a_extract-sql.ipynb)| Extracts a semi-random 10% sample of EUBUCCO database in chunks and stores the chunks in partitioned parquet files. <br /> This is helpful for performing statistical analysis on a local machine with limited computing resources, by using a largish subset of the data.|
| [01b_extract-sql.ipynb](./01b_extract-sql.ipynb)| The download from the database doesn't optimize the chunk size for further analysis with dask. <br /> This notebook repartitions the downloaded parquet files (into 300-MB partitions). |
| [02_error-analysis-HEIGHT.ipynb](02_error-analysis-HEIGHT.ipynb)| A general analysis of missing numbers, and of height data in different categories. The frequency and relative frequency of low heights and invalid/missing heights is visualised per country and region. Low heights are defined as below 2.5 m.  |
| [03_error-analysis-ELONGATION-AREA.ipynb](03_error-analysis-ELONGATION-AREA.ipynb)| Not all low buildings are necessarily misidentified structures. Here, the criteria of *area* and *elongation* are considered to help exclude undesirable and misidentified structures from the energy model.  |


## TO DOs
* Plot the elongation distribution for Switzerland (see notebook 03). Switzerland may have a significant number of elongated structures.
