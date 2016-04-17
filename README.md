# Big Data Random Forests in R
Project for MSA220 at Chalmers, April 2016

This project investigates using R to perform random forest classification on a large dataset of flight information. Several methods are applied to various sample sizes. The aim is to better understand the limitations of R, and how  to handle random forest classification for a given dataset size.

## Code

* Flight data needs to be downloaded from [ASA](http://stat-computing.org/dataexpo/2009/the-data.html)
* `preprocess.py` downloads the weather data, combines it with the flight data, and pulls a subset of rows and columns
* Random samples should be made of the dataset, a 10000 line example is given
* `small_data.R` classifies the flight data using the `randomForest` package
* `big_data.R` uses the `bigrf` package
