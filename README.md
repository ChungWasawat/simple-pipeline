# simple-pipeline

This project is designed to increase my understanding of how a data pipeline should be structured and implemented. I am using the guidelines and free templates provided by Joseph Machado in his [project](https://www.startdataengineering.com/post/data-engineering-projects-with-free-template/) as a foundation for this project.

## Data Description

|Key	            |Description |
|-------------------|------------|
|exchangeId	        |unique identifier for exchange|
|name	            |proper name of exchange|
|rank	            |rank is in ascending order - this number is directly associated with the total exchange volume whereas the highest volume exchange receives rank 1|
|percentTotalVolume	|the amount of daily volume a single exchange transacts in relation to total daily volume of all exchanges|
|volumeUsd	        |daily volume represented in USD|
|tradingPairs	    |number of trading pairs (or markets) offered by exchange|
|socket	            |true/false, true = trade socket available, false = trade socket unavailable|
|exchangeUrl	    |website to exchange|
|updated	        |UNIX timestamp (milliseconds) since information was received from this exchange|

## Tech breakdown
* Airflow



###  
1. `docker-compose up airflow-init` to set an airflow system
2. `docker-compose build`




## Acknowledgments
A big thanks to Joseph Machado for sharing this comprehensive project.
