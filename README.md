# Singapore Weather Data in 130 seconds and 24MB csv

Download and collect whole www.weather.gov.sg in 130 seconds with the power of async python3

Hiring Challenge for Data Engineer role at ComfortDelGro Singapore

1) Write a python script to download the historical climate data of all locations into a csv file.
2) 
http://www.weather.gov.sg/climate-historical-daily/

Output as a csv file in below format with all the stations in a file per month for 2022 Sep, 2022 Oct and 2022 Nov.

CSV format:
```
Station,Year,Month,Day,Daily Rainfall Total (mm),Highest 30 min Rainfall (mm),Highest 60 min Rainfall (mm),Highest 120 min Rainfall (mm),Mean Temperature (°C),Maximum Temperature (°C),Minimum Temperature (°C),Mean Wind Speed (km/h),Max Wind Speed (km/h)
Changi,2023,1,1,     0.0,     0.0,     0.0,     0.0,    27.2,    30.6,    25.3,     9.8,    31.5
Admiralty,2023,1,1,     0.0,     0.0,     0.0,     0.0,    26.8,    30.1,    24.8,    10.7,    27.8
```

Please share with us how to run your script.

```
pip3 install uv
uv venv venv
source venv/bin/activate
uv pip install -r requirements.txt
python3 main.py
```
