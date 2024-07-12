import os
import shutil
import asyncio
import aiohttp
import aiofiles
from bs4 import BeautifulSoup
from time import time
import duckdb

timeout_parameter = 200
retry_parameter = 100

month_mapping = {
    'January': '01',
    'February': '02',
    'March': '03',
    'April': '04',
    'May': '05',
    'June': '06',
    'July': '07',
    'August': '08',
    'September': '09',
    'October': '10',
    'November': '11',
    'December': '12'
}


async def data_cleaning(save_path, response_content, url):
    try:
        async with aiofiles.open(save_path, 'wb') as file:
            if isinstance(response_content, bytes):
                cleaned_content = (
                    response_content
                    .replace(b'(C)', b'(\xc2\xb0C)')
                    .replace(b' Min ', b' min ')
                    .replace(b'\xef\xbb\xbf', b'')
                )
                await file.write(cleaned_content)
            else:
                cleaned_content = (
                    response_content
                    .replace('(C)', '(°C)')
                    .replace(' Min ', ' min ')
                    .replace(b'\xef\xbb\xbf'.decode(), '').encode()
                )
                await file.write(cleaned_content)

    except Exception as e:
        print(f'Exception occurred while saving file: {e}, url: {url}')
        return ['get_saving', url, None, f'Error | {e}']

    try:
        async with aiofiles.open(save_path, 'r', encoding='utf-8', errors='ignore') as file:
            content = await file.read()

        async with aiofiles.open(save_path, 'w', encoding='utf-8') as file:
            await file.write(content)

    except Exception as e:
        print(f'Exception occurred while reading/writing file: {e}, url: {url}')
        return ['get_saving', url, None, f'Error | {e}']

    return ['success', None, None, None]


async def download_csv(url, save_path):
    for i in range(retry_parameter):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=timeout_parameter) as response:
                    if response.status == 200:
                        content = await response.read()
                        return await data_cleaning(save_path, content, url)
                    else:
                        if i == (retry_parameter - 1):
                            return ['get_downloading', url, None, f'Error | Final Retry - {retry_parameter}']
                        raise
        except Exception as e:
            if i == (retry_parameter - 1):
                return ['get_downloading', url, None, f'Error | {e}']

    return ['get_downloading', url, None, None]


async def get_csv_link(dictx):
    station, year, month = dictx["station"], dictx["year"], dictx["month"]
    print('get_csv_link', station, year, month)
    return await download_csv(
        f'http://www.weather.gov.sg/files/dailydata/DAILYDATA_{station}_{year}{month_mapping[month]}.csv',
        f'weather_files/DAILYDATA_{station}_{year}{month_mapping[month]}.csv'
    )


async def get_months_then_csv_link(dictx):
    station, cityname, year = dictx["station"], dictx["cityname"], dictx["year"]
    print('get_months_then_csv_link', station, cityname, year)
    months_response = None
    for i in range(retry_parameter):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                        'http://www.weather.gov.sg/wp-content/themes/wiptheme/page-functions/functions-climate-historical-daily-months.php',
                        data={
                            'year': year,
                            'cityname': cityname
                        },
                        timeout=timeout_parameter
                ) as response:
                    months_response = await response.read()
                    if not months_response or response.status != 200:
                        if i == (retry_parameter - 1):
                            return ['get_months', cityname, year, f'Error | Final Retry - {retry_parameter}']
                    else:
                        break
        except Exception as e:
            if i == (retry_parameter - 1):
                return ['get_months', cityname, year, f'Error | {e}']

    months_soup = BeautifulSoup(months_response, 'html.parser')
    all_months = [month_html.a['href'][1:] for month_html in months_soup.find('ul').find_all('li')]
    valuez = [{'station': station, 'year': year, 'month': month} for month in all_months]
    tasks = [asyncio.create_task(get_csv_link(d)) for d in valuez]
    return await asyncio.gather(*tasks)


async def get_years_then_months_then_csv_link(dictx):
    cityname, station = dictx[1], dictx[0]
    print('get_years_then_months_then_csv_link', cityname, station)
    years_response = None
    for i in range(retry_parameter):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                        'http://www.weather.gov.sg/wp-content/themes/wiptheme/page-functions/functions-climate-historical-daily-year.php',
                        data={
                            'stationCode': station
                        },
                        timeout=timeout_parameter
                ) as response:
                    years_response = await response.read()
                    if not years_response or response.status != 200:
                        if i == (retry_parameter - 1):
                            return ['get_years', cityname, f'Error | Final Retry - {retry_parameter}', None]
                        raise
                    else:
                        break
        except Exception as e:
            if i == (retry_parameter - 1):
                return ['get_years', cityname, f'Error | {e}', None]

    years_soup = BeautifulSoup(years_response, 'html.parser')
    all_years = [month_html.a['href'][1:] for month_html in years_soup.find('ul').find_all('li')]
    valuey = [{'station': station, 'cityname': cityname, 'year': year} for year in all_years]
    tasks = [asyncio.create_task(get_months_then_csv_link(d)) for d in valuey]
    return await asyncio.gather(*tasks)


async def main():
    if os.path.isdir('weather_files'):
        shutil.rmtree('weather_files')
    os.mkdir('weather_files')

    async with aiohttp.ClientSession() as session:
        async with session.get("http://www.weather.gov.sg/climate-historical-daily/",
                               timeout=timeout_parameter) as response:
            html_content = await response.read()
            if response.status != 200:
                raise

    soup = BeautifulSoup(html_content, 'html.parser')
    element = soup.find(id="cityname")
    sibling = element.find_next_sibling('ul')
    data_dict = {x.find(onclick=True)['onclick'][9:-2]: x.a.string for x in sibling.find_all('li')}

    tasks = [asyncio.create_task(get_years_then_months_then_csv_link(d)) for d in data_dict.items()]
    results = await asyncio.gather(*tasks)
    with open('log.txt', 'w') as f:
        f.write(str(results))


if __name__ == '__main__':
    start = time()
    asyncio.run(main())
    print(f'Downloaded Data in {time() - start} s')
    start2 = time()
    conn = duckdb.connect(':memory:')
    # Merge and sort the CSV files using a single SQL statement
    conn.execute("""
        create table weather_sg as 
        SELECT  Station,
                CAST(Year as INTEGER) as Year,
                CAST(Month as INTEGER) as Month,
                CAST(Day as INTEGER) as Day,
                replace("Daily Rainfall Total (mm)",' ','') as "Daily Rainfall Total (mm)",
                replace("Highest 30 min Rainfall (mm)",' ','') as "Highest 30 min Rainfall (mm)",
                replace("Highest 60 min Rainfall (mm)",' ','') as "Highest 60 min Rainfall (mm)",
                replace("Highest 120 min Rainfall (mm)",' ','') as "Highest 120 min Rainfall (mm)",
                replace("Mean Temperature (°C)",' ','') as "Mean Temperature (°C)",
                replace("Maximum Temperature (°C)",' ','') as "Maximum Temperature (°C)",
                replace("Minimum Temperature (°C)",' ','') as "Minimum Temperature (°C)",
                replace("Mean Wind Speed (km/h)",' ','') as "Mean Wind Speed (km/h)",
                replace("Max Wind Speed (km/h)",' ','') as "Max Wind Speed (km/h)"
        FROM
        READ_CSV(
            'weather_files/*.csv',
            skip=1,
            header=False,
            sep=',',
            QUOTE='"',
            escape='\\',
            columns={
                'Station':'VARCHAR',
                'Year':'INTEGER',
                'Month':'INTEGER',
                'Day':'INTEGER',
                'Daily Rainfall Total (mm)':'VARCHAR',
                'Highest 30 min Rainfall (mm)':'VARCHAR',
                'Highest 60 min Rainfall (mm)':'VARCHAR',
                'Highest 120 min Rainfall (mm)':'VARCHAR',
                'Mean Temperature (°C)':'VARCHAR',
                'Maximum Temperature (°C)':'VARCHAR',
                'Minimum Temperature (°C)':'VARCHAR',
                'Mean Wind Speed (km/h)':'VARCHAR',
                'Max Wind Speed (km/h)':'VARCHAR'
            }
        )
        order by Station asc,Year desc, Month desc, Day desc
       ;
    """)
    conn.execute("""COPY weather_sg TO 'output.csv' WITH (HEADER 1);""")
    conn.close()
    print(f'Created CSV in Data in {time() - start2} s')
    print('end')
