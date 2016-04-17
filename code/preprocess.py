from __future__ import division

import glob
import multiprocessing
import time
import urllib
import urllib2

import lxml.html
import numpy as np
import pandas as pd


def make_wbans():
    '''
    A mapping of FAA callsign to wbam number
    '''
    with open('../data/wbanmasterlist.html') as infile:
        doc = lxml.html.fromstring(infile.read())

    data = []
    wbans = {}
    names = {}

    rows = doc.cssselect('tr')
    for row in rows:
        wban = row.cssselect('td')[1].text_content()
        call_sign = row.cssselect('td')[7].text_content()
        name = row.cssselect('td')[2].text_content()
        if wban and call_sign:
            data.append((wban, call_sign, name))
            wbans[call_sign] = wban
            names[call_sign] = name

    return wbans


def process_filename(filename):
    try:
        # filename = url.split('/')[-1]
        year = filename.split('-')[-1].split('.')[0]
        url = 'http://www1.ncdc.noaa.gov/pub/data/noaa/isd-lite/{}/{}'.format(year, filename)
        local_filename = filename.split('-', 1)[-1]
        urllib.urlretrieve(url, '../data/noaa-isd-lite/' + year + '/' + local_filename)
        print filename
    except Exception as e:
        print e


def download_weather_data():
    '''
    Downloads noaa-isd-lite data for the required years,
    only for airport weather stations
    '''

    filenames = []
    years = range(1987, 2009)
    years = ['{}.gz'.format(y) for y in years]

    wbans = []
    with open('../data/STATION-LIST-CDMP.txt') as infile:
        lines = infile.readlines()
    lines = lines[11:]
    for line in lines:
        wban = line[7:12]
        wbans.append(wban)

    with open('../data/1st_order_wbans.txt') as infile:
        lines = infile.readlines()
    lines = lines[8:]
    wbans += [line[69:74] for line in lines if line.strip()]

    with open('../data/seen_wbans.txt') as infile:
        lines = infile.readlines()
    wbans += [line.strip() for line in lines]

    with open('../data/wbanmasterlist/wbanmasterlist.html') as infile:
        doc = lxml.html.fromstring(infile.read())
    rows = doc.cssselect('tr')
    for row in rows:
        wban = row.cssselect('td')[1].text_content().strip()
        name = row.cssselect('td')[2].text_content().strip()
        if wban and name:
            if 'airport' in name.lower() or name.lower().endswith('ap'):
                wbans.append(wban)

    wbans = set(wbans)
    print len(wbans)

    # Now get the actual filenames
    with open('../data/_pub_data_noaa_isd-lite_2007.html') as infile:
        doc = lxml.html.fromstring(infile.read())

    rows = doc.cssselect('tr')
    cells = doc.cssselect('td a')
    all_filenames = [c.text_content() for c in cells]

    print all_filenames[-1]
    for filename in all_filenames:
        wban = filename.split('-')[1]
        if wban in wbans:
            x = [filename.replace('2007.gz', y) for y in years]
            filenames += x

    pool = multiprocessing.Pool(processes=10)
    pool.map(process_filename, filenames)



def make_tz_offsets():
    '''
    Mapping of iata timezone name to integer hours offset,
    ignoring summer time
    '''
    with open('../data/tz-wikipedia.html') as infile:
        doc = lxml.html.fromstring(infile.read())

    rows = doc.cssselect('tbody tr')

    tz_offsets = {}

    for row in rows:
        name = row.cssselect('td')[2].text_content()
        offset = row.cssselect('td')[4].text_content()
        assert ':' in offset
        offset = offset.split(':')[0]
        offset = int(offset)
        tz_offsets[name] = offset

    return tz_offsets


def make_aiport_tzs():
    '''
    Make a dict mapping airport FAA callsign to iata timezone name
    '''

    airpot_tzs = {}

    with open('../data/iata.tzmap.txt') as infile:
        lines = infile.readlines()

    for line in lines:
        code = line.split('\t', 1)[0].strip()
        tz = line.split('\t', 1)[1].strip()
        airpot_tzs[code] = tz

    return airpot_tzs


def make_weathers(year):
    '''
    Requires weather data downloaded with download_weather_data()

    Mapping of WBAN_ YYYY MM DD HH to string of weather data
    '''
    glob_ = '../data/noaa-isd-lite/{}/*'.format(year)
    filenames = glob.glob(glob_)
    weathers = {}
    for filename in filenames:
        with open(filename) as infile:
            lines = infile.readlines()

        wban = filename.split('-')[-2]
        new_weathers = {wban + ' ' + line[:13]:line[13:].rstrip() for line in lines}
        weathers.update(new_weathers)

    return weathers


def print_progress(msg, start_time):
    '''
    Util to print a timestamped progress message
    '''
    msg_time = '{:.1f}'.format(time.time() - start_time).rjust(4)
    print msg_time, msg


def process_asa_file(asa_filename):
    '''
    Preprocess a single file of asa flight data
    - Removes unwanted columns and rows
    - Adds weather data for origin and desnitaiton airports
    - Some NA handling

    Requires the asa files to be downloaded from 
    http://stat-computing.org/dataexpo/2009/the-data.html
    into ../data/asa-flight-data/

    '''

    start_time = time.time()
    print_progress(asa_filename, start_time)
    print_progress('Starting', start_time)

    df = pd.read_csv('../data/asa-flight-data/' + asa_filename, engine='c')
    print_progress('Loaded data', start_time)

    cols_to_drop = [
        'TailNum',
        'FlightNum',
        'ArrTime',
        'DepTime',
        'CRSElapsedTime',
        'TaxiIn',
        'Distance',
        'TaxiOut',
        'CarrierDelay',
        'WeatherDelay',
        'NASDelay',
        'SecurityDelay',
        'LateAircraftDelay',
        'CancellationCode',
    ]
    df.drop(cols_to_drop, axis=1, inplace=True)
    print_progress('Dropped unwanted cols', start_time)

    # Restrict to single airport
    df = df[(df.Origin == 'ORD')]
    print_progress('Dropped unwanted rows', start_time)

    # y feature
    df['IsDepDelay'] = df['DepDelay'] > 15
    df['IsArrDelay'] = df['ArrDelay'] > 15
    df.drop(['DepDelay', 'ArrDelay'], axis=1, inplace=True)
    print_progress('Built y', start_time)
    

    # Remove bad flights
    df = df[df.Cancelled != 1]
    df = df[df.Diverted != 1]
    df.drop(['Diverted', 'Cancelled'], axis=1, inplace=True)
    print_progress('Removed bad flights', start_time)

    # Convert times to hours (data is provided in hhmm format, just want hh)
    df['DepHourLocal'] = np.round((df.CRSDepTime + df.CRSDepTime.mod(100) * 2/3) / 100).map(int)
    df['ArrHourLocal'] = np.round((df.CRSArrTime + df.CRSArrTime.mod(100) * 2/3) / 100).map(int)
    df.drop(['CRSDepTime', 'CRSArrTime'], axis=1, inplace=True)
    print_progress('Rounded times to hours', start_time)

    # Load conversion offsets
    airpot_tzs = make_aiport_tzs()
    tz_offsets = make_tz_offsets()
    offsets = {k:tz_offsets[v] for k, v in airpot_tzs.iteritems()}
    del airpot_tzs
    tz_offsets = None
    print_progress('Loaded tz data', start_time)
    def rep_offsets(x):
        return offsets.get(x)

    # Do conversion
    # Just a best effor approach to avoid loading a time library for each line
    # 1/24 times will be off by 1 hour, as days aren't wrapped
    # Also, daylight savings time is ignored, so 1/2 of times will be out by a futher hour
    origin_offsets = df['Origin'].map(rep_offsets)
    hours = df['DepHourLocal'] - origin_offsets
    hours[hours < 0] = 0
    hours[hours >= 24] = 23
    df['DepHourUtc'] = hours
    del hours
    del origin_offsets
    print_progress('Converted departures to utc', start_time)
    dest_offsets = df['Dest'].map(rep_offsets)
    hours = df['ArrHourLocal'] - dest_offsets
    hours[hours < 0] = 0
    hours[hours >= 24] = 23
    df['ArrHourUtc'] = hours
    del hours
    del dest_offsets
    offsets = None
    print_progress('Converted arrivals to utc', start_time)

    # Convert airport callsigns to wban numbers
    wbans = make_wbans()
    def wban_rep(x):
        return wbans.get(x, x)
    df['OriginWban'] = df['Origin'].map(wban_rep)
    df['DestWban'] = df['Dest'].map(wban_rep)
    wbans = None
    print_progress('Loaded wban data', start_time)

    # Build keys to look up weather data
    def pad(x):
        return x.rjust(2, '0')
    df['OriginWeatherKey'] = df['OriginWban'] + ' ' + df['Year'].map(str) + ' ' + df['Month'].map(str).map(pad) + ' ' + df['DayofMonth'].map(str).map(pad) + ' ' + df['DepHourUtc'].map(str).map(pad)
    df.drop(['OriginWban', 'DepHourUtc'], axis=1, inplace=True)
    print_progress('Built origin keys', start_time)
    df['DestWeatherKey'] = df['DestWban'] + ' ' + df['Year'].map(str) + ' ' + df['Month'].map(str).map(pad) + ' ' + df['DayofMonth'].map(str).map(pad) + ' ' + df['ArrHourUtc'].map(str).map(pad)
    df.drop(['DestWban','ArrHourUtc', 'DayofMonth'], axis=1, inplace=True)
    print_progress('Built dest keys', start_time)

    # Load weather data for the given year
    year = asa_filename.split('_')[0].split('.')[0]
    weathers = make_weathers(year)
    def rep(x):
        return weathers.get(x, '')
    print_progress('Loaded weather data', start_time)


    df['OriginWeather'] = df['OriginWeatherKey'].map(rep)
    df.drop(['OriginWeatherKey'], axis=1, inplace=True)
    print_progress('Loaded origin weathers', start_time)
    df['DestWeather'] = df['DestWeatherKey'].map(rep)
    df.drop(['DestWeatherKey'], axis=1, inplace=True)
    print_progress('Loaded dest weathers', start_time)

    df = df[df.OriginWeather != '']
    df = df[df.DestWeather != '']
    weathers = None

    # Parse weather
    def slice_(from_, to):
        def inner(x):
            return x[from_:to].strip().replace('-9999', '')
        return inner
    df['OriginAirTemp'] = df['OriginWeather'].map(slice_(2, 6))
    df['OriginAirPressure'] = df['OriginWeather'].map(slice_(14, 18))
    df['OriginWindSpeed'] = df['OriginWeather'].map(slice_(24, 30))
    df['OriginSkyCoverage'] = df['OriginWeather'].map(slice_(30, 36))
    df['OriginPrecip1'] = df['OriginWeather'].map(slice_(36, 42)).map(lambda x: x.replace('-1', '0'))

    df['DestAirTemp'] = df['DestWeather'].map(slice_(2, 6))
    df['DestAirPressure'] = df['DestWeather'].map(slice_(14, 18))
    df['DestWindSpeed'] = df['DestWeather'].map(slice_(24, 30))
    df['DestSkyCoverage'] = df['DestWeather'].map(slice_(30, 36))
    df['DestPrecip1'] = df['DestWeather'].map(slice_(36, 42)).map(lambda x: x.replace('-1', '0'))
    df.drop(['OriginWeather', 'DestWeather', 'Origin', 'Dest'], axis=1, inplace=True)
    print_progress('Parsed weather', start_time)
    print df.head()

    # Save data
    df.to_csv('./asa-flight-data/' + asa_filename.replace('.csv', '_processed.csv'), index=False)
    del df
    print_progress('Saved file', start_time)


def main():
    for year in xrange(1987, 2009):
        filename = '{}.csv'.format(year)
        try:
            process_asa_file(filename)
        except Exception as e:
            print e

if __name__ == '__main__':
    main()
