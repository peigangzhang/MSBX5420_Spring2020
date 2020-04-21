import datetime
import ftplib
import gzip
import io

from pyspark.sql import SparkSession


def get_filenames(ftp_server_url, folder_name):
    """
    get filename list from ftp server
    :param ftp_server_url:
    :param folder_name:
    :return:
    """
    with ftplib.FTP(ftp_server_url) as ftp:
        try:
            ftp.login()
            ftp.cwd(folder_name)

            filenames = ftp.nlst()  # get filenames within the directory
            return filenames

        except ftplib.all_errors as e:
            print('FTP error:', e)


def download_file_to_local(ftp_server_url, folder_name, filename, local_filename):
    """
    download file from ftp server to local
    :param ftp_server_url:
    :param folder_name:
    :param filename:
    :param local_filename:
    :return:
    """
    with ftplib.FTP(ftp_server_url) as ftp:
        try:
            ftp.login()
            ftp.cwd(folder_name)
            with open(local_filename, 'wb') as file:
                ftp.retrbinary('RETR ' + filename, file.write)
                return local_filename

        except ftplib.all_errors as e:
            print('FTP error:', e)
            return 'error'


def extract_gzfile_to_lines(ftp_server_url, folder_name, filename):
    """
    extract .gz file from ftp to line list
    :param ftp_server_url:
    :param folder_name:
    :param filename:
    :return:
    """
    if '.gz' not in filename:
        return [None]

    with ftplib.FTP(ftp_server_url) as ftp:
        try:
            ftp.login()
            ftp.cwd(folder_name)
            bytesIO = io.BytesIO()
            ftp.retrbinary('RETR ' + filename, bytesIO.write)
            decompressed_bytes = gzip.decompress(bytesIO.getvalue())
            return decompressed_bytes.decode("utf-8").split('\n')[1:]

        except ftplib.all_errors as e:
            print('FTP error:', e)
            return [None]


def parse_weather_record(line):
    """
    Parse one line of the weather record
    The parsing is done according to
    ftp://ftp.ncdc.noaa.gov/pub/data/gsod/readme.txt
    """
    try:
        field_names = ('date',
                       'temp', '_',
                       'dew_point', '_',
                       'sea_level_pressure', '_',
                       'station_pressure', '_',
                       'visibility', '_',
                       'wind_speed', '_',
                       'max_wind_speed',
                       'max_wind_gust',
                       'max_temp',  # has *
                       'min_temp',  # has *
                       'precipitation',  # has flags A-I
                       'snow_depth',
                       'indicators')
        # list of fields which values have to be converted to floats
        float_field_names = (
            'temp', 'dew_point', 'sea_level_pressure', 'station_pressure',
            'visibility', 'wind_speed', 'max_wind_speed', 'max_wind_gust',
            'max_temp', 'min_temp', 'precipitation', 'snow_depth',
        )

        # base conversion
        fields = line.strip().split()[2:]
        fields = [field.strip() or None for field in fields]
        obj = dict(zip(field_names, fields))
        # obj['date'] = str_to_datetime(obj['date'])
        # float conversion
        for field_name in float_field_names:
            value = obj[field_name].rstrip('*ABCDEFGHI')
            if value in ('9999.9', '99.99', '999.9'):
                value = None
            else:
                value = float(value)
            obj[field_name] = value
        # measurement units conversion
        temp_field_names = ('temp', 'max_temp', 'min_temp')
        for field_name in temp_field_names:
            celsius_field_name = field_name + '_c'
            value = obj[field_name]
            if value is not None:
                obj[celsius_field_name] = (value - 32) * 5 / 9
            else:
                obj[celsius_field_name] = None
        # parsing indicator values
        indicator_names = ('fog', 'rain', 'snow', 'hail', 'thunder', 'tornado')
        indicator_values = [flag == '1' for flag in obj['indicators']]
        indicator_obj = dict(zip(indicator_names, indicator_values))
        obj['weather_ok'] = not any(indicator_values)
        obj.update(indicator_obj)
        # finalization
        del obj['_']

        new_obj = {}
        for k, v in obj.items():
            new_obj[k] = str(v)
        return new_obj
    except Exception as ex:
        pass

    return None


def str_to_datetime(val):
    return val and datetime.datetime.strptime(val, '%Y%m%d')


def convert_location(val):
    """Remove plus sign from lat/lon value if present"""
    if val:
        if val[0] == '+':
            return val.lstrip('+')
        return val


spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

ftp_server_url = 'ftp.ncdc.noaa.gov'
folder_name = '/pub/data/gsod/2020/'
result_path = 's3://msbx5420-2020/peterzhang/weather/weather.parquet'

filenames = get_filenames(ftp_server_url, folder_name)

filenames_rdd = sc.parallelize(filenames)

decompressed_data_rdd = filenames_rdd.flatMap(
    lambda filename: extract_gzfile_to_lines(ftp_server_url, folder_name, filename))

parsed_data_rdd = decompressed_data_rdd.map(lambda line: parse_weather_record(line)).filter(
    lambda record: record is not None)

first_record = parsed_data_rdd.take(1)[0]

data_df = parsed_data_rdd.map(lambda x: tuple(x.values())).toDF(list(first_record.keys()))
data_df.write.parquet(result_path)
