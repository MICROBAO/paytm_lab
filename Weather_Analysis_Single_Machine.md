```python
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, FloatType, DoubleType

```


```python
spark = SparkSession \
    .builder \
    .appName("paytm_weather_analysis") \
    .getOrCreate()
```

    Setting default log level to "WARN".
    To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
    23/09/25 06:27:57 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable



```python
# Replace the paths with the right ones to make codes running on other machines
weather_data_path='/Users/dongzheli/Desktop/paytmteam_weather/data/2019/'
station_list_path='/Users/dongzheli/Desktop/paytmteam_weather/stationlist.csv'
country_list_path='/Users/dongzheli/Desktop/paytmteam_weather/countrylist.csv'

```


```python
#predefine schema for large dataset ingestion
weather_schema = StructType([StructField('STN---', IntegerType(), True),
                 StructField('WBAN', IntegerType(), True),
                 StructField('YEARMODA', IntegerType(), True),
                 StructField('TEMP', DoubleType(), True),                  
                 StructField('DEWP', DoubleType(), True),      
                 StructField('SLP', DoubleType(), True),
                 StructField('STP', DoubleType(), True),
                 StructField('VISIB', DoubleType(), True),
                 StructField('WDSP', DoubleType(), True),
                 StructField('MXSPD', DoubleType(), True),       
                 StructField('GUST', DoubleType(), True),       
                 StructField('MAX', DoubleType(), True),       
                 StructField('MIN', DoubleType(), True),                 
                 StructField('PRCP', DoubleType(), True),       
                 StructField('SNDP', DoubleType(), True),       
                 StructField('FRSHTT', StringType(), True)])
```


```python
weather_df = spark.read.csv(weather_data_path, header=True, schema=weather_schema)
weather_df.show(truncate=False)
```

    +------+-----+--------+----+----+------+------+-----+----+-----+-----+----+----+----+-----+------+
    |STN---|WBAN |YEARMODA|TEMP|DEWP|SLP   |STP   |VISIB|WDSP|MXSPD|GUST |MAX |MIN |PRCP|SNDP |FRSHTT|
    +------+-----+--------+----+----+------+------+-----+----+-----+-----+----+----+----+-----+------+
    |10260 |99999|20190101|26.1|21.2|1001.9|987.5 |20.6 |9.0 |15.9 |29.7 |29.8|NULL|NULL|18.5 |001000|
    |10260 |99999|20190102|24.9|22.1|1020.1|1005.5|5.4  |5.6 |13.6 |22.1 |NULL|20.7|NULL|22.8 |001000|
    |10260 |99999|20190103|31.7|29.1|1008.9|994.7 |13.6 |11.6|21.4 |49.5 |NULL|NULL|NULL|999.9|011000|
    |10260 |99999|20190104|32.9|30.3|1011.4|997.1 |15.8 |4.9 |7.8  |10.9 |36.1|31.8|NULL|999.9|001000|
    |10260 |99999|20190105|35.5|33.0|1015.7|1001.4|12.0 |10.4|13.6 |21.0 |NULL|32.7|NULL|23.6 |010000|
    |10260 |99999|20190106|38.5|34.1|1008.2|994.2 |12.8 |10.0|17.5 |28.9 |41.4|NULL|NULL|23.2 |010000|
    |10260 |99999|20190107|32.1|29.8|996.8 |982.7 |6.9  |11.3|15.5 |28.6 |NULL|30.4|NULL|999.9|001000|
    |10260 |99999|20190108|31.6|28.0|997.4 |983.3 |22.9 |5.9 |11.7 |19.0 |34.3|NULL|NULL|0.4  |011000|
    |10260 |99999|20190109|29.9|27.7|1011.6|997.3 |29.8 |7.6 |15.2 |26.6 |32.4|26.1|NULL|23.6 |001000|
    |10260 |99999|20190110|33.1|30.6|979.1 |965.3 |5.3  |17.8|24.9 |41.8 |41.4|NULL|NULL|999.9|011000|
    |10260 |99999|20190111|31.2|29.0|975.0 |961.1 |5.6  |11.6|17.5 |38.9 |NULL|NULL|NULL|0.4  |011100|
    |10260 |99999|20190112|28.3|26.1|988.2 |974.1 |8.2  |8.1 |13.6 |38.5 |NULL|NULL|NULL|999.9|001000|
    |10260 |99999|20190113|22.7|20.9|977.1 |963.0 |26.6 |4.1 |7.8  |15.2 |27.7|NULL|NULL|0.4  |001000|
    |10260 |99999|20190114|20.0|18.3|984.3 |970.0 |43.1 |3.6 |9.7  |10.7 |NULL|15.4|NULL|38.6 |000000|
    |10260 |99999|20190115|25.9|23.2|991.3 |977.1 |16.0 |7.4 |13.8 |20.8 |27.3|19.2|NULL|999.9|001000|
    |10260 |99999|20190116|24.8|21.8|992.5 |978.2 |33.4 |2.7 |5.8  |999.9|26.1|23.5|NULL|35.4 |001000|
    |10260 |99999|20190117|21.4|19.0|989.8 |975.5 |10.4 |6.1 |8.9  |13.4 |NULL|NULL|NULL|35.0 |001000|
    |10260 |99999|20190118|21.0|19.1|994.4 |980.0 |13.8 |5.6 |7.8  |12.8 |22.3|19.2|NULL|35.0 |001000|
    |10260 |99999|20190119|20.2|18.5|1000.8|986.3 |33.9 |3.4 |7.8  |10.3 |NULL|NULL|NULL|35.8 |001000|
    |10260 |99999|20190120|21.7|18.5|1009.0|994.4 |32.1 |9.5 |14.6 |20.4 |NULL|18.7|NULL|999.9|001000|
    +------+-----+--------+----+----+------+------+-----+----+-----+-----+----+----+----+-----+------+
    only showing top 20 rows
    



```python
station_schema = StructType(
                    [StructField('STN_NO', IntegerType(), True),
                     StructField('COUNTRY_ABBR', StringType(), True)]
                    )
station_df = spark.read.csv(station_list_path, header=True, schema=station_schema)
station_df.show(truncate=False)
```

    +------+------------+
    |STN_NO|COUNTRY_ABBR|
    +------+------------+
    |12240 |NO          |
    |20690 |SW          |
    |20870 |SW          |
    |21190 |SW          |
    |32690 |UK          |
    |33450 |UK          |
    |39290 |UK          |
    |39790 |EI          |
    |40480 |IC          |
    |41300 |IC          |
    |60100 |FO          |
    |61443 |DA          |
    |63401 |NL          |
    |71910 |FR          |
    |92640 |GM          |
    |123766|PL          |
    |125990|PL          |
    |129700|HU          |
    |132240|HR          |
    |156500|BU          |
    +------+------------+
    only showing top 20 rows
    



```python
country_schema = StructType(
                    [StructField('COUNTRY_ABBR', StringType(), True),
                     StructField('COUNTRY_FULL', StringType(), True)]
                    )
country_df = spark.read.csv(country_list_path, header=True, schema=country_schema)
country_df.show(truncate=False)
```

    +------------+---------------------------+
    |COUNTRY_ABBR|COUNTRY_FULL               |
    +------------+---------------------------+
    |AA          |ARUBA                      |
    |AC          |ANTIGUA AND BARBUDA        |
    |AF          |AFGHANISTAN                |
    |AG          |ALGERIA                    |
    |AI          |ASCENSION ISLAND           |
    |AJ          |AZERBAIJAN                 |
    |AL          |ALBANIA                    |
    |AM          |ARMENIA                    |
    |AN          |ANDORRA                    |
    |AO          |ANGOLA                     |
    |AQ          |AMERICAN SAMOA             |
    |AR          |ARGENTINA                  |
    |AS          |AUSTRALIA                  |
    |AT          |ASHMORE AND CARTIER ISLANDS|
    |AU          |AUSTRIA                    |
    |AV          |ANGUILLA                   |
    |AX          |ANTIGUA                    |
    |AY          |ANTARCTICA                 |
    |AZ          |AZORES                     |
    |BA          |BAHRAIN                    |
    +------------+---------------------------+
    only showing top 20 rows
    



```python
# Cleaning up and validation

from pyspark.sql.functions import col,isnan, when, count, to_date
country_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in country_df.columns]
   ).show()
#No nulls in country_df
country_df.groupBy("COUNTRY_FULL").count().where("count > 1").show()
```

    +------------+------------+
    |COUNTRY_ABBR|COUNTRY_FULL|
    +------------+------------+
    |           0|           0|
    +------------+------------+
    
    +------------+-----+
    |COUNTRY_FULL|count|
    +------------+-----+
    |       KOREA|    2|
    +------------+-----+
    



```python
station_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in station_df.columns]
   ).show()
```

    +------+------------+
    |STN_NO|COUNTRY_ABBR|
    +------+------------+
    |    40|           0|
    +------+------------+
    



```python
station_df_cleaned = station_df.na.drop(subset=["STN_NO"])
station_df_cleaned.count()
# 25266 removed 40 nulls where STN_NO is NULL
```




    25266




```python
# Clean up weather_df
weather_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in weather_df.columns]
   ).show()

```

    [Stage 15:==============================================>           (4 + 1) / 5]

    +------+----+--------+----+----+---+---+-----+----+-----+----+-------+-------+-------+----+------+
    |STN---|WBAN|YEARMODA|TEMP|DEWP|SLP|STP|VISIB|WDSP|MXSPD|GUST|    MAX|    MIN|   PRCP|SNDP|FRSHTT|
    +------+----+--------+----+----+---+---+-----+----+-----+----+-------+-------+-------+----+------+
    |     0|   0|       0|   0|   0|  0|  0|    0|   0|    0|   0|1814196|1723358|3397649|   0|     0|
    +------+----+--------+----+----+---+---+-----+----+-----+----+-------+-------+-------+----+------+
    


                                                                                    


```python
from pyspark.sql.functions import max
weather_df.select(
                    max(weather_df.TEMP).alias("TEMP_MAX"), 
                    max(weather_df.DEWP).alias("DEWP_MAX"),
                    max(weather_df.SLP).alias("SLP_MAX"),
                    max(weather_df.STP).alias("STP_MAX"),
                    max(weather_df.VISIB).alias("VISIB_MAX"),
                    max(weather_df.WDSP).alias("WDSP_MAX"),
                    max(weather_df.MXSPD).alias("MXSPD_MAX"),
                    max(weather_df.GUST).alias("GUST_MAX"),
                    max(weather_df.MAX).alias("MAX_MAX"),
                    max(weather_df.MIN).alias("MIN_MAX"),
                    max(weather_df.PRCP).alias("PRCP_MAX"),
                    max(weather_df.SNDP).alias("SNDP_MAX"),
                ).show()
```

    [Stage 18:==================================>                       (3 + 2) / 5]

    +--------+--------+-------+-------+---------+--------+---------+--------+-------+-------+--------+--------+
    |TEMP_MAX|DEWP_MAX|SLP_MAX|STP_MAX|VISIB_MAX|WDSP_MAX|MXSPD_MAX|GUST_MAX|MAX_MAX|MIN_MAX|PRCP_MAX|SNDP_MAX|
    +--------+--------+-------+-------+---------+--------+---------+--------+-------+-------+--------+--------+
    |   110.0|  9999.9| 9999.9| 9999.9|    999.9|   999.9|    999.9|   999.9| 9999.9| 9999.9|   99.99|   999.9|
    +--------+--------+-------+-------+---------+--------+---------+--------+-------+-------+--------+--------+
    


                                                                                    


```python
# Replace missing values with null
# Rename STN--- to STN_NO
# Convert YEARMODA from string to date
weather_df_fillnull = weather_df.withColumnRenamed("STN---", "STN_NO") \
            .withColumn("YEARMODA", to_date(weather_df.YEARMODA, 'yyyyMMdd')) \
            .withColumn("TEMP", when(weather_df.TEMP == 9999.9, None).otherwise(weather_df.TEMP)) \
            .withColumn("DEWP", when(weather_df.DEWP == 9999.9, None).otherwise(weather_df.DEWP)) \
            .withColumn("SLP", when(weather_df.SLP == 9999.9, None).otherwise(weather_df.SLP)) \
            .withColumn("STP", when(weather_df.STP == 9999.9, None).otherwise(weather_df.STP)) \
            .withColumn("VISIB", when(weather_df.VISIB == 999.9, None).otherwise(weather_df.VISIB)) \
            .withColumn("WDSP", when(weather_df.WDSP == 999.9, None).otherwise(weather_df.WDSP)) \
            .withColumn("MXSPD", when(weather_df.MXSPD == 999.9, None).otherwise(weather_df.MXSPD)) \
            .withColumn("GUST", when(weather_df.GUST == 999.9, None).otherwise(weather_df.GUST)) \
            .withColumn("MAX", when(weather_df.MAX == 9999.9, None).otherwise(weather_df.MAX)) \
            .withColumn("MIN", when(weather_df.MIN == 9999.9, None).otherwise(weather_df.MIN)) \
            .withColumn("PRCP", when(weather_df.PRCP == 99.99, None).otherwise(weather_df.PRCP)) \
            .withColumn("SNDP", when(weather_df.SNDP == 999.9, None).otherwise(weather_df.SNDP))
```


```python
# Validate if replace works
weather_df_fillnull.select(
                    max(weather_df_fillnull.TEMP).alias("TEMP_MAX"), 
                    max(weather_df_fillnull.DEWP).alias("DEWP_MAX"),
                    max(weather_df_fillnull.SLP).alias("SLP_MAX"),
                    max(weather_df_fillnull.STP).alias("STP_MAX"),
                    max(weather_df_fillnull.VISIB).alias("VISIB_MAX"),
                    max(weather_df_fillnull.WDSP).alias("WDSP_MAX"),
                    max(weather_df_fillnull.MXSPD).alias("MXSPD_MAX"),
                    max(weather_df_fillnull.GUST).alias("GUST_MAX"),
                    max(weather_df_fillnull.MAX).alias("MAX_MAX"),
                    max(weather_df_fillnull.MIN).alias("MIN_MAX"),
                    max(weather_df_fillnull.PRCP).alias("PRCP_MAX"),
                    max(weather_df_fillnull.SNDP).alias("SNDP_MAX"),
                ).show()
```

    [Stage 24:==================================>                       (3 + 2) / 5]

    +--------+--------+-------+-------+---------+--------+---------+--------+-------+-------+--------+--------+
    |TEMP_MAX|DEWP_MAX|SLP_MAX|STP_MAX|VISIB_MAX|WDSP_MAX|MXSPD_MAX|GUST_MAX|MAX_MAX|MIN_MAX|PRCP_MAX|SNDP_MAX|
    +--------+--------+-------+-------+---------+--------+---------+--------+-------+-------+--------+--------+
    |   110.0|    89.5| 1077.4| 1075.2|     91.7|    78.6|     95.0|   116.6|  129.9|  100.0|   12.56|   117.7|
    +--------+--------+-------+-------+---------+--------+---------+--------+-------+-------+--------+--------+
    


                                                                                    


```python
# Validate if there are values that are negatives
from pyspark.sql.functions import min
weather_df_fillnull.select(
                    min(weather_df_fillnull.TEMP).alias("TEMP_MIN"), 
                    min(weather_df_fillnull.DEWP).alias("DEWP_MIN"),
                    min(weather_df_fillnull.SLP).alias("SLP_MIN"),
                    min(weather_df_fillnull.STP).alias("STP_MIN"),
                    min(weather_df_fillnull.VISIB).alias("VISIB_MIN"),
                    min(weather_df_fillnull.WDSP).alias("WDSP_MIN"),
                    min(weather_df_fillnull.MXSPD).alias("MXSPD_MIN"),
                    min(weather_df_fillnull.GUST).alias("GUST_MIN"),
                    min(weather_df_fillnull.MAX).alias("MAX_MIN"),
                    min(weather_df_fillnull.MIN).alias("MIN_MIN"),
                    min(weather_df_fillnull.PRCP).alias("PRCP_MIN"),
                    min(weather_df_fillnull.SNDP).alias("SNDP_MIN"),
                ).show()
```

    [Stage 27:==============================================>           (4 + 1) / 5]

    +--------+--------+-------+-------+---------+--------+---------+--------+-------+-------+--------+--------+
    |TEMP_MIN|DEWP_MIN|SLP_MIN|STP_MIN|VISIB_MIN|WDSP_MIN|MXSPD_MIN|GUST_MIN|MAX_MIN|MIN_MIN|PRCP_MIN|SNDP_MIN|
    +--------+--------+-------+-------+---------+--------+---------+--------+-------+-------+--------+--------+
    |  -114.7|  -119.1|  923.5|  547.5|      0.0|     0.0|      0.2|     9.7|  -95.4| -116.0|     0.0|     0.4|
    +--------+--------+-------+-------+---------+--------+---------+--------+-------+-------+--------+--------+
    


                                                                                    


```python
# Cleaned up dataframe
weather_df_fillnull.show()
```

    +------+-----+----------+----+----+------+------+-----+----+-----+----+----+----+----+----+------+
    |STN_NO| WBAN|  YEARMODA|TEMP|DEWP|   SLP|   STP|VISIB|WDSP|MXSPD|GUST| MAX| MIN|PRCP|SNDP|FRSHTT|
    +------+-----+----------+----+----+------+------+-----+----+-----+----+----+----+----+----+------+
    | 10260|99999|2019-01-01|26.1|21.2|1001.9| 987.5| 20.6| 9.0| 15.9|29.7|29.8|NULL|NULL|18.5|001000|
    | 10260|99999|2019-01-02|24.9|22.1|1020.1|1005.5|  5.4| 5.6| 13.6|22.1|NULL|20.7|NULL|22.8|001000|
    | 10260|99999|2019-01-03|31.7|29.1|1008.9| 994.7| 13.6|11.6| 21.4|49.5|NULL|NULL|NULL|NULL|011000|
    | 10260|99999|2019-01-04|32.9|30.3|1011.4| 997.1| 15.8| 4.9|  7.8|10.9|36.1|31.8|NULL|NULL|001000|
    | 10260|99999|2019-01-05|35.5|33.0|1015.7|1001.4| 12.0|10.4| 13.6|21.0|NULL|32.7|NULL|23.6|010000|
    | 10260|99999|2019-01-06|38.5|34.1|1008.2| 994.2| 12.8|10.0| 17.5|28.9|41.4|NULL|NULL|23.2|010000|
    | 10260|99999|2019-01-07|32.1|29.8| 996.8| 982.7|  6.9|11.3| 15.5|28.6|NULL|30.4|NULL|NULL|001000|
    | 10260|99999|2019-01-08|31.6|28.0| 997.4| 983.3| 22.9| 5.9| 11.7|19.0|34.3|NULL|NULL| 0.4|011000|
    | 10260|99999|2019-01-09|29.9|27.7|1011.6| 997.3| 29.8| 7.6| 15.2|26.6|32.4|26.1|NULL|23.6|001000|
    | 10260|99999|2019-01-10|33.1|30.6| 979.1| 965.3|  5.3|17.8| 24.9|41.8|41.4|NULL|NULL|NULL|011000|
    | 10260|99999|2019-01-11|31.2|29.0| 975.0| 961.1|  5.6|11.6| 17.5|38.9|NULL|NULL|NULL| 0.4|011100|
    | 10260|99999|2019-01-12|28.3|26.1| 988.2| 974.1|  8.2| 8.1| 13.6|38.5|NULL|NULL|NULL|NULL|001000|
    | 10260|99999|2019-01-13|22.7|20.9| 977.1| 963.0| 26.6| 4.1|  7.8|15.2|27.7|NULL|NULL| 0.4|001000|
    | 10260|99999|2019-01-14|20.0|18.3| 984.3| 970.0| 43.1| 3.6|  9.7|10.7|NULL|15.4|NULL|38.6|000000|
    | 10260|99999|2019-01-15|25.9|23.2| 991.3| 977.1| 16.0| 7.4| 13.8|20.8|27.3|19.2|NULL|NULL|001000|
    | 10260|99999|2019-01-16|24.8|21.8| 992.5| 978.2| 33.4| 2.7|  5.8|NULL|26.1|23.5|NULL|35.4|001000|
    | 10260|99999|2019-01-17|21.4|19.0| 989.8| 975.5| 10.4| 6.1|  8.9|13.4|NULL|NULL|NULL|35.0|001000|
    | 10260|99999|2019-01-18|21.0|19.1| 994.4| 980.0| 13.8| 5.6|  7.8|12.8|22.3|19.2|NULL|35.0|001000|
    | 10260|99999|2019-01-19|20.2|18.5|1000.8| 986.3| 33.9| 3.4|  7.8|10.3|NULL|NULL|NULL|35.8|001000|
    | 10260|99999|2019-01-20|21.7|18.5|1009.0| 994.4| 32.1| 9.5| 14.6|20.4|NULL|18.7|NULL|NULL|001000|
    +------+-----+----------+----+----+------+------+-----+----+-----+----+----+----+----+----+------+
    only showing top 20 rows
    



```python
# Joining station with country to get full country name
station_with_country = station_df_cleaned.join(country_df, station_df_cleaned.COUNTRY_ABBR == country_df.COUNTRY_ABBR, "left").drop(country_df.COUNTRY_ABBR)
station_with_country.show()
```

    +------+------------+--------------+
    |STN_NO|COUNTRY_ABBR|  COUNTRY_FULL|
    +------+------------+--------------+
    | 12240|          NO|        NORWAY|
    | 20690|          SW|        SWEDEN|
    | 20870|          SW|        SWEDEN|
    | 21190|          SW|        SWEDEN|
    | 32690|          UK|UNITED KINGDOM|
    | 33450|          UK|UNITED KINGDOM|
    | 39290|          UK|UNITED KINGDOM|
    | 39790|          EI|       IRELAND|
    | 40480|          IC|       ICELAND|
    | 41300|          IC|       ICELAND|
    | 60100|          FO| FAROE ISLANDS|
    | 61443|          DA|       DENMARK|
    | 63401|          NL|   NETHERLANDS|
    | 71910|          FR|        FRANCE|
    | 92640|          GM|       GERMANY|
    |123766|          PL|        POLAND|
    |125990|          PL|        POLAND|
    |129700|          HU|       HUNGARY|
    |132240|          HR|       CROATIA|
    |156500|          BU|      BULGARIA|
    +------+------------+--------------+
    only showing top 20 rows
    



```python
weather_with_country = weather_df_fillnull.join(station_with_country, weather_df_fillnull.STN_NO ==  station_with_country.STN_NO, "left").drop(station_with_country.STN_NO)
weather_with_country.createOrReplaceTempView("weather_with_country")
```


```python
# 1. Which country had the hottest average mean temperature over the year?
spark.sql("""
SELECT
    SUM(TEMP)/COUNT(YEARMODA) as ave_mean_tmp,
    COUNTRY_ABBR,
    COUNTRY_FULL
FROM weather_with_country
WHERE year(YEARMODA) = 2019 and COUNTRY_FULL IS NOT NULL
GROUP BY COUNTRY_ABBR, COUNTRY_FULL
ORDER BY ave_mean_tmp DESC
""").head()
# DJIBOUTI has the hottest mean temperate over 2019 with 90.06114457831325
```

                                                                                    




    Row(ave_mean_tmp=90.06114457831325, COUNTRY_ABBR='DJ', COUNTRY_FULL='DJIBOUTI')




```python
#2. Which country had the most consecutive days of tornadoes/funnel cloud formations?
spark.sql("""
WITH IS_TORNADO_OR_FUNNEL_CTE as ( 
    SELECT  
        COUNTRY_FULL,
        YEARMODA,
        SUM(CAST(RIGHT(FRSHTT, 1) AS int)) AS IS_TORNADO_OR_FUNNEKL
    FROM weather_with_country
    WHERE YEAR(YEARMODA) = 2019 AND COUNTRY_FULL IS NOT NULL
    GROUP BY COUNTRY_FULL, YEARMODA
),
ROW_NUMBER_CTE AS 
(
    SELECT 
        COUNTRY_FULL,
        YEARMODA,
        IS_TORNADO_OR_FUNNEKL,
        ROW_NUMBER() OVER(PARTITION BY COUNTRY_FULL ORDER BY YEARMODA) as row_number
    FROM IS_TORNADO_OR_FUNNEL_CTE
    WHERE IS_TORNADO_OR_FUNNEKL > 0
)
SELECT 
    COUNTRY_FULL,
    COUNT(*) as CONSECUTIVE_DAYS,
    MIN(YEARMODA) AS START_DATE,
    MAX(YEARMODA) AS END_DATE
FROM ROW_NUMBER_CTE
GROUP BY COUNTRY_FULL,  DATE_ADD(day, -row_number, YEARMODA)
ORDER BY CONSECUTIVE_DAYS DESC
""").show()
```

    [Stage 49:==============>                                         (4 + 11) / 15]

    +--------------+----------------+----------+----------+
    |  COUNTRY_FULL|CONSECUTIVE_DAYS|START_DATE|  END_DATE|
    +--------------+----------------+----------+----------+
    |        CANADA|               2|2019-06-25|2019-06-26|
    |CAYMAN ISLANDS|               2|2019-10-31|2019-11-01|
    |         GHANA|               2|2019-09-06|2019-09-07|
    |         INDIA|               2|2019-09-07|2019-09-08|
    |         ITALY|               2|2019-01-23|2019-01-24|
    |         ITALY|               2|2019-10-02|2019-10-03|
    |         ITALY|               2|2019-11-14|2019-11-15|
    |         JAPAN|               2|2019-06-10|2019-06-11|
    |         JAPAN|               2|2019-12-03|2019-12-04|
    | UNITED STATES|               2|2019-01-12|2019-01-13|
    | UNITED STATES|               2|2019-06-29|2019-06-30|
    |       ALGERIA|               1|2019-07-04|2019-07-04|
    |        ANGOLA|               1|2019-02-06|2019-02-06|
    |        ANGOLA|               1|2019-04-05|2019-04-05|
    |      ANGUILLA|               1|2019-06-06|2019-06-06|
    |     ARGENTINA|               1|2019-05-10|2019-05-10|
    |         ARUBA|               1|2019-09-23|2019-09-23|
    |       AUSTRIA|               1|2019-01-20|2019-01-20|
    |       AUSTRIA|               1|2019-06-16|2019-06-16|
    |       AUSTRIA|               1|2019-06-19|2019-06-19|
    +--------------+----------------+----------+----------+
    only showing top 20 rows
    


                                                                                    


```python
# 3. Which country had the second highest average mean wind speed over the year?
spark.sql("""
WITH AVE_MEAN_WDSP AS (
SELECT
    SUM(WDSP)/COUNT(YEARMODA) as AVE_MEAN_WDSP,
    COUNTRY_FULL
FROM weather_with_country
WHERE year(YEARMODA) = 2019 and COUNTRY_FULL IS NOT NULL
GROUP BY COUNTRY_ABBR, COUNTRY_FULL
)
SELECT 
    COUNTRY_FULL,
    MAX(AVE_MEAN_WDSP) AS SECOND_HIGHEST
FROM AVE_MEAN_WDSP 
WHERE AVE_MEAN_WDSP < (SELECT MAX(AVE_MEAN_WDSP) FROM AVE_MEAN_WDSP)
GROUP BY COUNTRY_FULL
ORDER BY SECOND_HIGHEST DESC
""").head()
```

    23/09/25 07:53:36 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.
                                                                                    




    Row(COUNTRY_FULL='FALKLAND ISLANDS (ISLAS MALVINAS)', SECOND_HIGHEST=17.84236111111111)




```python

```
