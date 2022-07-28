import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("Boston_analytics_pyspark").getOrCreate()

crimes, offence_codes, output_parquet = str(sys.argv[1]), str(sys.argv[2]), str(sys.argv[3])

df_crimes = spark.read.csv(
    crimes,
    inferSchema=True, header=True).dropDuplicates()

df_codes = spark.read.csv(
    offence_codes,
    inferSchema=True, header=True).dropDuplicates()

df_codes_sp = df_codes.withColumn('NAME', split(df_codes['name'], '-').getItem(0))

crimeStatsWithBroadcast = df_crimes.join(broadcast(df_codes_sp), df_crimes.OFFENSE_CODE == df_codes_sp.CODE)

crimeStatsWithBroadcast = crimeStatsWithBroadcast.where("district is not null")

crimeStatsWithBroadcast = crimeStatsWithBroadcast.withColumn('cnt', lit(1))

crimeStatsWithBroadcast.createOrReplaceTempView("tmp_crimes")

crimeTotal = spark.sql("select DISTRICT, YEAR, MONTH, count(*) as crimes_total, avg(Lat) as  lat, avg(Long) as long "
                       "from tmp_crimes " 
                       "group by DISTRICT, YEAR, MONTH order by crimes_total desc")

crimeTotal.createOrReplaceTempView("tmp_crimes_b2")

crimeTotalb2 = spark.sql("select DISTRICT, sum(crimes_total) as crimes_total, percentile_approx(crimes_total, 0.5) as crimes_monthly , avg(Lat) as  lat, avg(Long) as long "
                       "from tmp_crimes_b2 " 
                       "group by DISTRICT order by crimes_total desc")

freq_crimes = spark.sql("select DISTRICT, Name, count(1) as cnt from tmp_crimes group by district, name order by district, cnt desc")
freq_crimes.createOrReplaceTempView("freq_crimes")
freq_crimes = spark.sql("SELECT * FROM ("
    "SELECT e.*, " 
    "ROW_NUMBER() OVER (partition by district ORDER BY cnt DESC) rn " 
    "FROM freq_crimes e )"
    "WHERE rn <= 3")

freq_crimes.createOrReplaceTempView("freq_crimes")

freq_crimes = spark.sql("SELECT DISTRICT as distr, array_join(collect_list(name),', ') as frequent_crime_types from freq_crimes group by district")

crimeStats = freq_crimes.join(crimeTotalb2, crimeTotalb2.DISTRICT == freq_crimes.distr)

crimeStats.createOrReplaceTempView("crimeStats")

crimeStats = spark.sql("select district, crimes_total, crimes_monthly, frequent_crime_types, lat, long from crimeStats order by crimes_total desc")

crimeStats.write.parquet(output_parquet)

crimeStats.show()

spark.stop()