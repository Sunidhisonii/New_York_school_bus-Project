Import pyspark 
from prspark import sparkContext
from pyspark import SQLContext
from pyspark.sql import functions as F

-----------------------------------------------------------------------------------------------------------

#Load data 
bus_data = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ",").load("E:\data\Bus.csv")
busdata.printSchema()
busdata.show()

-----------------------------------------------------------------------------------------------------------

#Create dataframe
df=sqlContext.read.csv("E:\Bus.csv")
df.show()

------------------------------------------------------------------------------------------------------------

1# Most common reasons for either a delay or breaking down of the bus
 
 df1 = busdata.groupBy("Reason").count()
 df1 = df.withColumnRenamed("count","bus_data").show()
+--------------------+--------+
|              Reason|bus_data|
+--------------------+--------+
|                null|       2|
|         Problem Run|    3987|
|  Weather Conditions|    6853|
|               Other|   37019|
|           Flat Tire|    8307|
|  Mechanical Problem|   27821|
|   Delayed by School|    2207|
|         Won`t Start|   12172|
|Late return from ...|    5613|
|            Accident|    2472|
|       Heavy Traffic|  170660|
+--------------------+--------+

 df1.sort(df.bus_data.desc()).show(1,truncate=False)
+-------------+--------+
|Reason       |bus_data|
+-------------+--------+
|Heavy Traffic|170660  |
+-------------+--------+

------------------------------------------------------------------------------------------------------------
2# Top five route numbers where the bus was either delayed or broke down

 df1 = bus_data.groupBy("Reason","Route_Number").count()
 df1.show()
+------------------+------------+-----+
|            Reason|Route_Number|count|
+------------------+------------+-----+
|     Heavy Traffic|        X067|   77|
|     Heavy Traffic|       3604A|   10|
|     Heavy Traffic|        K243|   55|
|     Heavy Traffic|        X252|   40|
|         Flat Tire|        X798|    2|
|Mechanical Problem|        X432|    5|
|     Heavy Traffic|       17 AM|    2|
|     Heavy Traffic|        W644|   21|
|     Heavy Traffic|        M006|   15|
|     Heavy Traffic|       R1006|   10|
|     Heavy Traffic|       R1116|    4|
|             Other|        K489|    1|
|     Heavy Traffic|        Y907|   17|
|             Other|        X706|    8|
|     Heavy Traffic|        R664|   20|
|             Other|        M281|    2|
|     Heavy Traffic|        X072|   25|
|     Heavy Traffic|        K295|  108|
|Weather Conditions|       X2358|    2|
|     Heavy Traffic|       K9346|    2|
+------------------+------------+-----+
only showing top 20 rows

df2=df1.withColumnRenamed("count","reason_count")
df2.show()
+------------------+------------+------------+
|            Reason|Route_Number|reason_count|
+------------------+------------+------------+
|     Heavy Traffic|        X067|          77|
|     Heavy Traffic|       3604A|          10|
|     Heavy Traffic|        K243|          55|
|     Heavy Traffic|        X252|          40|
|         Flat Tire|        X798|           2|
|Mechanical Problem|        X432|           5|
|     Heavy Traffic|       17 AM|           2|
|     Heavy Traffic|        W644|          21|
|     Heavy Traffic|        M006|          15|
|     Heavy Traffic|       R1006|          10|
|     Heavy Traffic|       R1116|           4|
|             Other|        K489|           1|
|     Heavy Traffic|        Y907|          17|
|             Other|        X706|           8|
|     Heavy Traffic|        R664|          20|
|             Other|        M281|           2|
|     Heavy Traffic|        X072|          25|
|     Heavy Traffic|        K295|         108|
|Weather Conditions|       X2358|           2|
|     Heavy Traffic|       K9346|           2|
+------------------+------------+------------+
only showing top 20 rows

 y=df2.sort(df2.reason_count.desc())
 y.show()
+-------------+------------+------------+
|       Reason|Route_Number|reason_count|
+-------------+------------+------------+
|Heavy Traffic|           1|        3141|
|Heavy Traffic|           2|        2313|
|Heavy Traffic|           5|        2169|
|Heavy Traffic|           3|        2010|
|Heavy Traffic|           4|        1152|
|Heavy Traffic|           6|         881|
|Heavy Traffic|           7|         791|
|Heavy Traffic|           8|         521|
|        Other|           1|         427|
|Heavy Traffic|        M966|         371|
|Heavy Traffic|        M545|         359|
|        Other|           2|         344|
|Heavy Traffic|        M617|         341|
|Heavy Traffic|        M788|         335|
|Heavy Traffic|        M261|         329|
|Heavy Traffic|        M950|         328|
|Heavy Traffic|          18|         305|
|Heavy Traffic|        M978|         305|
|Heavy Traffic|        M767|         293|
|Heavy Traffic|        M771|         285|
+-------------+------------+------------+
only showing top 20 rows

> y.show(5,truncate=False)
+-------------+------------+------------+
|Reason       |Route_Number|reason_count|
+-------------+------------+------------+
|Heavy Traffic|1           |3141        |
|Heavy Traffic|2           |2313        |
|Heavy Traffic|5           |2169        |
|Heavy Traffic|3           |2010        |
|Heavy Traffic|4           |1152        |
+-------------+------------+------------+
only showing top 5 rows
-------------------------------------------------------------------------------------------------------------

3# The total number of incidents, year-wise, when the students were On the bus

  df1= bus_data.filter(bus_data.Number_Of_Students_On_The_Bus.contains("0")).groupBy("School_Year").count()
  df1.show()
+-----------+-----+
|School_Year|count|
+-----------+-----+
|  2016-2017|48286|
|  2017-2018|63421|
|  2019-2020|    1|
|  2018-2019|28167|
|  2015-2016|32757|
+-----------+-----+

-------------------------------------------------------------------------------------------------------------

4# The year in which accidents were less

  df1 = bus_data.filter(busdata.Reason.contains("Accident")).groupby("School_Year").count()
  df1.show()
+-----------+-----+
|School_Year|count|
+-----------+-----+
|  2016-2017|  725|
|  2017-2018|  782|
|  2018-2019|  339|
|  2015-2016|  626|
+-----------+-----+
  df2=df1.withColumnRenamed("count","year_count")
  df2.show()

+-----------+----------+
|School_Year|year_count|
+-----------+----------+
|  2016-2017|       725|
|  2017-2018|       782|
|  2018-2019|       339|
|  2015-2016|       626|
+-----------+----------+
  y=df.sort(df.year_count.asc())
  y.show()
+-----------+----------+
|School_Year|year_count|
+-----------+----------+
|  2018-2019|       339|
|  2015-2016|       626|
|  2016-2017|       725|
|  2017-2018|       782|
+-----------+----------+
 y.show(1,truncate=False)
+-----------+----------+
|School_Year|year_count|
+-----------+----------+
|2018-2019  |339       |
+-----------+----------+
only showing top 1 row
--------------------------------------------------------------------------------------------------------------

5# How many incidents are done in 2015-2016 and 2017-2018
 df1 = busdata.filter(busdata.School_Year.contains("2015-2016")).groupBy("Reason").count()
 df1.show
+--------------------+-----+
|              Reason|count|
+--------------------+-----+
|         Problem Run| 1030|
|  Weather Conditions| 2529|
|               Other|11125|
|           Flat Tire| 1709|
|  Mechanical Problem| 5516|
|   Delayed by School|  617|
|         Won`t Start| 2945|
|Late return from ...| 1742|
|            Accident|  626|
|       Heavy Traffic|35334|
+--------------------+-----+
  df2 = busdata.filter(busdata.School_Year.contains("2017-2018")).groupBy("Reason").count()
  df2.show()
+--------------------+-----+
|              Reason|count|
+--------------------+-----+
|                null|    2|
|         Problem Run|  708|
|  Weather Conditions| 1612|
|               Other|11428|
|           Flat Tire| 2770|
|  Mechanical Problem|10193|
|   Delayed by School|  591|
|         Won`t Start| 3743|
|Late return from ...| 1707|
|            Accident|  782|
|       Heavy Traffic|55897|
+--------------------+-----+
   
   df3 = df1.union(df2)
   df3.show(30,truncate=False)
+---------------------------+-----+
|Reason                     |count|
+---------------------------+-----+
|null                       |2    |
|Problem Run                |708  |
|Weather Conditions         |1612 |
|Other                      |11428|
|Flat Tire                  |2770 |
|Mechanical Problem         |10193|
|Delayed by School          |591  |
|Won`t Start                |3743 |
|Late return from Field Trip|1707 |
|Accident                   |782  |
|Heavy Traffic              |55897|
|null                       |2    |
|Problem Run                |708  |
|Weather Conditions         |1612 |
|Other                      |11428|
|Flat Tire                  |2770 |
|Mechanical Problem         |10193|
|Delayed by School          |591  |
|Won`t Start                |3743 |
|Late return from Field Trip|1707 |
|Accident                   |782  |
|Heavy Traffic              |55897|
+---------------------------+-----+

-------------------------------------------------------------------------------------------------------------

6# count total incidents year-wise

  df1= busdata.groupBy("School_Year").count()
  df1.show()

+-----------+-----+
|School_Year|count|
+-----------+-----+
|  2016-2017|83140|
|  2017-2018|89433|
|  2019-2020|    1|
|  2018-2019|41366|
|  2015-2016|63173|
+-----------+-----+

---------------------------------------------------------------------------------------------------------------











