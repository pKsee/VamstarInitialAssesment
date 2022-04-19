Last login: Fri Apr  8 00:05:58 on ttys001
pksee@pKsee ~ % pyspark
Python 3.9.6 (v3.9.6:db3ff76da1, Jun 28 2021, 11:49:53) 
[Clang 6.0 (clang-600.0.57)] on darwin
Type "help", "copyright", "credits" or "license" for more information.
22/04/08 00:34:31 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
22/04/08 00:34:32 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.1.2
      /_/

Using Python version 3.9.6 (v3.9.6:db3ff76da1, Jun 28 2021 11:49:53)
Spark context Web UI available at http://192.168.29.253:4041
Spark context available as 'sc' (master = local[*], app id = local-1649358273144).
SparkSession available as 'spark'.
>>> df1 = spark.read.option("inferSchema","true").option("multiline", "true").json("/Users/pksee/Desktop/data.json")
>>> df1.createOrReplaceTempView("temp_table")                                   
>>> df1.printSchema()
root
 |-- Gender: string (nullable = true)
 |-- HeightCm: long (nullable = true)
 |-- WeightKg: long (nullable = true)

>>> df1.show(10)
+------+--------+--------+
|Gender|HeightCm|WeightKg|
+------+--------+--------+
|  Male|     171|      96|
|  Male|     161|      85|
|  Male|     180|      77|
|Female|     166|      62|
|Female|     150|      70|
|Female|     167|      82|
+------+--------+--------+

>>> ## The below code solves the scenario given in question 1
>>> spark.sql("""with bmi_table as (
...     SELECT Gender, HeightCm, WeightKg, ROUND((WeightKg/power((HeightCm/100),2)),2) as BMI
...     FROM temp_table
... )
... SELECT *,
... CASE WHEN BMI < 18.4 THEN "Underweight"
...      WHEN BMI BETWEEN 18.5 AND 24.9 THEN "Normal weight"
...      WHEN BMI BETWEEN 25 AND 29.9 THEN "Overweight"
...      WHEN BMI BETWEEN 30 AND 34.9 THEN "Moderately obese"
...      WHEN BMI BETWEEN 35 AND 39.9 THEN "Severely obese"
...      ELSE "Very severely obese" 
... END AS BMICategory,
... CASE WHEN BMI < 18.4 THEN "Malnutrition risk"
...      WHEN BMI BETWEEN 18.5 AND 24.9 THEN "Low risk"
...      WHEN BMI BETWEEN 25 AND 29.9 THEN "Enhanced risk"
...      WHEN BMI BETWEEN 30 AND 34.9 THEN "Medium risk"
...      WHEN BMI BETWEEN 35 AND 39.9 THEN "High risk"
...      ELSE "Very high risk" 
... END AS HealthRisk FROM bmi_table""").show(10)
+------+--------+--------+-----+----------------+-------------+
|Gender|HeightCm|WeightKg|  BMI|     BMICategory|   HealthRisk|
+------+--------+--------+-----+----------------+-------------+
|  Male|     171|      96|32.83|Moderately obese|  Medium risk|
|  Male|     161|      85|32.79|Moderately obese|  Medium risk|
|  Male|     180|      77|23.77|   Normal weight|     Low risk|
|Female|     166|      62| 22.5|   Normal weight|     Low risk|
|Female|     150|      70|31.11|Moderately obese|  Medium risk|
|Female|     167|      82| 29.4|      Overweight|Enhanced risk|
+------+--------+--------+-----+----------------+-------------+

>>> 
>>> 
>>> # Creating table from above details
>>> df2 = spark.sql("""with bmi_table as (
...     SELECT Gender, HeightCm, WeightKg, ROUND((WeightKg/power((HeightCm/100),2)),2) as BMI
...     FROM temp_table
... )
... SELECT *,
... CASE WHEN BMI < 18.4 THEN "Underweight"
...      WHEN BMI BETWEEN 18.5 AND 24.9 THEN "Normal weight"
...      WHEN BMI BETWEEN 25 AND 29.9 THEN "Overweight"
...      WHEN BMI BETWEEN 30 AND 34.9 THEN "Moderately obese"
...      WHEN BMI BETWEEN 35 AND 39.9 THEN "Severely obese"
...      ELSE "Very severely obese" 
... END AS BMICategory,
... CASE WHEN BMI < 18.4 THEN "Malnutrition risk"
...      WHEN BMI BETWEEN 18.5 AND 24.9 THEN "Low risk"
...      WHEN BMI BETWEEN 25 AND 29.9 THEN "Enhanced risk"
...      WHEN BMI BETWEEN 30 AND 34.9 THEN "Medium risk"
...      WHEN BMI BETWEEN 35 AND 39.9 THEN "High risk"
...      ELSE "Very high risk" 
... END AS HealthRisk FROM bmi_table""")
>>> df2.createOrReplaceTempView("final_table")
>>> spark.sql("select * from final_table").show(10)
+------+--------+--------+-----+----------------+-------------+
|Gender|HeightCm|WeightKg|  BMI|     BMICategory|   HealthRisk|
+------+--------+--------+-----+----------------+-------------+
|  Male|     171|      96|32.83|Moderately obese|  Medium risk|
|  Male|     161|      85|32.79|Moderately obese|  Medium risk|
|  Male|     180|      77|23.77|   Normal weight|     Low risk|
|Female|     166|      62| 22.5|   Normal weight|     Low risk|
|Female|     150|      70|31.11|Moderately obese|  Medium risk|
|Female|     167|      82| 29.4|      Overweight|Enhanced risk|
+------+--------+--------+-----+----------------+-------------+

>>> # Count the total number of overweight people using ranges in the column BMI Category of Table 1, check this is consistent programmatically and add any other observations in the documentation
>>> spark.sql("""SELECT count(1) from final_table where BMICategory = "Overweight" """)
DataFrame[count(1): bigint]
>>> spark.sql("""SELECT count(1) from final_table where BMICategory = "Overweight" """).show(10)
+--------+
|count(1)|
+--------+
|       1|
+--------+
