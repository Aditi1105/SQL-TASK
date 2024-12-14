import os
import urllib.request
import ssl

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

urls_and_paths = {
    "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt": os.path.join(data_dir, "test.txt"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe": os.path.join(data_dir1, "winutils.exe"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll": os.path.join(data_dir1, "hadoop.dll")
}

# Create an unverified SSL context
ssl_context = ssl._create_unverified_context()

for url, path in urls_and_paths.items():
    # Use the unverified context with urlopen
    with urllib.request.urlopen(url, context=ssl_context) as response, open(path, 'wb') as out_file:
        data = response.read()
        out_file.write(data)
import os, urllib.request, ssl; ssl_context = ssl._create_unverified_context(); [open(path, 'wb').write(urllib.request.urlopen(url, context=ssl_context).read()) for url, path in { "https://github.com/saiadityaus1/test1/raw/main/df.csv": "df.csv", "https://github.com/saiadityaus1/test1/raw/main/df1.csv": "df1.csv", "https://github.com/saiadityaus1/test1/raw/main/dt.txt": "dt.txt", "https://github.com/saiadityaus1/test1/raw/main/file1.txt": "file1.txt", "https://github.com/saiadityaus1/test1/raw/main/file2.txt": "file2.txt", "https://github.com/saiadityaus1/test1/raw/main/file3.txt": "file3.txt", "https://github.com/saiadityaus1/test1/raw/main/file4.json": "file4.json", "https://github.com/saiadityaus1/test1/raw/main/file5.parquet": "file5.parquet", "https://github.com/saiadityaus1/test1/raw/main/file6": "file6", "https://github.com/saiadityaus1/test1/raw/main/prod.csv": "prod.csv", "https://github.com/saiadityaus1/test1/raw/main/state.txt": "state.txt", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv"}.items()]

# ======================================================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['JAVA_HOME'] = r'C:\Users\Shuvait\.jdks\corretto-1.8.0_432'

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)


##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################






## SQL PREPARATION



print("========= DATA PREPARATION======")

data = [
    (0, "06-26-2011", 300.4, "Exercise", "GymnasticsPro", "cash"),
    (1, "05-26-2011", 200.0, "Exercise Band", "Weightlifting", "credit"),
    (2, "06-01-2011", 300.4, "Exercise", "Gymnastics Pro", "cash"),
    (3, "06-05-2011", 100.0, "Gymnastics", "Rings", "credit"),
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "06-05-2011", 100.0, "Exercise", "Rings", "credit"),
    (7, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (8, "02-14-2011", 200.0, "Gymnastics", None, "cash")
]

df = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])
df.show()





data2 = [
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "02-14-2011", 200.0, "Winter", None, "cash"),
    (7, "02-14-2011", 200.0, "Winter", None, "cash")
]

df1 = spark.createDataFrame(data2, ["id", "tdate", "amount", "category", "product", "spendby"])
df1.show()






data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]



cust = spark.createDataFrame(data4, ["id", "name"])
cust.show()

data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]

prod = spark.createDataFrame(data3, ["id", "product"])
prod.show()


# Register DataFrames as temporary views
df.createOrReplaceTempView("df")
df1.createOrReplaceTempView("df1")
cust.createOrReplaceTempView("cust")
prod.createOrReplaceTempView("prod")


print("=========DONE GOOD TO GO BELOW======")





print("!!!!!select data from df!!!!!!")
print()
spark.sql("select id,tdate from df;").show()


print("!!!!!Single Column Filter!!!!!!")
print()
spark.sql("select * from df where category = 'Exercise';").show()



print("!!!!!Multi Column filter!!!!!!")
print()
spark.sql("select id,tdate,category,spendby from df where category = 'Exercise' and spendby = 'cash';").show()


print("!!!!!Multivalue filter using in operator!!!!!!")
print()
spark.sql("select * from df where category in ('Exercise', 'Gymnastics');").show()




print("!!!!! LIKE OPERATOR !!!!!!")
print()
spark.sql("select * from df where category like '%Gymnastics%';").show()



print("!!!!! NOT OPERATOR !!!!!!")
print()
spark.sql("select * from df where category != 'Exercise';").show()


print("!!!!! MULTIVALUE NOT OPERATOR !!!!!!")
print()
spark.sql("select * from df where category not in ('Exercise','Gymnastics');").show()


print("!!!!! NULL OPERATOR !!!!!!")
print()
spark.sql("select * from df where product is NULL;").show()



print("!!!!! NOT NULL OPERATOR !!!!!!")
print()
spark.sql("select * from df where product is not NULL;").show()



print("!!!!! MAX OF ID !!!!!!")
print()
spark.sql("select max(id) as idmax from df;").show()


print("!!!!! MIN OF ID !!!!!!")
print()
spark.sql("select min(id) as idmin from df;").show()


print("!!!!! COUNT OF ROWS !!!!!!")
print()
spark.sql("select count(1) from df;").show()   ### high performance use '1' in count

##spark.sql("select count(*) from df;").show()  ### we can use this also



print("!!!!! CASE OPERATOR !!!!!!")
print()
spark.sql("select *, case when spendby = 'cash' then 1 else 0 end as status from df;").show()



print("!!!!! MULTI CASE OPERATOR !!!!!!")
print()
spark.sql("select *, case when spendby = 'cash' then 1 when spendby = 'paytm' then 'NA' else 0 end as status from df;").show()



print("!!!!! CONCAT TWO COLUMNS !!!!!!")
print()
spark.sql("select id, category,concat(id,'-',category) as condata from df;").show()



print("!!!!! CONCAT MULTI COLUMNS !!!!!!")
print()
spark.sql("select id, category,product,concat_ws('-',id,category,product) as condata from df;").show()



print("!!!!! LOWER CASE !!!!!!")
print()
spark.sql("select category, lower(category) as lower from df;").show()



print("!!!!! UPPER CASE !!!!!!")
print()
spark.sql("select category, upper(category) as upper from df;").show()


print("!!!!! CEIL !!!!!!")
print()
spark.sql("select amount, ceil(amount) as ceil from df;").show()


print("!!!!! ROUND !!!!!!")
print()
spark.sql("select amount, round(amount) as round from df;").show()


print("!!!!! REPLACE NULLS !!!!!!")
print()
spark.sql("select product, coalesce(product, 'NA') as nullrep from df;").show()



print("!!!!! TRIM SPACE !!!!!!")
print()
spark.sql("select trim(product) as trimm from df;").show()



print("!!!!! DISTINCT !!!!!!")
print()
spark.sql("select distinct category as distnct from df;").show()


print("!!!!! DISTINCT on MULTI COLUMNS !!!!!!")
print()
spark.sql("select distinct category,spendby from df;").show()


print("!!!!! SUBSTRING !!!!!!")
print()
spark.sql("select substring(product,1,10) as substr from df;").show()



print("!!!!! SUBSTRING WITH SPLIT !!!!!!")
print()
spark.sql("select product,split(product,' ')[0] as split from df;").show()



##UNION####
print()
print("!!!!!  UNION ALL !!!!!")
print()
spark.sql("select * from df union all select * from df1;").show()  ## DONT REMOVE THE DUPLICATES
print()




print("!!!!!  UNION !!!!!")
print()
spark.sql("select * from df union  select * from df1;").show()  ##  REMOVE THE DUPLICATES
print()



print("!!!!!  GROUP BY !!!!!")
print()
spark.sql("select category, sum(amount) as sum from df group by category ;").show()
print()



print("!!!!! MULTI COLUMN GROUP BY !!!!!")
print()
spark.sql("select category, spendby, sum(amount) as sum from df group by category,spendby ;").show()
print()



print("!!!!! MULTI COLUMN GROUP BY WITH COUNT !!!!!")
print()
spark.sql("select category, spendby, sum(amount) as sum, count(amount) as count from df group by category,spendby ;").show()
print()



print("!!!!! MAX GROUP BY !!!!!")
print()
spark.sql("select category, max(amount) as max from df group by category order by category ;").show()
print()



# WINDOW ROW NUMBER -> categorizing the column
print("!!!!! row number !!!!!")
print()
spark.sql("select category, amount, row_number() OVER (partition by category order by amount desc) as row_number from df;").show()
print()




print("!!!!! DENSE RANK !!!!!")
print()
spark.sql("select category, amount, dense_rank() OVER (partition by category order by amount desc) as dense_rank from df;").show()
print()



print("!!!!!  RANK !!!!!")
print()
spark.sql("select category, amount, rank() OVER (partition by category order by amount desc) as rank from df;").show()
print()



print("!!!!!  LEAD !!!!!")
print()
spark.sql("select category, amount, lead(amount) OVER (partition by category order by amount desc) as lead from df;").show()
print()



print("!!!!!  LAG !!!!!")
print()
spark.sql("select category, amount, lag(amount) OVER (partition by category order by amount desc) as lag from df;").show()
print()


# HAVING ##

print("!!!!!  HAVING !!!!!")
print()
spark.sql("select category,count(category) as cnt from df group by category having count(category) > 1;").show()
print()





# JOINS ##

print()
print(" !!! INNER JOIN !!!")
print()
spark.sql("select a.*, b.product from cust a join prod b on a.id=b.id;").show()
print()



print()
print(" !!! LEFT JOIN !!!")
print()
spark.sql("select a.*, b.product from cust a left join prod b on a.id = b.id;").show()
print()


print()
print(" !!! RIGHT JOIN !!!")
print()
spark.sql("select a.*, b.product from cust a right join prod b on a.id = b.id;").show()
print()


print()
print(" !!! FULL JOIN !!!")
print()
spark.sql("select a.*, b.product from cust a full join prod b on a.id = b.id;").show()
print()


print()
print(" !!! LEFT ANTI JOIN !!!")  ### from left table get only the data which is not in right table
print()
spark.sql("select a.* from cust a left anti join prod b on a.id = b.id;").show()
print()



## Date Format












































