val file = sc.textFile("hdfs://<IP>/nasa/jul95")

/*
QUESTIONS:

1. Подготовить список запросов, которые закончились 5xx ошибкой, с количеством неудачных запросов
	Какие пары? (ошибка -> количество), "METHOD ADDRESS" -> количество?)
	one = rdd.dropFailed().toColumns().map(line => (line, code))
	two = one.map((line, code) => (request, 1)).reduceByKey((a, b) => a + b)

2. Подготовить временной ряд с количеством запросов по датам для всех используемых комбинаций http методов и return codes. Исключить из результирующего файла комбинации, где количество событий в сумме было меньше 10.
	one = rdd.dropFailed().toColumns().map(line => ((date, method, code), 1)).reduceByKey((a, b) => a + b).sortBy(a => a._0._0)
	two = one.filter(a => a._1 >= 10)

3. Произвести расчет скользящим окном в одну неделю количества запросов закончившихся с кодами 4xx и 5xx (?)
*/

// TASK 1
val myLogRegex = raw"""^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(.+)" (\d{3}) (\S+)""".r
file.
	filter(line => line match {
		case myLogRegex(host, client_id, user_id, datetime, request, code, size) => code(0) == '5'
		case _ => false
	}).	
	map(line => line match {
		case myLogRegex(host, client_id, user_id, datetime, request, code, size) => (request, 1)
	}).
	reduceByKey((a, b) => a + b)
rdd.
	collect().
	foreach(pair => println(pair._2 + ": " + pair._1))


// TASK 2.
val myLogRegex = raw"""^(\S+) (\S+) (\S+) \[([\w/]+):([\w:]+\s[+\-]\d{4})\] "(\S+) (.*)" (\d{3}) (\S+)""".r
val rdd = file.
	filter(line => line match {
		case myLogRegex(_*) => true
		case _ => false
	}).
	map(line => line match {
		case myLogRegex(host, client_id, user_id, date, time, method, request, code, size) => ((date, method, code), 1)
		case _ => (("01/Jan/1960", "UNKNOWN", -1), 1)
	}).
	reduceByKey((a, b) => a + b).
	sortBy(a => a._1._1)
rdd.
	collect().
	foreach(pair => println(pair._1 + ": " + pair._2))


// TASK 3
val logRegex = raw"""^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(.+)" (\d{3}) (\S+)""".r
val myLogRegex = raw"""^(\S+) (\S+) (\S+) \[([\w/]+):([\w:]+\s[+\-]\d{4})\] "(\S+) (.*)" (\d{3}) (\S+)""".r
val days = (8 to 31).toArray
val rdd = file.
	filter(line => line match {
		case logRegex(host, client_id, user_id, datetime, request, code, size) => (code(0) == '5' || code(0) == 4)
		case _ => false
	}).
	map(line => line match {
		case myLogRegex(host, client_id, user_id, date, time, method, request, code, size) => (date, 1)
		case _ => ("01/Jan/1960", 1)
	}).
	reduceByKey((a, b) => a + b).
	cartesian(sc.parallelize(days)).
	filter(a => a._1._1.split("/")(0).toInt <= a._2 && a._1._1.split("/")(0).toInt > (a._2 - 7)).
	map(a => ((a._2 - 7) + "-" + (a._2), a._1._2)).
	reduceByKey((a, b) => a + b).
	sortBy(a => a._1.split("-")(0).toInt)
rdd.
	collect().
	foreach(pair => println(pair._1 + ":\t" + pair._2))

=== OR ===

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DateType};
import java.text.SimpleDateFormat
import org.apache.spark.sql.Row
val oldFormat = new SimpleDateFormat("dd/MMM/yyyy")
val newFormat = new SimpleDateFormat("yyyy-MM-dd")
val timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
def dfSchema(columnNames: List[String]): StructType =
	StructType(
		Seq(
			StructField(name = "date", dataType = StringType, nullable = false),
			StructField(name = "count", dataType = IntegerType, nullable = false)
		)
	)
val schema = dfSchema(List("date", "count"))
val rdd = file.
	filter(line => line match {
		case logRegex(host, client_id, user_id, datetime, request, code, size) => (code(0) == '5' || code(0) == 4)
		case _ => false
	}).
	map(line => line match {
		case myLogRegex(host, client_id, user_id, date, time, method, request, code, size) => 
			Row(newFormat.format(oldFormat.parse(date)), 1)
		case _ => Row("01/Jan/1960", 1)
	})
val df = spark.
	createDataFrame(rdd, schema).
	withColumn("date", col("date").cast("date")).
	groupBy(window($"date", "1 week", "1 day")).
	agg(sum("count")).
	orderBy("window")
df.
	collect().
	foreach(row => println(row(0) + ":\t" + row(1)))