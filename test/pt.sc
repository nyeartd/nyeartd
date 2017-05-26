
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext

// define function
val ptupper = udf((a:Double,b:Double)=>a*b)

// load data
var a = spark.read.json("a.json")
a.show

// performe function
var b  = a.withColumn("PTUpper",ptupper($"MKT", $"RISK"))


// aggregate per selection trade/leg/curve
b.groupBy("TRADE","LEG","CURVE").sum("PTUpper").show

//  val colNames = List("PTUpper")
//  b.groupBy($"TRADE",$"CURVE").sum(colNames: _*).show
