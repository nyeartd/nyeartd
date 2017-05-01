Scala note

// run with color package
>   spark-shell --conf spark.driver.extraJavaOptions="-Dscala.color"

// print with color
>   println(Console.CYAN + "in cyan color " + Console.WHITE + " now white ")

// load file
>  :load memo/dummy.sc

// sample data to test out dummy PnL logic

 val t0 = Seq( ("t001", 1, "new") , ("t002", 2, "roll") , ("t003", 2, "cancel")).toDF("trade", "version0", "event0")

 val t1 = Seq( ("t001", 2, "roll") , ("t002", 3, "roll") , ("t004", 1, "new")).toDF("trade", "version1", "event1")

 val p0 = Seq( ("t001", "int", 1.0), ("t001", "rebate", -1.0), ("t001", "principle", 10000.0) , ("t002", "int", 1.0), ("t002", "rebate", -1.0), ("t002", "principle", 10000.0) , ("t003", "int", 1.0), ("t003", "rebate", -1.0), ("t003", "principle", 10000.0) ).toDF("trade", "leg", "tm1close")

             
 val p1 = Seq( ("t001", "int", 1.10), ("t001", "rebate", -1.90), ("t001", "principle", 10000.0) , ("t002", "int", 1.10), ("t002", "rebate", -1.10), ("t002", "principle", 10000.0) , ("t003", "int", 1.0), ("t003", "rebate", -1.0), ("t003", "principle", 10000.0) ).toDF("trade", "leg", "t0tm1open")


 val p2 = Seq( ("t001", "int", 1.90), ("t001", "rebate", -2.90), ("t001", "principle", 10000.0) , ("t002", "int", 2.10), ("t002", "rebate", -2.10), ("t002", "principle", 10000.0) , ("t004", "int", 1.0), ("t004", "rebate", -1.0), ("t004", "principle", 20000.0) ).toDF("trade", "leg", "t0open")

 val p3 = Seq( ("t001", "int", 1.95), ("t001", "rebate", -2.190), ("t001", "principle", 10000.0) , ("t002", "int", 2.210), ("t002", "rebate", -2.910), ("t002", "principle", 10000.0) , ("t004", "int", 1.40), ("t004", "rebate", -1.50), ("t004", "principle", 20000.0) ).toDF("trade", "leg", "t0close")

 // join by trade/leg per trade life cycle logic

 val j1 = t0.join(p0, Seq("trade"), "outer")
 val j2 = t1.join(p3, Seq("trade"), "outer")
 val j3 = j1.join(p1, Seq("trade", "leg"), "left")
 val j4 = j2.join(p2, Seq("trade", "leg"), "left")
 val j = j3.join(j4, Seq("trade", "leg"), "outer")
 // turn to a dataSet
 case class LegRecord(trade: String, leg: String, version0: Integer, event0: String, tm1close: Double, t0tm1open: Double, version1: Integer, event1: String, t0close: Double, t0open: Double)

 val J = j.as[LegRecord]

// do rounding 
 def RND(n: Double): Double = { val s = math pow (10, 3); (math round n * s) / s } 

 def Pnl(r:Row) = { if (r.isNullAt(0)) RND(r.getDouble(1)) else { if (r.isNullAt(1)) RND(r.getDouble(0)) else RND(r.getDouble(0) - r.getDouble(1))} } 
   // assume udf registered ...
 J1.withColumn("PnL", callUDF("Pnl",struct($"t0close",$"tm1close"))).show 

// UserDefinedFunction

import org.apache.spark.sql.functions.udf
val upper: String => String = _.toUpperCase
val upperUDF = udf(upper)

  J.withColumn("LegUpper", upperUDF('leg)).show
                                |_ new col     |_ fun  |_ use field notice the notation

 // another example
val pnl = udf((a:Double, b:Double) => b - a )
 J4.withColumn("PnL", pnl($"tm1close", $"t0close")).show


// not easy to detect null in simple OPTION way, easier to handle with table row way
// not the PnL function below is really for demo purpose 

import org.apache.spark.sql._
val sqlContext = new SQLContext(sc)
def Pnl(r:Row) = { if (r.isNullAt(0)) r.getDouble(1) else { if (r.isNullAt(1)) r.getDouble(0) else r.getDouble(0) - r.getDouble(1)} } 
sqlContext.udf.register("Pnl", Pnl _)
J1.withColumn("PnL", callUDF("Pnl",struct($"t0close",$"tm1close"))).show


// load file example
 import org.apache.spark.sql.SQLContext
 val sqlContext = new SQLContext(sc)
 val Q = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("myfile.csv")

// write file example
J.write.format("com.databricks.spark.csv").option("header", "true").save("myfile.csv")

// sort
J.orderBy($"trade",$"leg").show

// schema
j.printSchema

// rename column
t0_npv.withColumnRenamed("value","t0npv").show

// chaining
 val j = t0_npv.withColumnRenamed("value", "t0npv").join(tm1_npv.withColumnRenamed("value","tm1npv"), Seq("trade"))

// select
 val J4=J.select("trade","leg","t0close", "tm1close")

