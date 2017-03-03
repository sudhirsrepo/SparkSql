package com.sudhir.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object WorkingWithFiles {
  case class People(name: String, age:Long)
  
  def main(args: Array[String]){
    val spark = SparkSession
      .builder()
      .appName("WOrking with Files in Spark Sql")
      .config("spark.master", "local")
      .getOrCreate()
      
    import spark.implicits._
    
    //runBasicDataFrame(spark)
    
    //runDataSetCreation(spark)
    
    //runInferSchema(spark)
    
    runProgrammingSchema(spark)
    
    spark.stop()
  }
  
  private def runBasicDataFrame(sparkSession: SparkSession): Unit = {
    val df = sparkSession.read.json("resources/people.json")
    
    df.show()
    
    df.printSchema()
    
    import sparkSession.implicits._
    
    df.select("name").show()
    
    df.select($"name", $"age" + 1).show()
    
    df.filter($"age" > 25).show()
    
    df.groupBy("age").count().show()
    
    df.createOrReplaceTempView("people")
    
    val sqlDF = sparkSession.sql("SELECT * FROM PEOPLE")  //View is case in-sensitive
    
    sqlDF.show()
    
    //df.createGlobalTempView("people")
    //sparkSession.sql("SELECT * FROM global_temp.people").show()
    //sparkSession.newSession().sql("SELECT * FROM global_temp.people").show()
    
    
  }
  
  private def runDataSetCreation(sparkSession: SparkSession):Unit = {
    
    import sparkSession.implicits._
    
    val caseClassDS = Seq(People("Andy", 32)).toDS()
    caseClassDS.show()
    
    
    val primitiveDS = Seq(1,2,3).toDS()
    primitiveDS.map(_+1).collect()
                .foreach(println)
                
    val peopleDS = sparkSession.read.json("resources/people.json").as[People]
    peopleDS.show()
    
    
  }
  
  private def runInferSchema(sparkSession: SparkSession):Unit = {
    
    import sparkSession.implicits._
    
    val peopleDF = sparkSession.sparkContext
                                .textFile("resources/people.txt")
                                .map(_.split(","))
                                .map(rec => People(rec(0), rec(1).trim.toInt))
                                .toDF()
    peopleDF.show()                            
    
    peopleDF.createOrReplaceTempView("people")
    
    val teenAgersDF = sparkSession.sql("SELECT * FROM people WHERE age BETWEEN 13 AND 29")
    
    teenAgersDF.show()
    
    teenAgersDF.map(row => "Name: " + row(0))
    
    teenAgersDF.show()
    
    teenAgersDF.map(attr => "Name: " + attr.getAs[String]("name")).show()
    
    teenAgersDF.map(attr => "Age: " + attr.getAs("age")).show()
    
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    teenAgersDF.map(teenager => teenager.getValuesMap[Any](List("name","age"))).collect().foreach(println)
    print(teenAgersDF.map(teenager => teenager.getValuesMap[Any](List("name","age"))).collect().toList)
    
    
  }
  
  private def runProgrammingSchema(sparkSession: SparkSession): Unit = { 
    
    val peopleRDD = sparkSession.sparkContext
                                .textFile("resources/people.txt")
                                
    val schemaString = "name age"
    
    val fields = schemaString.split(" ")
                              .map(fieldName => StructField(fieldName, StringType, nullable = true))
                              
    val schema = StructType(fields)
    
    val rowRDD = peopleRDD.map(_.split(",")).map(attr => Row(attr(0), attr(1).trim))

    val peopleDF = sparkSession.createDataFrame(rowRDD, schema)
    
    peopleDF.createOrReplaceTempView("people")
    
    val result = sparkSession.sql("SELECT * FROM people")
    
    result.show()
     
  }
}