package com.sudhir.spark.sql

import org.apache.spark.sql.SparkSession
import java.util.Properties
import java.text.Format
import org.apache.spark.sql.SaveMode

object WorkingWithDataSource {
  
  case class Person(name: String, age: Long)
  
  def main(args: Array[String]){
    val spark = SparkSession
      .builder()
      .appName("Working with DataSource in Spark Sql")
      .config("spark.master", "local")
      .getOrCreate()
      
    //runBasicDataSource(spark)
    //runBasicParquet(spark)
    //runParquetSchemaMerging(spark)
     //runJsonDataSet(spark)
    runJdbcDataSet(spark)
     
    spark.stop()
      
  }
  
  private def runBasicDataSource(sparkSession: SparkSession): Unit = {
    val usersDF = sparkSession.read.load("resources/users.parquet")
    usersDF.select("name", "favorite_color").show()
    usersDF.select("name", "favorite_color").write.save("resources/namesAndFavColors.parquet")
    
    
    val peopleDF = sparkSession.read.format("json").load("resources/people.json")
    peopleDF.select("name", "age").show()
    peopleDF.select("name", "age").write.format("parquet").save("resources/namesAndAges.parquet")
    
    
    val sqlDF = sparkSession.sql("SELECT * FROM parquet.`resources/users.parquet`")
    sqlDF.show()
  }
  
  
  private def runBasicParquet(sparkSession: SparkSession): Unit = {
    
    import sparkSession.implicits._
    
    val peopleDF = sparkSession.read.json("resources/people.json")
    peopleDF.write.parquet("resources/people.parquet")
    
    val parquetFileDF = sparkSession.read.parquet("resources/people.parquet")
    parquetFileDF.createOrReplaceTempView("parquetFile")
    
    val parquetDF = sparkSession.sql("SELECT * FROM parquetFile WHERE age BETWEEN 13 AND 19")
    parquetDF.show()
    val namesDF = sparkSession.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19")
    namesDF.map(attributes => "Name: " + attributes(0)).show()
    
  }
  
  
  private def runParquetSchemaMerging(sparkSession: SparkSession): Unit = {
    
    import sparkSession.implicits._
    
    val squareDF = sparkSession.sparkContext.makeRDD(6 to 10).map(num => (num, num*num)).toDF("value","square")
    squareDF.show()
    squareDF.write.parquet("data/test_table/key=1")
    
    val cubeDF = sparkSession.sparkContext.makeRDD(6 to 10).map(num => (num, num*num*num)).toDF("value","cube")
    squareDF.printSchema()

    cubeDF.show()
    cubeDF.write.parquet("data/test_table/key=2")
    
    val mergedDF = sparkSession.read.option("mergeSchema", true).parquet("data/test_table")
    
    squareDF.printSchema()
    cubeDF.printSchema()
    mergedDF.printSchema()
    
    mergedDF.show()
  }
  
  
  private def runJsonDataSet(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    
    val peopleDF = sparkSession.read.json("resources/people.json")
    peopleDF.printSchema()
    
    peopleDF.createOrReplaceTempView("people")
    val teenagerNamesDF = sparkSession.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19")
    teenagerNamesDF.show()    
    
    val otherPeopleDataset = sparkSession.createDataset("""{"name":"SRP","address":{"city":"San Jose","state":"CA"}}""" :: Nil)
    otherPeopleDataset.show()
    
    val otherPeople = sparkSession.read.json(otherPeopleDataset.toJavaRDD)
    otherPeople.show()
  }
  
  private def runJdbcDataSet(sparkSession: SparkSession): Unit = {
    
    import sparkSession.implicits._
    
    val jdbcDF = sparkSession.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://localhost:5433/postgres")
      .option("dbtable", "employee")
      //.option("user", "")
      //.option("password", "")
      .load()
      
    val connectionProperties = new Properties()
    //connectionProperties.put("user", "")
    //connectionProperties.put("password", "")
    
    val jdbcDFRetrive = sparkSession.read
      .jdbc("jdbc:postgresql://localhost:5433/postgres", "employee", connectionProperties)
      
    jdbcDFRetrive.show()
    
    //val peopleDF = sparkSession.read.format("json").load("resources/people.json")
    //peopleDF.show()
    
    
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.Row
    
    val peopleRDD = sparkSession.sparkContext.textFile("resources/people.txt")
                                
    val schemaString = "name address"
    
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
                              
    val schema = StructType(fields)
    
    val rowRDD = peopleRDD.map(_.split(",")).map(attr => Row(attr(0), attr(1).trim))

    val peopleDF = sparkSession.createDataFrame(rowRDD, schema)
    peopleDF.show()
    
    peopleDF.write
    .mode(SaveMode.Append)
    .jdbc("jdbc:postgresql://localhost:5433/postgres", "employee", connectionProperties)
    
    
    val jdbcDFRetrive1 = sparkSession.read
      .jdbc("jdbc:postgresql://localhost:5433/postgres", "employee", connectionProperties)
      
    jdbcDFRetrive1.show()
    
    
    peopleDF.write
    .mode(SaveMode.Ignore)
    .jdbc("jdbc:postgresql://localhost:5433/postgres", "employee", connectionProperties)
    
    
    val jdbcDFRetrive2 = sparkSession.read
      .jdbc("jdbc:postgresql://localhost:5433/postgres", "employee", connectionProperties)
      
    jdbcDFRetrive2.show()
    
  }
  
}