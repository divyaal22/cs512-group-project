package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle:String, pointString:String))))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle:String, pointString:String))))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1:String, pointString2:String, distance:Double))))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1:String, pointString2:String, distance:Double))))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def ST_Contains(queryRectangle:String, pointString:String): Boolean ={

    val rectangleCoordinates= queryRectangle.trim.split(",").map(_.toDouble)
    val pointCoordinates = pointString.trim.split(",").map(_.toDouble)
    var result = false

    val minXCoordinate = math.min(rectangleCoordinates(0),rectangleCoordinates(2))
    val maxXCoordinate = math.max(rectangleCoordinates(0),rectangleCoordinates(2))
    val minYCoordinate = math.min(rectangleCoordinates(1),rectangleCoordinates(3))
    val maxYCoordinate = math.max(rectangleCoordinates(1),rectangleCoordinates(3))

    if (pointCoordinates(0) >= minXCoordinate && pointCoordinates(0) <= maxXCoordinate && pointCoordinates(1) >= minYCoordinate && pointCoordinates(1) <= maxYCoordinate){
      result = true
    }

    return result

  }

  def ST_Within(pointString1:String, pointString2:String, distance:Double): Boolean ={

    val xCoordinates = pointString1.trim.split(",").map(_.toDouble)
    val yCoordinates = pointString2.trim.split(",").map(_.toDouble)

    //Calculate shortest distance
    val pointDistance = Math.sqrt(Math.pow((xCoordinates(0) - yCoordinates(0)), 2) + Math.pow((xCoordinates(1) - yCoordinates(1)), 2))

    val result = if (pointDistance <= distance) true else false

    return result
  }


}
