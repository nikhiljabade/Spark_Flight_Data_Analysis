package com.cloudwick.spark.assignment

import java.util.Calendar
import org.apache.spark.{SparkConf, SparkContext}
import com.typesafe.config.ConfigFactory

/**
  * Created by Nikhil Jabade on 3/16/2017
  */

object Airport {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/Cloudwick/Spark/hadoop-common-2.2.0-bin-master/hadoop-common-2.2.0-bin-master");
    val confFile = ConfigFactory.load()

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("flightdata")
    val sc = new SparkContext(sparkconf)
    val airportDataMain = sc.textFile("C:/Cloudwick/Spark/Assignment Flight Data/200*.csv")
    val airportData = airportDataMain.map(line => line.split(","))
    val destData = airportData.filter(row => row(17).contains(confFile.getString("airport.name")) && !row(17).contains("NA"))
      .filter(row=> !row(0).contains("NA") && !row(1).contains("NA") && !row(2).contains("NA") && !row(14).contains("NA"))

    val arrDelayData = destData.map{line => val x=weekOfTheYear(line(0).toInt,line(1).toInt,line(2).toInt); ((line(0).toInt,line(8),x),line(14).toInt)}
    arrDelayData.filter(x=>x._2>0).reduceByKey((x,y)=>x+y).sortByKey(true).saveAsTextFile("C:/Cloudwick/Spark/Assignment Flight Data Arrival Delay/")

    println("============================================================================================")

    val originData = airportData.filter(row => row(16).contains(confFile.getString("airport.name")) && !row(16).contains("NA")).filter(row=> !row(0).contains("NA") && !row(1).contains("NA") && !row(2).contains("NA") && !row(15).contains("NA"))
    val depDelayData = originData.map{line => val x=weekOfTheYear(line(0).toInt,line(1).toInt,line(2).toInt); ((line(0).toInt,line(8),x),line(15).toInt)}
    depDelayData.filter(x=>x._2>0).reduceByKey((x,y)=>x+y).sortByKey(true).saveAsTextFile("C:/Cloudwick/Spark/Assignment Flight Data Dep Delay/")
  }

  def weekOfTheYear(year:Int, month:Int, dayOfMonth:Int):Int= {
    val ca1:Calendar = Calendar.getInstance()
    ca1.set(year, month-1, dayOfMonth)
    ca1.get(Calendar.WEEK_OF_YEAR)
  }
}
