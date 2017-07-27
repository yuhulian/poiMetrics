package com.simon

import java.util

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by simon on 2017/7/26.
  *  This small program is designed to metric poi information of a user both in a local and roaming city,including:
  *  1. num of POIs in local city
  *  2, total dwell frequency in local city
  *  3. num of POIs in roaming cities
  *  4. total dwell frequency in roaming cities
  *  5. num of roaming cities
  *  6. commute journey distances from home to work or work to home
  */
object poiMetrics {
  def main(args: Array[String]): Unit = {
  //1. we need to extract home and poi for each user first, if no work found, commute distance is set to zero
    if (args.length != 3) {
      println("Wrong number of arguments!")
      println("Usage: <$PROVINCE> <$POI_DATE> <$RESULT_DIR>")
      System.exit(1)
    }
    val province = args(0)
    val monthid = args(1)
    val year = args(1).toString.substring(0, 4)
    val month = args(1).toString.substring(4, 6)
    val day = args(1).toString.substring(6)
    val resultDir = args(2)

    val conf = new SparkConf().setAppName("poiMetrics")
    val sc = new SparkContext(conf)

    val basicDir = "/user/ss_deploy/"
    val poiSubDir = "/cip/ulm/poi/0/"
    val poiVersion ="/csv_1_2"
    val jpSubDir = "/cip/ulm/journey_plus_final/0/"
    val jpVersion = "/csv_1_4"

    val poiRDD = sc.textFile(basicDir + province + poiSubDir + year + "/" + month + "/" + day + poiVersion + "/p*").map(_.split("\\|")).map(x => {
      //get basic information first
      val user_id = x(0)
      val poiType = x(4)
      val zone = x(5)
      val dwellFrequency = x(13).toInt
      (user_id,poiType+"|"+zone+"|"+dwellFrequency)
    }).reduceByKey((_)+","+(_))
    poiRDD.cache()

    val poiMetricsRDD = poiRDD.map(x => {
      //create two lists to store  both local and roaming pois
      val user_id = x._1
      var rawList = new util.ArrayList[String]
      var localList = new util.ArrayList[String]
      var roamList = new util.ArrayList[String]
      var cityList = List("")

      var strArr: Array[String] = x._2.split(",")
      for (i <- 0 to (strArr.length - 1)) {
        val split: Array[String] = strArr(i).split("\\|")
        rawList.add(strArr(i))
      }

      //5 features we need to collect
      var localNumPOI =0
      var localNumDwells = 0
      var roamNumPOIs = 0
      var roamNumDwells = 0
      var numRoamCity = 0

      var homeCity = "" //a variable used to distinguish local and roaming POIs
      for(i <- 0 to rawList.size()-1){
        //
        var poiInfo =  rawList.get(i)
        var poiType = poiInfo.substring(0,1).toInt
        var zone = poiInfo.substring(2,poiInfo.lastIndexOf("|"))
        var city = zone.substring(0,3)
        var numDwells = poiInfo.substring(poiInfo.lastIndexOf("|")+1,poiInfo.length).toInt

        //        println(poiType+"|"+zone+"|"+city+"|"+numDwells)
        cityList = city +: cityList
        if (poiType == 1){
          homeCity = city
        }

        if (("Unk").equals(homeCity)){
          //local POI metrics, if one home POI spotted, add 1 POI and sum its dwell frequency
          roamNumDwells = 0
          localNumPOI += 1
          localNumDwells += numDwells
        }else if (city.equals(homeCity)) {
          localNumPOI += 1
          localNumDwells += numDwells
        }
        else{
          //roam POI metrics, if one roam POi spotted, add 1 POI and sum its dwell frequency
          roamNumPOIs += 1
          roamNumDwells += numDwells
        }
      }
      numRoamCity = cityList.distinct.size - 2

      (user_id,localNumPOI,localNumDwells,roamNumPOIs,roamNumDwells,numRoamCity)
    }
    ).map(x=>(x._1,x._2+"|"+x._3+"|"+x._4+"|"+x._5+"|"+x._6))


    //get commute journey metric distance
    val jpRDD = sc.textFile(basicDir + province + jpSubDir + year + "/" + month + "/*/" + jpVersion + "/p*").map(_.split("\\|")).map(x=>
      (x(1),x(17),x(42)+x(43))).filter(x=>(x._3.equals("12") ||x._3.equals("21"))).map(x=>(x._1,x._2)).distinct()

    poiMetricsRDD.leftOuterJoin(jpRDD).map(x=>x._1+"|"+x._2._1+"|"+x._2._2.getOrElse(-999)).coalesce(64).saveAsTextFile(resultDir)
  }
}
