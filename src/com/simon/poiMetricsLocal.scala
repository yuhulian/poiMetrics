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
  *  6. commute journey distances from home to work
  */
object poiMetricsLocal {
  def main(args: Array[String]): Unit = {
  //1. we need to extract home and poi for each user first, if no work found, commute distance is set to zero
  //2.
//    if (args.length != 3) {
//      println("Wrong number of arguments!")
//      println("Usage: <$PROVINCE> <$POI_DATE> <$WORKSPACE>")
//      System.exit(1)
//    }
//    val province = args(0)
//    val monthid = args(1)
//    val year = args(1).toString.substring(0, 4)
//    val month = args(1).toString.substring(4, 6)
//    val day = args(1).toString.substring(6)

    val conf = new SparkConf().setMaster("local").setAppName("POIMetricsLocal")
    val sc = new SparkContext(conf)

//    val basicDir = "/user/ss_deploy/"
//    val subDir = "/cip/ulm/poi/0"
//    val version ="csv_1_2"

    val poiRDD = sc.textFile("E:\\【05#运维信息】\\~UE文档\\poi_test.txt").map(_.split("\\|")).map(x => {
      //get basic information first
      val user_id = x(0)
      val poiType = x(4)
      val zone = x(5)
      val dwellFrequency = x(13).toInt
      (user_id,poiType+"|"+zone+"|"+dwellFrequency)
    }).reduceByKey((_)+","+(_))
    poiRDD.cache()
//    poiRDD.take(3).foreach(println)
//     samples
//    (-9220241472228397567,1|110108|21,2|110108|26,0|110102|5,0|110108|1,0|110108|1,0|110108|4,0|110108|3,0|130825|9,0|110108|1,0|110105|1)
//    (-9222178016306422271,1|Unknown|5)
//    (-9220183144533836799,2|110106|6,1|130730|5)

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
//        roamList.add(strArr(i))
//        println(strArr(i))
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

//        println("homeCity-->"+homeCity)

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

//      println("localNumPOI-->"+localNumPOI)
//      println("localNumDWells--> "+localNumDwells)
//      println("roamNumPOIs-->"+roamNumPOIs)
//      println("roamNumDwells-->"+roamNumDwells)
//      println("numRoamCity-->"+numRoamCity)
      (user_id,localNumPOI,localNumDwells,roamNumPOIs,roamNumDwells,numRoamCity)
    }
    ).map(x=>(x._1,x._2+"|"+x._3+"|"+x._4+"|"+x._5+"|"+x._6))


    //get commute journey metric distance
    val jpRDD = sc.textFile("E:\\【05#运维信息】\\~UE文档\\journey_plus.txt").map(_.split("\\|")).map(x=>
      (x(1),x(17),x(42)+x(43))).filter(x=>(x._3.equals("12") ||x._3.equals("21"))).map(x=>(x._1,x._2)).distinct()

    poiMetricsRDD.leftOuterJoin(jpRDD).map(x=>x._1+"|"+x._2._1+"|"+x._2._2.getOrElse(-999)).take(10).foreach(println)
  }
}
