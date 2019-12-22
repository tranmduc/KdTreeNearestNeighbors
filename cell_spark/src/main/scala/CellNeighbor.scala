import java.io.IOException
import java.text.DecimalFormat
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object CellNeighbor {
  def distance(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    val theta: Double = lon1 - lon2
    var dist: Double = Math.sin(Math.toRadians(lat1)) * Math.sin(
      Math.toRadians(lat2)) +
      Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
        Math.cos(Math.toRadians(theta))
    dist = Math.acos(dist)
    dist = Math.toDegrees(dist)
    dist = dist * 60 * 1.1515
    dist = dist * 1.609344

    return dist
  }

  def toX(lat: Double, lon: Double): Double ={
    val R = 6371;
    val x: Double = Math.cos(Math.toRadians(lat)) * Math.cos(Math.toRadians(lon)) * R
    return x
  }

  def toY(lat: Double, lon: Double): Double ={
    val R = 6371;
    val y: Double = Math.cos(Math.toRadians(lat)) * Math.sin(Math.toRadians(lon)) * R
    return y
  }

  def toZ(lat: Double, lon: Double): Double ={
    val R = 6371;
    val z: Double = Math.sin(Math.toRadians(lat)) * R
    return z
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("CellNeighbor").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val session = SparkSession.builder().appName("CellNeighbor").master("local[*]").getOrCreate()
    //val lines = sc.textFile("in/cellTest.txt")
    val lines = sc.textFile("in/cell_neighbors_5km.csv")

    val rdd = lines
      .filter(line => !line.contains("CELL,LATITUDE,LONGITUDE"))
      .filter(line => line.split(",").length == 3)
      .filter(line => {
        val splits = line.split(",")
        (splits(0) != "" && splits(1) != "" && splits(2) != "")
      })
      .map(line => {
      val splits = line.split(",")
      coordinate(splits(0).trim, toX(splits(1).toDouble, splits(2).toDouble), toY(splits(1).toDouble, splits(2).toDouble), toZ(splits(1).toDouble, splits(2).toDouble))
    })



    //println(lines.count())
//
//    //for ((cell, location) <- rdd.collect()) println(cell + ": " + location)
//
//    val eps = 50.0
//
//    val distanceBetweenPoints = rdd.cartesian(rdd)
//      .filter{case (x,y) => (x != y)}
//      .map{case (x,y) => (x._1, (y._1, distance(x._2._1, x._2._2, y._2._1, y._2._2)))}
//
//    //println(distanceBetweenPoints.count())
//
//    //distanceBetweenPoints.coalesce(1).saveAsTextFile("out/test/distanceBetweenPoints_1")
//
//    val pointsWithinEps = distanceBetweenPoints.filter{case (x, (y, distance)) => (distance <= eps)}
//
//    //println(pointsWithinEps.count())
//
//    //pointsWithinEps.coalesce(1).saveAsTextFile("out/test/pointsWithinEps_1")
//
//    val cellNeighbor = pointsWithinEps.map(x => (x._1, x._2._1))
//
//    //println(cellNeighbor.count())
//
//    //cellNeighbor.coalesce(1).saveAsTextFile("out/test/cellNeighbor")
//
//    val cellNeighborByKey = cellNeighbor.groupByKey
//
//    //cellNeighborByKey.coalesce(1).saveAsTextFile("out/test/cellNeighborByKey_2")
//
    import session.implicits._
    val DF = rdd.toDF("CELL", "X", "Y", "Z")

    try{
      DF.coalesce(1).write.mode("overwrite").format("csv").save("out/test/cell2")

      val fs = FileSystem.get(sc.hadoopConfiguration)
      val filePath = "out/test/cell2/"
      val fileName = fs.globStatus(new Path(filePath+"part*"))(0).getPath.getName

      fs.rename(new Path(filePath+fileName), new Path(filePath+"cell2.csv"))
    }catch{
      case e: IOException => e.printStackTrace
    }
  }
}
