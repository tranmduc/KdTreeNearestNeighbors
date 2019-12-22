object test_distance {

  def main(args: Array[String]){
    println(distance(10.357000, 106.510000, 10.457000,106.260000) + " Kilometers\n")
  }

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

    val R = 6371;
    val x1: Double = Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lon1)) * R
    val x2: Double = Math.cos(Math.toRadians(lat2)) * Math.cos(Math.toRadians(lon2)) * R
    val y1: Double = Math.cos(Math.toRadians(lat1)) * Math.sin(Math.toRadians(lon1)) * R
    val y2: Double = Math.cos(Math.toRadians(lat2)) * Math.sin(Math.toRadians(lon2)) * R
    val z1: Double = Math.sin(Math.toRadians(lat1)) * R
    val z2: Double = Math.sin(Math.toRadians(lat2)) * R

    val dist2 = Math.sqrt(Math.pow(x1-x2,2) + Math.pow(y1-y2,2) + Math.pow(z1-z2,2))

    return dist
  }
}
