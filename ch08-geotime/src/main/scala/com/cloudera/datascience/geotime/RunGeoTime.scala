/*
 * Copyright 2015 and onwards Sanford Ryza, Juliet Hougland, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.cloudera.datascience.geotime

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Locale

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._

import org.joda.time.Duration

import com.esri.core.geometry.Point
import spray.json._

import com.cloudera.datascience.geotime.GeoJsonProtocol._

case class Trip(
  license: String,
  pickupTime: Long,
  dropoffTime: Long,
  pickupX: Double,
  pickupY: Double,
  dropoffX: Double,
  dropoffY: Double)

object RunGeoTime extends Serializable {

  val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val taxiRaw = spark.read.textFile("taxidata")

    // TODO: CSV parsing and filtering
    val taxiGood = taxiRaw.map(parse)
    taxiGood.cache()

    val hours = (pickup: Long, dropoff: Long) => {
      val d = new Duration(pickup, dropoff)
      d.getStandardHours
    }
    val hoursUDF = udf(hours)

    taxiGood.groupBy(hoursUDF('pickupTime, 'dropoffTime)).count().show()

    // TODO: add in 3 hour filter again
    val taxiClean = taxiGood.filter(hoursUDF('pickupTime, 'dropoffTime) > 0)

    val geojson = scala.io.Source.fromURL(this.getClass.getResource("/nyc-boroughs.geojson")).mkString

    val features = geojson.parseJson.convertTo[FeatureCollection]
    val areaSortedFeatures = features.sortBy(f => {
      val borough = f("boroughCode").convertTo[Int]
      (borough, -f.geometry.area2D())
    })

    val bFeatures = spark.sparkContext.broadcast(areaSortedFeatures)

    val borough = (x: Double, y: Double) => {
      val feature: Option[Feature] = bFeatures.value.find(f => {
        f.geometry.contains(new Point(x, y))
      })
      feature.map(f => {
        f("borough").convertTo[String]
      }).getOrElse("NA")
    }
    val boroughUDF = udf(borough)

    taxiClean.groupBy(boroughUDF('dropoffX, 'dropoffY)).count().show()
    val taxiDone = taxiClean.filter("dropoff_x != 0 and dropoff_y != 0 and pickup_x != 0 and pickup_y != 0")
    taxiDone.groupBy(boroughUDF('dropoffX, 'dropoffY)).count().show()

    taxiGood.unpersist()

    val sessions = taxiDone.repartition('license).sortWithinPartitions('license, 'pickupTime)

    def boroughDuration(t1: Trip, t2: Trip): (String, Long) = {
      val b = borough(t1.dropoffX, t1.dropoffY)
      val d = new Duration(t1.dropoffTime, t2.pickupTime)
      (b, d.getStandardSeconds)
    }

    val boroughDurations: DataFrame =
      sessions.mapPartitions(trips => {
        val iter: Iterator[Seq[Trip]] = trips.sliding(2)
        val viter = iter.filter(_.size == 2)
        viter.map(p => boroughDuration(p(0), p(1)))
      }).toDF("borough", "seconds")

    boroughDurations.select("seconds / 60 as hours").describe("hours").show()
    taxiDone.unpersist()

    boroughDurations.where("seconds > 0").groupBy("borough").agg(sum("seconds")).show()

    boroughDurations.unpersist()
  }

  def parse(line: String): Trip = {
    val fields = line.split(',')
    val license = fields(1)
    // Not thread-safe:
    val formatterCopy = formatter.clone().asInstanceOf[SimpleDateFormat]
    val pickupTime = formatterCopy.parse(fields(5)).getTime()
    val dropoffTime = formatterCopy.parse(fields(6)).getTime()
    val pickupX = fields(10).toDouble
    val pickupY = fields(11).toDouble
    val dropoffX = fields(12).toDouble
    val dropoffY = fields(13).toDouble
    Trip(license, pickupTime, dropoffTime, pickupX, pickupY, dropoffX, dropoffY)
  }
}
