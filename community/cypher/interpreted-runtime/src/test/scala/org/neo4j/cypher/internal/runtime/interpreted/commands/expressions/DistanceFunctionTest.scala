/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.internal.helpers.collection.Pair
import org.neo4j.values.storable.CRSCalculator.GeographicCalculator.EARTH_RADIUS_METERS
import org.neo4j.values.storable.CoordinateReferenceSystem
import org.neo4j.values.storable.PointValue
import org.neo4j.values.storable.Values
import org.scalactic.Equality
import org.scalactic.TolerantNumerics
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.language.implicitConversions

class DistanceFunctionTest extends CypherFunSuite {

  implicit def javaToScalaPair(pair: Pair[PointValue, PointValue]): (PointValue, PointValue) =
    (pair.first(), pair.other())

  def boundingBox(center: PointValue, distance: Double): Seq[(PointValue, PointValue)] =
    center.getCoordinateReferenceSystem.getCalculator.boundingBox(center, distance).asScala.map(pair =>
      (pair.first(), pair.other())
    ).toSeq

  def boundingBoxLengthOne(center: PointValue, distance: Double): (PointValue, PointValue) = {
    val boxes = boundingBox(center, distance)
    boxes should have length 1
    boxes.head
  }

  def distance(p1: PointValue, p2: PointValue): Double =
    p1.getCoordinateReferenceSystem.getCalculator.distance(p1, p2)

  test("should calculate correct bounding box for WGS84") {
    testBoundingBox((x, y) => Values.pointValue(CoordinateReferenceSystem.WGS_84, x, y))
  }

  test("should calculate correct bounding box for WGS84-3D") {
    testBoundingBox((x, y) => Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, x, y, 100))
  }

  test("distance zero bounding box returns same point in WGS84") {
    val point = Values.pointValue(CoordinateReferenceSystem.WGS_84, 0, 0)
    val (bottomLeft, topRight) = boundingBoxLengthOne(point, 0.0)
    bottomLeft should equal(point)
    topRight should equal(point)
  }

  test("bounding box touching the date line from west") {
    val point = Values.pointValue(CoordinateReferenceSystem.WGS_84, -180, 0)
    val boxes = boundingBox(point, 1000.0)
    boxes should have length 2
    val ((bottomLeft1, topRight1), (bottomLeft2, topRight2)) = (boxes.head, boxes(1))
    bottomLeft1.coordinate()(0) shouldBe >(0.0)
    topRight1.coordinate()(0) should be(180)
    bottomLeft2.coordinate()(0) should be(-180)
    topRight2.coordinate()(0) shouldBe <(0.0)

    bottomLeft1.coordinate()(1) should be(bottomLeft2.coordinate()(1))
    topRight1.coordinate()(1) should be(topRight2.coordinate()(1))
  }

  test("bounding box touching the date line from east") {
    val point = Values.pointValue(CoordinateReferenceSystem.WGS_84, 180, 0)
    val boxes = boundingBox(point, 1000.0)
    boxes should have length 2
    val ((bottomLeft1, topRight1), (bottomLeft2, topRight2)) = (boxes.head, boxes(1))
    bottomLeft1.coordinate()(0) shouldBe >(0.0)
    topRight1.coordinate()(0) should be(180)
    bottomLeft2.coordinate()(0) should be(-180)
    topRight2.coordinate()(0) shouldBe <(0.0)

    bottomLeft1.coordinate()(1) should be(bottomLeft2.coordinate()(1))
    topRight1.coordinate()(1) should be(topRight2.coordinate()(1))
  }

  test("bounding box passing the north-pole") {
    val point = Values.pointValue(CoordinateReferenceSystem.WGS_84, 0, 89.99)
    val boxes = boundingBox(point, 1000_000)
    boxes should have length 1
    val (bottomLeft, topRight) = boxes.head
    bottomLeft.coordinate()(0) shouldBe -180
    bottomLeft.coordinate()(1) shouldBe <(89.99)
    topRight.coordinate()(0) shouldBe 180
    topRight.coordinate()(1) shouldBe 90
  }

  test("bounding box passing the south-pole") {
    val point = Values.pointValue(CoordinateReferenceSystem.WGS_84, 0, -89.99)
    val boxes = boundingBox(point, 1000_000)
    boxes should have length 1
    val (bottomLeft, topRight) = boxes.head
    bottomLeft.coordinate()(0) shouldBe -180
    bottomLeft.coordinate()(1) shouldBe -90
    topRight.coordinate()(0) shouldBe 180
    topRight.coordinate()(1) shouldBe >(-89.99)
  }

  test("bounding box passing both poles") {
    val point = Values.pointValue(CoordinateReferenceSystem.WGS_84, 0, 0)
    val boxes = boundingBox(point, 2.1 * EARTH_RADIUS_METERS)
    boxes should have length 1
    val (bottomLeft, topRight) = boxes.head
    // the world is not enough
    bottomLeft.coordinate()(0) shouldBe -180
    bottomLeft.coordinate()(1) shouldBe -90
    topRight.coordinate()(0) shouldBe 180
    topRight.coordinate()(1) shouldBe 90
  }

  test("distance should account for wraparound in longitude in WGS84") {
    val farWest = Values.pointValue(CoordinateReferenceSystem.WGS_84, -179.99, 0)
    val farEast = Values.pointValue(CoordinateReferenceSystem.WGS_84, 179.99, 0)

    distance(farEast, farWest) should be < EARTH_RADIUS_METERS / 100.0
  }

  test("bounding box including the north pole should be extended to all longitudes in WGS84") {
    val farNorth = Values.pointValue(CoordinateReferenceSystem.WGS_84, 0, 90.0)
    val (bottomLeft, topRight) = boundingBoxLengthOne(farNorth, 100.0)
    bottomLeft.coordinate()(0) should be(-180)
    topRight.coordinate()(0) should be(180)
    topRight.coordinate()(1) should be(90)
  }

  test("bounding box including the south pole should be extended to all longitudes in WGS84") {
    val farSouth = Values.pointValue(CoordinateReferenceSystem.WGS_84, 0, -90.0)
    val (bottomLeft, topRight) = boundingBoxLengthOne(farSouth, 100.0)
    bottomLeft.coordinate()(0) should be(-180)
    bottomLeft.coordinate()(1) should be(-90)
    topRight.coordinate()(0) should be(180)
  }

  test("distance should account for poles in WGS84") {
    implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.0001)
    val np = Values.pointValue(CoordinateReferenceSystem.WGS_84, 0, -90)
    val alsoNP = Values.pointValue(CoordinateReferenceSystem.WGS_84, -180, -90)

    distance(np, alsoNP) should equal(0.0)
  }

  test("distance zero bounding box returns same point in WGS84-3D") {
    val point = Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, 0, 0, 0)
    val (bottomLeft, topRight) = boundingBoxLengthOne(point, 0.0)
    bottomLeft should equal(point)
    topRight should equal(point)
  }

  test("bounding box touching the date line from west in WGS84-3D") {
    val point = Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, -180, 0, 0)
    val boxes = boundingBox(point, 1000.0)
    boxes should have length 2
    val ((bottomLeft1, topRight1), (bottomLeft2, topRight2)) = (boxes.head, boxes(1))
    bottomLeft1.coordinate()(0) shouldBe >(0.0)
    topRight1.coordinate()(0) should be(180)
    bottomLeft2.coordinate()(0) should be(-180)
    topRight2.coordinate()(0) shouldBe <(0.0)

    bottomLeft1.coordinate()(1) should be(bottomLeft2.coordinate()(1))
    topRight1.coordinate()(1) should be(topRight2.coordinate()(1))

    bottomLeft1.coordinate()(2) should be(-1000)
    bottomLeft2.coordinate()(2) should be(-1000)
    topRight1.coordinate()(2) should be(1000)
    topRight2.coordinate()(2) should be(1000)
  }

  test("bounding box touching the date line from east in WGS84-3D") {
    val point = Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, 180, 0, 0)
    val boxes = boundingBox(point, 1000.0)
    boxes should have length 2
    val ((bottomLeft1, topRight1), (bottomLeft2, topRight2)) = (boxes.head, boxes(1))
    bottomLeft1.coordinate()(0) shouldBe >(0.0)
    topRight1.coordinate()(0) should be(180)
    bottomLeft2.coordinate()(0) should be(-180)
    topRight2.coordinate()(0) shouldBe <(0.0)

    bottomLeft1.coordinate()(1) should be(bottomLeft2.coordinate()(1))
    topRight1.coordinate()(1) should be(topRight2.coordinate()(1))

    bottomLeft1.coordinate()(2) should be(-1000)
    bottomLeft2.coordinate()(2) should be(-1000)
    topRight1.coordinate()(2) should be(1000)
    topRight2.coordinate()(2) should be(1000)
  }

  test("distance should account for wraparound in longitude in WGS84-3D") {
    val farWest = Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, -179.99, 0, 0)
    val farEast = Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, 179.99, 0, 0)

    distance(farEast, farWest) should be < EARTH_RADIUS_METERS / 100.0
  }

  test("bounding box including the north pole should be extended to all longitudes in WGS84-3D") {
    val farNorth = Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, 0, 90.0, 0)
    val (bottomLeft, topRight) = boundingBoxLengthOne(farNorth, 100.0)
    bottomLeft.coordinate()(0) should be(-180)
    topRight.coordinate()(0) should be(180)
    topRight.coordinate()(1) should be(90)
  }

  test("bounding box including the south pole should be extended to all longitudes in WGS84-3D") {
    val farSouth = Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, 0, -90.0, 0)
    val (bottomLeft, topRight) = boundingBoxLengthOne(farSouth, 100.0)
    bottomLeft.coordinate()(0) should be(-180)
    bottomLeft.coordinate()(1) should be(-90)
    topRight.coordinate()(0) should be(180)
  }

  test("distance should account for poles in WGS84-3D") {
    implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.0001)
    val np = Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, 0, -90, 0)
    val alsoNP = Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, -180, -90, 0)

    distance(np, alsoNP) should equal(0.0)
  }

  test("bounding box should gives reasonable results in WGS84") {
    implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.0001)
    val malmo = Values.pointValue(CoordinateReferenceSystem.WGS_84, 13.0, 56.0)
    val (bottomLeft, topRight) = boundingBoxLengthOne(malmo, 1000.0)
    bottomLeft.coordinate()(0) should equal(12.984)
    bottomLeft.coordinate()(1) should equal(55.991)
    topRight.coordinate()(0) should equal(13.016)
    topRight.coordinate()(1) should equal(56.009)
  }

  test("bounding box should consider height in WGS84-3D") {
    implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.0001)
    val malmo = Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, 13.0, 56.0, 1000)
    val (bottomLeft, topRight) = boundingBoxLengthOne(malmo, 1000.0)
    bottomLeft.coordinate()(0) should equal(12.984)
    bottomLeft.coordinate()(1) should equal(55.991)
    bottomLeft.coordinate()(2) should equal(0.0)
    topRight.coordinate()(0) should equal(13.016)
    topRight.coordinate()(1) should equal(56.009)
    topRight.coordinate()(2) should equal(2000.0)
  }

  test("distance with WGS-84-3D should work with opposite points on the earth surface") {
    val high = Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, -90, 0, 0)
    val low = Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, 90, 0, 0)

    distance(high, low) should equal(Math.PI * EARTH_RADIUS_METERS)
  }

  test("distance with WGS-84-3D should work with opposite points and high heights") {
    val height = 35786000 // geostationary orbit height
    val high = Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, -90, 0, height)
    val low = Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, 90, 0, height)

    distance(high, low) should equal(Math.PI * (EARTH_RADIUS_METERS + height))
  }

  private def testBoundingBox(makePoint: (Double, Double) => PointValue): Unit = {
    val southPole = makePoint(0, -90)
    val northPole = makePoint(0, 90)

    val points =
      for (x <- -180 to 180 by 36; y <- -75 to 75 by 30) yield {
        makePoint(x.toDouble, y.toDouble)
      }
    val distances = Seq(1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0, 1000000.0)

    for (point <- points; distance <- distances) {
      val calculator = point.getCoordinateReferenceSystem.getCalculator
      withClue(s"Calculating bounding box with distance $distance of $point\n") {
        val boxes = boundingBox(point, distance)
        var minLat = 180.0
        var maxLat = -180.0
        var minLong = 90.0
        var maxLong = -90.0

        // Test that points on the circle lie inside the bounding box
        for (brng <- BigDecimal(0) to 2.0 * Math.PI by 0.01) {
          val dest = destinationPoint(point, distance, brng.doubleValue)
          dest should beInsideOneBoundingBox(boxes, tolerant = true)
          val destLat = dest.coordinate()(1)
          val destLong = dest.coordinate()(0)

          if (destLat < minLat) minLat = destLat
          if (destLat > maxLat) maxLat = destLat
          if (destLong < minLong) minLong = destLong
          if (destLong > maxLong) maxLong = destLong
        }
        val southPoleIncluded = calculator.distance(point, southPole) <= distance
        val northPoleIncluded = calculator.distance(point, northPole) <= distance

        // Test that values slightly further apart are not in the bounding box
        val delta = 1.0

        if (!southPoleIncluded) {
          makePoint(minLong, minLat - delta) shouldNot beInsideOneBoundingBox(boxes)
        }
        if (!northPoleIncluded) {
          makePoint(maxLong, maxLat + delta) shouldNot beInsideOneBoundingBox(boxes)
        }

        // Special cases where poles are included
        if (northPoleIncluded) {
          boxes should have length 1
          val (bottomLeft, topRight) = boxes.head

          bottomLeft.coordinate()(0) should be(-180)
          topRight.coordinate()(0) should be(180)
          topRight.coordinate()(1) should be(90)
        } else if (southPoleIncluded) {
          boxes should have length 1
          val (bottomLeft, topRight) = boxes.head

          bottomLeft.coordinate()(0) should be(-180)
          bottomLeft.coordinate()(1) should be(-90)
          topRight.coordinate()(0) should be(180)
        }
      }
    }
  }

  private def insideBoundingBox(
    point: PointValue,
    bottomLeft: PointValue,
    topRight: PointValue,
    tolerant: Boolean
  ): Boolean = {
    val doubleEquality =
      if (tolerant) TolerantNumerics.tolerantDoubleEquality(0.0001)
      else new Equality[Double] {
        def areEqual(a: Double, b: Any): Boolean = {
          b match {
            case bDouble: Double => a == bDouble
            case _               => false
          }
        }
      }

    val lat = point.coordinate()(1)
    val long = point.coordinate()(0)

    val blLat = bottomLeft.coordinate()(1)
    val trLat = topRight.coordinate()(1)
    val blLong = bottomLeft.coordinate()(0)
    val trLong = topRight.coordinate()(0)

    (lat > blLat || doubleEquality.areEquivalent(lat, blLat)) &&
    (lat < trLat || doubleEquality.areEquivalent(lat, trLat)) &&
    (long > blLong || doubleEquality.areEquivalent(long, blLong)) &&
    (long < trLong || doubleEquality.areEquivalent(long, trLong))
  }

  private def beInsideOneBoundingBox(
    boxes: Seq[(PointValue, PointValue)],
    tolerant: Boolean = false
  ): Matcher[PointValue] = new Matcher[PointValue] {

    override def apply(point: PointValue): MatchResult = {
      MatchResult(
        matches =
          boxes.exists { case (bottomLeft, topRight) => insideBoundingBox(point, bottomLeft, topRight, tolerant) },
        rawFailureMessage = s"$point should be inside one of $boxes, but was not.",
        rawNegatedFailureMessage = s"$point should not be inside one of $boxes, but was."
      )
    }
  }

  // from https://www.movable-type.co.uk/scripts/latlong.html
  private def destinationPoint(startingPoint: PointValue, d: Double, brng: Double): PointValue = {
    if (d == 0.0) {
      return startingPoint
    }

    val lat1 = Math.toRadians(startingPoint.coordinate()(1))
    val long1 = Math.toRadians(startingPoint.coordinate()(0))
    val R = EARTH_RADIUS_METERS
    val lat2 = Math.asin(Math.sin(lat1) * Math.cos(d / R) + Math.cos(lat1) * Math.sin(d / R) * Math.cos(brng))
    val long2 = long1 + Math.atan2(
      Math.sin(brng) * Math.sin(d / R) * Math.cos(lat1),
      Math.cos(d / R) - Math.sin(lat1) * Math.sin(lat2)
    )
    val normLong2 = (long2 + 3 * Math.PI) % (2 * Math.PI) - Math.PI

    Values.pointValue(CoordinateReferenceSystem.WGS_84, Math.toDegrees(normLong2), Math.toDegrees(lat2))
  }

}
