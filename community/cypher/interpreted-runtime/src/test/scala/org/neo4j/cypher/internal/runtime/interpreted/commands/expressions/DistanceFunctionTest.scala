/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.storable.{CRSCalculator, CoordinateReferenceSystem, PointValue, Values}
import org.scalactic.{Equality, TolerantNumerics}
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.language.implicitConversions

class DistanceFunctionTest extends CypherFunSuite {

  implicit def javaToScalaPair(pair: org.neo4j.helpers.collection.Pair[PointValue, PointValue]): (PointValue, PointValue) = (pair.first(), pair.other())

  def boundingBox(center: PointValue, distance: Double): (PointValue, PointValue) =
    center.getCoordinateReferenceSystem.getCalculator.boundingBox(center, distance)

  def distance(p1: PointValue, p2: PointValue): Double =
    p1.getCoordinateReferenceSystem.getCalculator.distance(p1, p2)

  test("should calculate correct bounding box for WGS84") {
    testDistanceAndBoundingBox((x, y) => Values.pointValue(CoordinateReferenceSystem.WGS84, x, y))
  }

  test("should calculate correct bounding box for WGS84-3D") {
    testDistanceAndBoundingBox((x, y) => Values.pointValue(CoordinateReferenceSystem.WGS84_3D, x, y, 100))
  }

  private def testDistanceAndBoundingBox(makePoint: (Double, Double) => PointValue): Unit = {
    val southPole = makePoint(0, -90)
    val northPole = makePoint(0, 90)

    val points =
      for (x <- -180.0 to 180.0 by 36.0; y <- -75.0 to 75.0 by 30.0) yield {
        makePoint(x, y)
      }
    val distances = Seq(1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0, 1000000.0)

    for (point <- points; distance <- distances) {
      val calculator = point.getCoordinateReferenceSystem.getCalculator
      withClue(s"Calculating bounding box with distance $distance of $point\n") {
        val (bottomLeft, topRight) = boundingBox(point, distance)
        var minLat = Double.MaxValue
        var maxLat = Double.MinValue
        var minLong = Double.MaxValue
        var maxLong = Double.MinValue

        // Test that points on the circle lie inside the bounding box
        for (brng <- 0.0 to 2.0 * Math.PI by 0.01) {
          val dest = destinationPoint(point, distance, brng)
          dest should beInsideBoundingBox(bottomLeft, topRight, tolerant = true)
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
          makePoint(minLong, minLat - delta) shouldNot beInsideBoundingBox(bottomLeft, topRight)
        }
        if (!northPoleIncluded) {
          makePoint(maxLong, maxLat + delta) shouldNot beInsideBoundingBox(bottomLeft, topRight)
        }
        if (!northPoleIncluded && !southPoleIncluded) {
          makePoint(minLong - delta, minLat) shouldNot beInsideBoundingBox(bottomLeft, topRight)
          makePoint(maxLong + delta, maxLat) shouldNot beInsideBoundingBox(bottomLeft, topRight)
        }

        // Special cases where poles are included
        if (northPoleIncluded) {
          bottomLeft.coordinate()(0) should be(-180)
          topRight.coordinate()(0) should be(180)
          topRight.coordinate()(1) should be(90)
        } else if (southPoleIncluded) {
          bottomLeft.coordinate()(0) should be(-180)
          bottomLeft.coordinate()(1) should be(-90)
          topRight.coordinate()(0) should be(180)
        }
      }
    }
  }

  test("distance zero bounding box returns same point in WGS84") {
    val point = Values.pointValue(CoordinateReferenceSystem.WGS84, 0, 0)
    val (bottomLeft, topRight) = boundingBox(point, 0.0)
    bottomLeft should equal(point)
    topRight should equal(point)
  }

  test("bounding box touching the date line should extend to the whole longitude in WGS84") {
    val point = Values.pointValue(CoordinateReferenceSystem.WGS84, -180, 0)
    val (bottomLeft, topRight) = boundingBox(point, 1000.0)
    bottomLeft.coordinate()(0) should be(-180)
    topRight.coordinate()(0) should be(180)
  }

  test("distance should account for wraparound in longitude in WGS84") {
    val farWest = Values.pointValue(CoordinateReferenceSystem.WGS84, -179.99, 0)
    val farEast = Values.pointValue(CoordinateReferenceSystem.WGS84, 179.99, 0)

    distance(farEast, farWest) should be < CRSCalculator.GeographicCalculator.EARTH_RADIUS_METERS / 100.0
  }

  test("bounding box including the north pole should be extended to all longitudes in WGS84") {
    val farNorth = Values.pointValue(CoordinateReferenceSystem.WGS84, 0, 90.0)
    val (bottomLeft, topRight) = boundingBox(farNorth, 100.0)
    bottomLeft.coordinate()(0) should be(-180)
    topRight.coordinate()(0) should be(180)
    topRight.coordinate()(1) should be(90)
  }

  test("bounding box including the south pole should be extended to all longitudes in WGS84") {
    val farSouth = Values.pointValue(CoordinateReferenceSystem.WGS84, 0, -90.0)
    val (bottomLeft, topRight) = boundingBox(farSouth, 100.0)
    bottomLeft.coordinate()(0) should be(-180)
    bottomLeft.coordinate()(1) should be(-90)
    topRight.coordinate()(0) should be(180)
  }

  test("distance should account for poles in WGS84") {
    implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.0001)
    val np = Values.pointValue(CoordinateReferenceSystem.WGS84, 0, -90)
    val alsoNP = Values.pointValue(CoordinateReferenceSystem.WGS84, -180, -90)

    distance(np, alsoNP) should equal(0.0)
  }

  test("distance zero bounding box returns same point in WGS84-3D") {
    val point = Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 0, 0, 0)
    val (bottomLeft, topRight) = boundingBox(point, 0.0)
    bottomLeft should equal(point)
    topRight should equal(point)
  }

  test("bounding box touching the date line should extend to the whole longitude in WGS84-3D") {
    val point = Values.pointValue(CoordinateReferenceSystem.WGS84_3D, -180, 0, 0)
    val (bottomLeft, topRight) = boundingBox(point, 1000.0)
    bottomLeft.coordinate()(0) should be(-180)
    topRight.coordinate()(0) should be(180)
  }

  test("distance should account for wraparound in longitude in WGS84-3D") {
    val farWest = Values.pointValue(CoordinateReferenceSystem.WGS84_3D, -179.99, 0, 0)
    val farEast = Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 179.99, 0, 0)

    distance(farEast, farWest) should be < CRSCalculator.GeographicCalculator.EARTH_RADIUS_METERS / 100.0
  }

  test("bounding box including the north pole should be extended to all longitudes in WGS84-3D") {
    val farNorth = Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 0, 90.0, 0)
    val (bottomLeft, topRight) = boundingBox(farNorth, 100.0)
    bottomLeft.coordinate()(0) should be(-180)
    topRight.coordinate()(0) should be(180)
    topRight.coordinate()(1) should be(90)
  }

  test("bounding box including the south pole should be extended to all longitudes in WGS84-3D") {
    val farSouth = Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 0, -90.0, 0)
    val (bottomLeft, topRight) = boundingBox(farSouth, 100.0)
    bottomLeft.coordinate()(0) should be(-180)
    bottomLeft.coordinate()(1) should be(-90)
    topRight.coordinate()(0) should be(180)
  }

  test("distance should account for poles in WGS84-3D") {
    implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.0001)
    val np = Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 0, -90, 0)
    val alsoNP = Values.pointValue(CoordinateReferenceSystem.WGS84_3D, -180, -90, 0)

    distance(np, alsoNP) should equal(0.0)
  }

  test("bounding box should gives reasonable results in WGS84") {
    implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.0001)
    val malmo = Values.pointValue(CoordinateReferenceSystem.WGS84, 13.0, 56.0)
    val (bottomLeft, topRight) = boundingBox(malmo, 1000.0)
    bottomLeft.coordinate()(0) should equal(12.984)
    bottomLeft.coordinate()(1) should equal(55.991)
    topRight.coordinate()(0) should equal(13.016)
    topRight.coordinate()(1) should equal(56.009)
  }

  test("bounding box should consider height in WGS84-3D") {
    implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.0001)
    val malmo = Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 13.0, 56.0, 1000)
    val (bottomLeft, topRight) = boundingBox(malmo, 1000.0)
    bottomLeft.coordinate()(0) should equal(12.984)
    bottomLeft.coordinate()(1) should equal(55.991)
    bottomLeft.coordinate()(2) should equal(0.0)
    topRight.coordinate()(0) should equal(13.016)
    topRight.coordinate()(1) should equal(56.009)
    topRight.coordinate()(2) should equal(2000.0)
  }

  private def insideBoundingBox(point: PointValue, bottomLeft: PointValue, topRight: PointValue, tolerant: Boolean): Boolean = {
    val doubleEquality =
      if (tolerant) TolerantNumerics.tolerantDoubleEquality(0.0001)
      else new Equality[Double] {
        def areEqual(a: Double, b: Any): Boolean = {
          b match {
            case bDouble: Double => a == bDouble
            case _ => false
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

  private def beInsideBoundingBox(bottomLeft: PointValue, topRight: PointValue, tolerant: Boolean = false): Matcher[PointValue] = new Matcher[PointValue] {
    override def apply(point: PointValue): MatchResult = {
      MatchResult(
        matches = insideBoundingBox(point, bottomLeft, topRight, tolerant),
        rawFailureMessage = s"$point should be inside $bottomLeft -> $topRight, but was not.",
        rawNegatedFailureMessage = s"$point should not be inside $bottomLeft -> $topRight, but was.")
    }
  }

  // from https://www.movable-type.co.uk/scripts/latlong.html
  private def destinationPoint(startingPoint: PointValue, d: Double, brng: Double): PointValue = {
    if (d == 0.0) {
      return startingPoint
    }

    val lat1 = Math.toRadians(startingPoint.coordinate()(1))
    val long1 = Math.toRadians(startingPoint.coordinate()(0))
    val R = CRSCalculator.GeographicCalculator.EARTH_RADIUS_METERS
    val lat2 = Math.asin(Math.sin(lat1) * Math.cos(d / R) + Math.cos(lat1) * Math.sin(d / R) * Math.cos(brng))
    val long2 = long1 + Math.atan2(Math.sin(brng) * Math.sin(d / R) * Math.cos(lat1), Math.cos(d / R) - Math.sin(lat1) * Math.sin(lat2))
    val normLong2 = (long2 + 3 * Math.PI) % (2 * Math.PI) - Math.PI

    Values.pointValue(CoordinateReferenceSystem.WGS84, Math.toDegrees(normLong2), Math.toDegrees(lat2))
  }

}
