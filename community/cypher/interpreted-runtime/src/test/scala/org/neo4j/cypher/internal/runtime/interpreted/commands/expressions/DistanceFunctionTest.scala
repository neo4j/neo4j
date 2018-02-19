package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.storable.{CoordinateReferenceSystem, PointValue, Values}
import org.scalactic.{Equality, TolerantNumerics}
import org.scalatest.matchers.{MatchResult, Matcher}

class DistanceFunctionTest extends CypherFunSuite {

  test("should calculate correct bounding box") {
    val southPole = Values.pointValue(CoordinateReferenceSystem.WGS84, 0, -90)
    val northPole = Values.pointValue(CoordinateReferenceSystem.WGS84, 0, 90)

    val points =
      for (x <- -180.0 to 180.0 by 36.0; y <- -90.0 to 90.0 by 30.0) yield {
        Values.pointValue(CoordinateReferenceSystem.WGS84, x, y)
      }
    val distances = Seq(1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0, 1000000.0)

    for (point <- points; distance <- distances) {
      withClue(s"Calculating bounding box with distance $distance of $point\n") {
        val (bottomLeft, topRight) = HaversinCalculator.boundingBox(point, distance)
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
        val southPoleIncluded = HaversinCalculator(point, southPole) <= distance
        val northPoleIncluded = HaversinCalculator(point, northPole) <= distance


        // Test that values slightly further apart are not in the bounding box
        val delta = 1.0

        if (!southPoleIncluded) {
          Values.pointValue(CoordinateReferenceSystem.WGS84, minLong, minLat - delta) shouldNot beInsideBoundingBox(bottomLeft, topRight)
        }
        if (!northPoleIncluded) {
          Values.pointValue(CoordinateReferenceSystem.WGS84, maxLong, maxLat + delta) shouldNot beInsideBoundingBox(bottomLeft, topRight)
        }
        if (!northPoleIncluded && !southPoleIncluded) {
          Values.pointValue(CoordinateReferenceSystem.WGS84, minLong - delta, minLat) shouldNot beInsideBoundingBox(bottomLeft, topRight)
          Values.pointValue(CoordinateReferenceSystem.WGS84, maxLong + delta, maxLat) shouldNot beInsideBoundingBox(bottomLeft, topRight)
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

  test("distance zero bounding box returns same point") {
    val point = Values.pointValue(CoordinateReferenceSystem.WGS84, 0, 0)
    val (bottomLeft, topRight) = HaversinCalculator.boundingBox(point, 0.0)
    bottomLeft should equal(point)
    topRight should equal(point)
  }

  test("bounding box touching the date line should extend to the whole longitude") {
    val point = Values.pointValue(CoordinateReferenceSystem.WGS84, -180, 0)
    val (bottomLeft, topRight) = HaversinCalculator.boundingBox(point, 1000.0)
    bottomLeft.coordinate()(0) should be(-180)
    topRight.coordinate()(0) should be(180)
  }

  test("distance should account for wraparound in longitude") {
    val farWest = Values.pointValue(CoordinateReferenceSystem.WGS84, -179.99, 0)
    val farEast = Values.pointValue(CoordinateReferenceSystem.WGS84, 179.99, 0)

    HaversinCalculator((farEast, farWest)) should be < HaversinCalculator.EARTH_RADIUS_METERS
  }

  test("distance should account for poles") {
    implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.0001)
    val np = Values.pointValue(CoordinateReferenceSystem.WGS84, 0, -90)
    val alsoNP = Values.pointValue(CoordinateReferenceSystem.WGS84, -180, -90)

    HaversinCalculator((np, alsoNP)) should equal(0.0)
  }

  private def insideBoundingBox(point: PointValue, bottomLeft: PointValue, topRight: PointValue, tolerant: Boolean): Boolean = {
    val doubleEquality =
      if(tolerant) TolerantNumerics.tolerantDoubleEquality(0.0001)
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
    val R = HaversinCalculator.EARTH_RADIUS_METERS
    val lat2 = Math.asin(Math.sin(lat1) * Math.cos(d / R) + Math.cos(lat1) * Math.sin(d / R) * Math.cos(brng))
    val long2 = long1 + Math.atan2(Math.sin(brng) * Math.sin(d / R) * Math.cos(lat1), Math.cos(d / R) - Math.sin(lat1) * Math.sin(lat2))
    val normLong2 = (long2 + 3 * Math.PI) % (2 * Math.PI) - Math.PI

    Values.pointValue(CoordinateReferenceSystem.WGS84, Math.toDegrees(normLong2), Math.toDegrees(lat2))
  }

}
