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
package org.neo4j.cypher.internal.compiler

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.util.test_helpers.CypherScalaCheckDrivenPropertyChecks
import org.neo4j.values.storable.CoordinateReferenceSystem
import org.neo4j.values.storable.DateTimeValue
import org.neo4j.values.storable.DateValue
import org.neo4j.values.storable.DoubleValue
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.storable.LocalDateTimeValue
import org.neo4j.values.storable.LocalTimeValue
import org.neo4j.values.storable.LongValue
import org.neo4j.values.storable.PointValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.TimeValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.utils.TemporalUtil
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.scalacheck.Shrink
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

import java.time.ZoneId
import java.time.ZoneOffset
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.SetHasAsScala

/**
 * After a failure, do this to reproduce with the actual values that caused the error:
 *
 * place a single call to testOperator after the for-loop. As an example
 * {{{
 * testOperator("=", ValueSetup[DateTimeValue]("dateTimes", Gen.oneOf(Seq(
 *     DateTimeValue.parse("1901-12-13T20:45:52Z", null),
 *     DateTimeValue.parse("1901-12-13T20:45:52Z[Europe/Brussels]", null)
 * )), x => x.sub(oneDay), x => x.add(oneDay)))
 * }}}
 *
 */
class SemanticIndexAcceptanceTest extends ExecutionEngineFunSuite with CypherScalaCheckDrivenPropertyChecks {

  // we don't want scala check to shrink strings since it hides the actual error
  implicit val dontShrink: Shrink[String] = Shrink(s => Stream.empty)

  private val allNonGeographicCRS: Map[Int, Array[CoordinateReferenceSystem]] =
    CoordinateReferenceSystem.all().asScala.filterNot(crs => crs.isGeographic).toArray.groupBy(_.getDimension)
  private val allNonGeographicCRSDimensions = allNonGeographicCRS.keys.toArray
  private val oneDay = DurationValue.duration(0, 1, 0, 0)
  private val oneSecond = DurationValue.duration(0, 0, 1, 0)
  private val timeZones: Seq[ZoneId] = ZoneId.getAvailableZoneIds.asScala.toSeq.map(ZoneId.of)
  private val MAX_NANOS_PER_DAY = 86399999999999L

  // ----------------
  // the actual test
  // ----------------

  for {
    valueGen <- List(
      ValueSetup[LongValue]("longs", longGen, x => x.minus(1L), x => x.plus(1L)),
      ValueSetup[DoubleValue]("doubles", doubleGen, x => x.minus(0.1), x => x.plus(0.1)),
      ValueSetup[TextValue](
        "strings",
        textGen,
        changeLastChar(c => (c - 1).toChar),
        changeLastChar(c => (c + 1).toChar)
      ),
      ValueSetup[PointValue]("geometric points", pointGen, modifyPoint(_ - 0.1), modifyPoint(_ + 0.1)),
      ValueSetup[PointValue]("2d geographic points", wgs84_2D_pointGen, modifyPoint(_ - 0.1), modifyPoint(_ + 0.1)),
      ValueSetup[PointValue]("3d geographic points", wgs84_3D_pointGen, modifyPoint(_ - 0.1), modifyPoint(_ + 0.1)),
      ValueSetup[DateValue]("dates", dateGen, x => x.sub(oneDay), x => x.add(oneDay)),
      ValueSetup[DateTimeValue]("dateTimes", dateTimeGen, x => x.sub(oneDay), x => x.add(oneDay)),
      ValueSetup[LocalDateTimeValue]("localDateTimes", localDateTimeGen, x => x.sub(oneDay), x => x.add(oneDay)),
      ValueSetup[TimeValue]("times", timeGen, x => x.sub(oneSecond), x => x.add(oneSecond)),
      ValueSetup[LocalTimeValue]("localTimes", localTimeGen, x => x.sub(oneSecond), x => x.add(oneSecond))
    )
    bound <- List("<", "<=", "=", ">", ">=")
  } {
    testOperator(bound, valueGen)
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    graph.createNodeIndex("Label", "indexed")
    graph.withTx(tx =>
      tx.schema().awaitIndexesOnline(30, TimeUnit.SECONDS)
    )
    givenTx {
      for (_ <- 1 to 1000) createLabeledNode("Label")
    }
  }

  /**
   * Value distribution to test. Allow value generation. Can also provide a slightly smaller
   * or larger version of a value, which allows testing around a property bound.
   */
  case class ValueSetup[T <: Value](name: String, generator: Gen[T], lessThan: T => T, moreThan: T => T)

  // GENERATORS

  def longGen: Gen[LongValue] =
    for (x <- Gen.chooseNum(Long.MinValue + 1, Long.MaxValue - 1)) yield Values.longValue(x)

  def doubleGen: Gen[DoubleValue] =
    for (x <- arbitrary[Double]) yield Values.doubleValue(x)

  def textGen: Gen[TextValue] =
    for (x <- Gen.alphaStr) yield Values.stringValue(x)

  def pointGen: Gen[PointValue] =
    for {
      dimension <- Gen.oneOf(allNonGeographicCRSDimensions)
      coordinates <- Gen.listOfN(dimension, arbitrary[Double].retryUntil(java.lang.Double.isFinite(_)))
      crs <- Gen.oneOf(allNonGeographicCRS(dimension))
    } yield Values.pointValue(crs, coordinates: _*)

  def wgs84_3D_pointGen: Gen[PointValue] =
    for {
      x <- arbitrary[Double].retryUntil(d => java.lang.Double.isFinite(d) && -180 <= d && d <= 180)
      y <- arbitrary[Double].retryUntil(d => java.lang.Double.isFinite(d) && -90 <= d && d <= 90)
      z <- arbitrary[Double].retryUntil(java.lang.Double.isFinite(_))
    } yield Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, x, y, z)

  def wgs84_2D_pointGen: Gen[PointValue] =
    for {
      x <- arbitrary[Double].retryUntil(d => java.lang.Double.isFinite(d) && -180 <= d && d <= 180)
      y <- arbitrary[Double].retryUntil(d => java.lang.Double.isFinite(d) && -90 <= d && d <= 90)
    } yield Values.pointValue(CoordinateReferenceSystem.WGS_84, x, y)

  def timeGen: Gen[TimeValue] =
    for { // stay one second off min and max time, to allow getting a bigger and smaller value
      nanosOfDayLocal <- Gen.chooseNum(TemporalUtil.NANOS_PER_SECOND, MAX_NANOS_PER_DAY - TemporalUtil.NANOS_PER_SECOND)
      timeZone <- zoneOffsetGen
    } yield TimeValue.time(TemporalUtil.nanosOfDayToUTC(nanosOfDayLocal, timeZone.getTotalSeconds), timeZone)

  def localTimeGen: Gen[LocalTimeValue] =
    for {
      nanosOfDay <- Gen.chooseNum(TemporalUtil.NANOS_PER_SECOND, MAX_NANOS_PER_DAY - TemporalUtil.NANOS_PER_SECOND)
    } yield LocalTimeValue.localTime(nanosOfDay)

  def dateGen: Gen[DateValue] =
    for {
      epochDays <- arbitrary[Int] // we only generate epochDays as an int, to avoid overflowing
      // the limits of the underlying java types
    } yield DateValue.epochDate(epochDays)

  def dateTimeGen: Gen[DateTimeValue] =
    for {
      epochSecondsUTC <- arbitrary[Int]
      nanosOfSecond <- Gen.chooseNum(0, TemporalUtil.NANOS_PER_SECOND - 1)
      timeZone <- Gen.oneOf(zoneIdGen, zoneOffsetGen)
    } yield DateTimeValue.datetime(epochSecondsUTC, nanosOfSecond, timeZone)

  def localDateTimeGen: Gen[LocalDateTimeValue] =
    for {
      epochSeconds <- arbitrary[Int]
      nanosOfSecond <- Gen.chooseNum(0, TemporalUtil.NANOS_PER_SECOND - 1)
    } yield LocalDateTimeValue.localDateTime(epochSeconds, nanosOfSecond)

  def zoneIdGen: Gen[ZoneId] = Gen.oneOf(timeZones)

  def zoneOffsetGen: Gen[ZoneOffset] =
    Gen.chooseNum(-18 * 60, 18 * 60).map(minute => ZoneOffset.ofTotalSeconds(minute * 60))

  /**
   * Test a single value setup and operator
   */
  private def testOperator[T <: Value](operator: String, setup: ValueSetup[T]): Unit = {

    val queryNotUsingIndex =
      s"MATCH (n:Label) WHERE n.nonIndexed $operator $$prop RETURN n, n.nonIndexed AS prop ORDER BY id(n)"
    val queryUsingIndex = s"MATCH (n:Label) WHERE n.indexed $operator $$prop RETURN n, n.indexed AS prop ORDER BY id(n)"

    case object behaveEqualWithAndWithoutIndex extends Matcher[Value] {
      def apply(value: Value): MatchResult = {
        val valueObject = value.asObject()
        val indexedResult = execute(queryUsingIndex, "prop" -> valueObject)
        val nonIndexedResult = execute(queryNotUsingIndex, "prop" -> valueObject)
        indexedResult.executionPlanDescription().toString should include("NodeIndexSeek")
        val result = nonIndexedResult.toList.equals(indexedResult.toList)

        MatchResult(
          result,
          s"Different results with and without index. Without index: ${nonIndexedResult.toList} vs. with index: ${indexedResult.toList}",
          s"Expected different results with and without index but were the same: ${nonIndexedResult.toList}."
        )
      }
    }

    test(s"testing ${setup.name} with n.prop $operator $$argument") {
      forAll(setup.generator) { (propertyValue: T) =>
        createLabeledNode(Map("nonIndexed" -> propertyValue.asObject(), "indexed" -> propertyValue.asObject()), "Label")

        withClue("with TxState\n") {
          propertyValue should behaveEqualWithAndWithoutIndex
          setup.lessThan(propertyValue) should behaveEqualWithAndWithoutIndex
          setup.moreThan(propertyValue) should behaveEqualWithAndWithoutIndex
        }

        withClue("without TxState\n") {
          propertyValue should behaveEqualWithAndWithoutIndex
          setup.lessThan(propertyValue) should behaveEqualWithAndWithoutIndex
          setup.moreThan(propertyValue) should behaveEqualWithAndWithoutIndex
        }
      }
    }
  }

  private def changeLastChar(f: Char => Char)(in: TextValue): TextValue = {
    val str = in.stringValue()
    if (str.isEmpty) in
    else
      Values.stringValue(str.substring(0, in.length - 1) + (str.last - 1).toChar)
  }

  private def modifyPoint(f: Double => Double)(in: PointValue): PointValue =
    Values.pointValue(in.getCoordinateReferenceSystem, in.coordinate().map(f): _*)

}
