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
package org.neo4j.cypher.internal.compiler.v3_4

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.values.storable._
import org.scalacheck.Gen
import org.scalacheck.Arbitrary.arbitrary
import org.scalatest.prop.PropertyChecks

import scala.collection.JavaConversions._

class SemanticIndexAcceptanceTest extends ExecutionEngineFunSuite with PropertyChecks {

  private val allCRS: Map[Int, Array[CoordinateReferenceSystem]] = CoordinateReferenceSystem.all().toArray.groupBy(_.getDimension)
  private val allCRSDimensions = allCRS.keys.toArray
  private val oneDay = DurationValue.duration(0, 1, 0, 0)
  private val MAX_EPOCH_DAYS = 999999999 * 365

  // ----------------
  // the actual test
  // ----------------

  for {
    bound <- List("<", "<=", "=", ">", ">=")
    valueGen <- List(
      ValueSetup[LongValue]("longs", longGen, x => x.minus(1L), x => x.plus(1L)),
      ValueSetup[DoubleValue]("doubles", doubleGen, x => x.minus(0.1), x => x.plus(0.1)),
      ValueSetup[TextValue]("strings", textGen, changeLastChar(c => (c - 1).toChar), changeLastChar(c => (c + 1).toChar)),
//      ValueSetup[PointValue]("points", pointGen, modifyPoint(_ - 0.1), modifyPoint(_ + 0.1)), // TODO: here is a bug
      ValueSetup[DateValue]("dates", dateGen, x => x.sub(oneDay), x => x.add(oneDay))
    )
  } {
    testOperator(bound, valueGen)
  }

  override protected def initTest(): Unit = {
    super.initTest()
    for(_ <- 1 to 1000) createLabeledNode("Label")
  }

  /**
    * Value distribution to test. Allow value generation. Can also provide a slightly smaller
    * or larger version of a value, which allows testing around a property bound.
    */
  case class ValueSetup[T <: Value](name: String, generator:Gen[T], lessThan: T => T, moreThan: T => T)

  // GENERATORS

  def longGen: Gen[LongValue] =
    for (x <- Gen.chooseNum(Long.MinValue+1, Long.MaxValue-1)) yield Values.longValue(x)

  def doubleGen: Gen[DoubleValue] =
    for (x <- arbitrary[Double]) yield Values.doubleValue(x)

  def textGen: Gen[TextValue] =
    for (x <- Gen.alphaStr) yield Values.stringValue(x)

  def pointGen: Gen[PointValue] =
    for {
      dimension <- Gen.oneOf(allCRSDimensions)
      coordinates <- Gen.listOfN(dimension, arbitrary[Double])
      crs <- Gen.oneOf(allCRS(dimension))
    } yield Values.pointValue(crs, coordinates:_*)

  def dateGen: Gen[DateValue] =
    for {
      epochDays <- arbitrary[Int] //Gen.chooseNum(-MAX_EPOCH_DAYS+1, MAX_EPOCH_DAYS-1)?
    } yield DateValue.epochDate(epochDays)

  /**
    * Test a single value setup and operator
    */
  private def testOperator[T <: Value](operator: String, setup: ValueSetup[T]): Unit = {

    val queryNotUsingIndex = s"match (n:Label) where n.nonIndexed $operator {prop} return n order by id(n)"
    val queryUsingIndex = s"match (n:Label) where n.indexed $operator {prop} return n order by id(n)"

    def testValue(queryNotUsingIndex: String, queryUsingIndex: String, value: Value): Unit = {
      val valueObject = value.asObject()
      val indexedResult = execute(queryUsingIndex, "prop" -> valueObject)
      execute(queryNotUsingIndex, "prop" -> valueObject).toList should equal(indexedResult.toList)
      indexedResult.executionPlanDescription().toString should include("NodeIndexSeek")
    }

    test(s"testing ${setup.name} with $operator") {
      graph.createIndex("Label", "indexed")
      forAll(setup.generator) { propertyValue: T =>
        graph.inTx {
          createLabeledNode(Map("nonIndexed" -> propertyValue.asObject(), "indexed" -> propertyValue.asObject()), "Label")

          withClue("with TxState") {
            testValue(queryNotUsingIndex, queryUsingIndex, propertyValue)
            testValue(queryNotUsingIndex, queryUsingIndex, setup.lessThan(propertyValue))
            testValue(queryNotUsingIndex, queryUsingIndex, setup.moreThan(propertyValue))
          }
        }
        withClue("without TxState") {
          testValue(queryNotUsingIndex, queryUsingIndex, propertyValue)
          testValue(queryNotUsingIndex, queryUsingIndex, setup.lessThan(propertyValue))
          testValue(queryNotUsingIndex, queryUsingIndex, setup.moreThan(propertyValue))
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
    Values.pointValue(in.getCoordinateReferenceSystem, in.coordinate().map(f):_*)

}
