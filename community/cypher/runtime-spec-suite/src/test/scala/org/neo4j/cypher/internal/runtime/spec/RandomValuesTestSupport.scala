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
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.values.storable.RandomValues
import org.neo4j.values.storable.RandomValuesUtils
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.ValueType
import org.scalatest.Failed
import org.scalatest.Outcome
import org.scalatest.TestSuite
import org.scalatest.TestSuiteMixin

import scala.util.Random

trait RandomValuesTestSupport[CONTEXT <: RuntimeContext] extends TestSuiteMixin with TestSuite {
  self: RuntimeTestSuite[CONTEXT] =>

  private val initialSeedSeed = Random.nextLong()

  // Used to seed the random number generator of the individual tests.
  lazy val seedRandom = new Random(initialSeedSeed)

  private[this] var _initialSeed: Long = _
  private[this] var _random: Random = _
  private[this] var _randomValues: RandomValues = _

  def initialSeed: Long = {
    while (_initialSeed == 0L) {
      _initialSeed = seedRandom.nextLong()
    }
    _initialSeed
  }

  def setInitialSeed(seed: Long): Unit = {
    _initialSeed = seed
  }

  // Only initialize the test fixtures that are used.
  def random: Random = {
    if (_random == null) {
      _random = new Random(initialSeed)
    }
    _random
  }

  // Not all tests run with a graphDb available. For these tests, just assume that all values are permitted
  def randomValues: RandomValues = {
    if (_randomValues == null) {
      _randomValues = RandomValues.create(
        new java.util.Random(initialSeed),
        Option(graphDb).map(
          RandomValuesUtils.selectStorageEngineDependentConfigurationBuilder
        ).getOrElse(RandomValues.newConfigurationBuilder)
          .maxCodePoint(10_000)
          .build
      )
    }
    _randomValues
  }

  // Scala compat
  def randomValue(valueType: ValueType): Value = randomValues.nextValueOfType(valueType)

  def randomValues(size: Int, valueTypes: ValueType*): Array[Value] =
    randomValues.nextValuesOfTypes(size, valueTypes: _*)
  def randomAmong[T](values: Seq[T]): T = values(randomValues.nextInt(values.size))
  def shuffle[T](values: Seq[T]): Seq[T] = random.shuffle(values)

  abstract override def withFixture(test: NoArgTest): Outcome = {
    val clue = new { // Trick to defer evaluation since initialSeed is not available before the test is run.
      override def toString: String =
        s"""
           |${classOf[RandomValuesTestSupport[CONTEXT]].getSimpleName} test failed with initial seed: ${initialSeed}L
           |To reproduce, put the following line at the top of the test that failed:
           |setInitialSeed(${initialSeed}L)
           |
           |""".stripMargin
    }
    withClue(clue) {
      try {
        val outcome = super.withFixture(test)
        outcome match {
          case Failed(_: org.scalatest.exceptions.ModifiableMessage[_]) =>
          // Clue will be included in the exception by the wrapping withClue
          case Failed(_) =>
            // Print clue to stderr since withClue won't include it
            System.err.println(clue)
          case _ =>
          // Do nothing
        }
        outcome
      } catch {
        case e: Throwable if !e.isInstanceOf[org.scalatest.exceptions.ModifiableMessage[_]] =>
          // Print clue to stderr
          System.err.println(clue)
          throw e
      }
    }
  }
}
