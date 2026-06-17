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

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.util.test_helpers.WithFixtureClue
import org.neo4j.internal.kernel.api.procs.Neo4jTypes
import org.neo4j.internal.kernel.api.procs.QualifiedName
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature
import org.neo4j.kernel.api.procedure.CallableUserFunction.BasicUserFunction
import org.neo4j.kernel.api.procedure.Context
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.RandomValues
import org.neo4j.values.storable.RandomValuesUtils
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.ValueType
import org.neo4j.values.storable.Values

import scala.util.Random
import scala.util.Try

trait RandomValuesTestSupport[CONTEXT <: RuntimeContext] extends WithFixtureClue {
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

  protected def effectivePipelinedBatchSizes: Option[(Int, Int)] =
    Option(graphDb).flatMap { db =>
      Try {
        val config = db.asInstanceOf[GraphDatabaseFacade].getDependencyResolver.resolveDependency(classOf[Config])
        (
          config.get(GraphDatabaseInternalSettings.cypher_pipelined_batch_size_small).intValue(),
          config.get(GraphDatabaseInternalSettings.cypher_pipelined_batch_size_big).intValue()
        )
      }.toOption
    }

  // Scala compat
  def randomValue(valueType: ValueType): Value = randomValues.nextValueOfType(valueType)

  def randomValues(size: Int, valueTypes: ValueType*): Array[Value] =
    randomValues.nextValuesOfTypes(size, valueTypes: _*)
  def randomAmong[T](values: Seq[T]): T = values(randomValues.nextInt(values.size))
  def shuffle[T](values: Seq[T]): Seq[T] = random.shuffle(values)

  override protected def testFailureClue: AnyRef =
    new { // Trick to defer evaluation since initialSeed is not available before the test is run.
      override def toString: String =
        RandomValuesTestSupport.reproductionClue(initialSeed, effectivePipelinedBatchSizes)
    }

  def restartTxWithSeededRandFunction(name: String = "test.seededRand"): String = {
    val seededRandom = new java.util.Random(random.nextLong())
    registerFunction(new BasicUserFunction(
      UserFunctionSignature.functionSignature(new QualifiedName(name))
        .out(Neo4jTypes.NTFloat).threadSafe().build()
    ) {
      override def apply(ctx: Context, input: Array[AnyValue]): AnyValue =
        Values.doubleValue(seededRandom.nextDouble())
    })
    restartTx()
    name
  }
}

object RandomValuesTestSupport {

  def reproductionClue(initialSeed: Long, pipelinedBatchSizes: Option[(Int, Int)]): String = {
    val lines =
      Seq(
        "",
        s"RandomValuesTestSupport test failed with initial seed: ${initialSeed}L",
        "To reproduce, put the following line at the top of the test that failed:",
        s"setInitialSeed(${initialSeed}L)"
      ) ++ pipelinedBatchSizes.map { case (small, big) =>
        s"effective pipelined batch size: small=$small, big=$big"
      } ++ Seq("", "")
    lines.mkString("\n")
  }
}
