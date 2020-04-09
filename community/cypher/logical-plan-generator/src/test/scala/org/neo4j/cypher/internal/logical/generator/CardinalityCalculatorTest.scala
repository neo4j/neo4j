/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.logical.generator

import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.DoNotIncludeTies
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.attribution.Default
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.scalatest.FunSuite
import org.scalatest.Matchers

class CardinalityCalculatorTest extends FunSuite with Matchers {

  private val pos = InputPosition.NONE

  private val defaultSourceCardinality = Cardinality(123)

  private val defaultState = LogicalPlanGenerator.State(Map.empty, Map.empty).copy(
    cardinalities = new Cardinalities with Default[LogicalPlan, Cardinality] {
      override protected def defaultValue: Cardinality = defaultSourceCardinality
    }
  )

  private implicit val defaultIdGen: IdGen = defaultState.idGen

  test("ProduceResult") {
    val plan = ProduceResult(Argument(), Seq.empty)

    val c = CardinalityCalculator.produceResultCardinality(plan, defaultState, new TestGraphStatistics, Map.empty)
    c should equal(defaultSourceCardinality)
  }

  test("AllNodesScan") {
    val plan = AllNodesScan("a", Set.empty)
    val stats = new TestGraphStatistics {
      override def nodesAllCardinality(): Cardinality = Cardinality(321)
    }

    val c = CardinalityCalculator.allNodesScanCardinality(plan, defaultState, stats, Map.empty)
    c should equal(stats.nodesAllCardinality())
  }

  test("AllNodesScan under Apply") {
    val plan = AllNodesScan("a", Set.empty)
    val multiplier = Cardinality(10)
    val state = defaultState.pushLeafCardinalityMultiplier(multiplier)
    val stats = new TestGraphStatistics {
      override def nodesAllCardinality(): Cardinality = Cardinality(321)
    }

    val c = CardinalityCalculator.allNodesScanCardinality(plan, state, stats, Map.empty)
    c should equal(stats.nodesAllCardinality() * multiplier)
  }

  test("NodeByLabelScan") {
    val plan = NodeByLabelScan("a", LabelName("Label")(pos), Set.empty, IndexOrderNone)
    val labelCardinality = Cardinality(321)
    val labelIds = Map("Label" -> 1)
    val stats = new TestGraphStatistics {
      override def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality = {
        labelId.get.id should be(labelIds("Label"))
        labelCardinality
      }
    }

    val c = CardinalityCalculator.nodeByLabelScanCardinality(plan, defaultState, stats, labelIds)
    c should equal(labelCardinality)
  }

  test("NodeByLabelScan under Apply") {
    val plan = NodeByLabelScan("a", LabelName("Label")(pos), Set.empty, IndexOrderNone)
    val multiplier = Cardinality(10)
    val labelCardinality = Cardinality(321)
    val state = defaultState.pushLeafCardinalityMultiplier(multiplier)
    val labelIds = Map("Label" -> 1)
    val stats = new TestGraphStatistics {
      override def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality ={
        labelId.get.id should be(labelIds("Label"))
        labelCardinality
      }
    }

    val c = CardinalityCalculator.nodeByLabelScanCardinality(plan, state, stats, labelIds)
    c should equal(labelCardinality * multiplier)
  }

  test("Argument") {
    val plan = Argument()
    val c = CardinalityCalculator.argumentCardinality(plan, defaultState, new TestGraphStatistics, Map.empty)
    c should equal(Cardinality.SINGLE)
  }

  test("Argument under Apply") {
    val plan = Argument()
    val multiplier = Cardinality(10)
    val state = defaultState.pushLeafCardinalityMultiplier(multiplier)

    val c = CardinalityCalculator.argumentCardinality(plan, state, new TestGraphStatistics, Map.empty)
    c should equal(multiplier)
  }

  test("Expand (from)") {
    val rels = Seq("A", "B", "C")
    val relIds = rels.zipWithIndex.toMap

    val allNodesCount = 100
    val individualRelCount = 42

    val state = LogicalPlanGenerator.State(Map.empty, relIds).copy(
      arguments = Set("from"),
      cardinalities = new Cardinalities with Default[LogicalPlan, Cardinality] {
        override protected def defaultValue: Cardinality = defaultSourceCardinality
      }
    )

    val stats = new TestGraphStatistics {
      override def nodesAllCardinality(): Cardinality =
        Cardinality(allNodesCount)

      override def patternStepCardinality(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId]): Cardinality = {
        fromLabel should be(empty)
        toLabel should be(empty)

        relTypeId match {
          case Some(_) => Cardinality(individualRelCount)
          case None => Cardinality(individualRelCount * rels.size)
        }
      }
    }

    for (relNames <- Seq(Seq.empty, Seq("A"), Seq("A", "B")))
      withClue(s"Expand (from)-[$relNames]->(to)") {
        val relTypes = relNames.map(r => RelTypeName(r)(pos))

        val relCount = if (relTypes.isEmpty) rels.size else relTypes.size
        val avgRelsPerNode = (relCount * individualRelCount) / allNodesCount.toDouble

        val plan = Expand(Argument(), "from", SemanticDirection.OUTGOING, relTypes, "to", "rel")

        val expectedAmountApprox = avgRelsPerNode * defaultSourceCardinality.amount
        val Cardinality(actualAmount) = CardinalityCalculator.expandCardinality(plan, state, stats, Map.empty)

        val marginOfError = expectedAmountApprox * 0.01
        actualAmount should equal(expectedAmountApprox +- marginOfError)
      }
  }

  test("Expand (from:Label)") {
    val rels = Seq("A", "B", "C")
    val relIds = rels.zipWithIndex.toMap

    val labelIds = Map("Label" -> 1)

    val allNodesCount = 100
    val labeledNodesCount = allNodesCount / 2
    val individualRelCount = 42

    val state = LogicalPlanGenerator.State(labelIds, relIds).copy(
      arguments = Set("from"),
      cardinalities = new Cardinalities with Default[LogicalPlan, Cardinality] {
        override protected def defaultValue: Cardinality = defaultSourceCardinality
      },
      labelInfo = Map("from" -> Set(LabelName("Label")(pos)))
    )

    val stats = new TestGraphStatistics {
      override def nodesAllCardinality(): Cardinality =
        Cardinality(allNodesCount)

      override def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality = {
        labelId.get.id should be (labelIds("Label"))
        Cardinality(labeledNodesCount)
      }

      override def patternStepCardinality(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId]): Cardinality = {
        fromLabel.get.id should be(labelIds("Label"))
        toLabel should be(empty)

        relTypeId match {
          case Some(_) => Cardinality(individualRelCount)
          case None => Cardinality(individualRelCount * rels.size)
        }
      }
    }

    for (relNames <- Seq(Seq.empty, Seq("A"), Seq("A", "B")))
      withClue(s"Expand (from:Label)-[$relNames]->(to)") {
        val relTypes = relNames.map(r => RelTypeName(r)(pos))

        val relCount = if (relTypes.isEmpty) rels.size else relTypes.size
        val avgRelsPerLabeledNode = (relCount * individualRelCount) / labeledNodesCount.toDouble

        val plan = Expand(Argument(), "from", SemanticDirection.OUTGOING, relTypes, "to", "rel")

        val expectedAmountApprox = avgRelsPerLabeledNode * defaultSourceCardinality.amount
        val Cardinality(actualAmount) = CardinalityCalculator.expandCardinality(plan, state, stats, Map.empty)

        val marginOfError = expectedAmountApprox * 0.01
        actualAmount should equal(expectedAmountApprox +- marginOfError)
      }
  }

  test("Limit amount < node count") {
    val limitAmount = 100
    val plan = Limit(Argument(), SignedDecimalIntegerLiteral(limitAmount.toString)(pos), DoNotIncludeTies)

    val c = CardinalityCalculator.limitCardinality(plan, defaultState, new TestGraphStatistics, Map.empty)
    c should equal(Cardinality(limitAmount))
  }

  test("Limit amount > node count") {
    val limitAmount = 1000
    val plan = Limit(Argument(), SignedDecimalIntegerLiteral(limitAmount.toString)(pos), DoNotIncludeTies)

    val c = CardinalityCalculator.limitCardinality(plan, defaultState, new TestGraphStatistics, Map.empty)
    c should equal(defaultSourceCardinality)
  }

  test("Skip amount < node count") {
    val skipAmount = 100
    val plan = Skip(Argument(), SignedDecimalIntegerLiteral(skipAmount.toString)(pos))

    val c = CardinalityCalculator.skipCardinality(plan, defaultState, new TestGraphStatistics, Map.empty)
    c should equal(defaultSourceCardinality.map(_ - skipAmount))
  }

  test("Skip amount > node count") {
    val skipAmount = 1000
    val plan = Skip(Argument(), SignedDecimalIntegerLiteral(skipAmount.toString)(pos))

    val c = CardinalityCalculator.skipCardinality(plan, defaultState, new TestGraphStatistics, Map.empty)
    c should equal(Cardinality.EMPTY)
  }

  test("Projection") {
    val plan = Projection(Argument(), Map.empty)

    val c = CardinalityCalculator.projectionCardinality(plan, defaultState, new TestGraphStatistics, Map.empty)
    c should equal(defaultSourceCardinality)
  }

  test("Aggregation without grouping") {
    val plan = Aggregation(Argument(), Map.empty, Map.empty)

    val c = CardinalityCalculator.aggregationCardinality(plan, defaultState, new TestGraphStatistics, Map.empty)
    c should equal(Cardinality.SINGLE)
  }

  test("Aggregation with grouping") {
    val plan = Aggregation(Argument(), Map("x" -> CountStar()(pos)), Map.empty)

    val c = CardinalityCalculator.aggregationCardinality(plan, defaultState, new TestGraphStatistics, Map.empty)
    c should equal(Cardinality.sqrt(defaultSourceCardinality))
  }

  test("Apply") {
    val plan = Apply(Argument(), Argument())

    val c = CardinalityCalculator.applyCardinality(plan, defaultState, new TestGraphStatistics, Map.empty)
    c should equal(defaultSourceCardinality)
  }

  test("CartesianProduct") {
    val plan = CartesianProduct(Argument(), Argument())

    val c = CardinalityCalculator.cartesianProductCardinality(plan, defaultState, new TestGraphStatistics, Map.empty)
    c should equal(defaultSourceCardinality * defaultSourceCardinality)
  }

  private class TestGraphStatistics extends GraphStatistics {
    override def nodesAllCardinality(): Cardinality =
      fail()
    override def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality =
      fail()
    override def patternStepCardinality(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId]): Cardinality =
      fail()
    override def uniqueValueSelectivity(index: IndexDescriptor): Option[Selectivity] =
      fail()
    override def indexPropertyExistsSelectivity(index: IndexDescriptor): Option[Selectivity] =
      fail()

    private def fail() = throw new IllegalStateException("Should not have been called in this test.")
  }
}
