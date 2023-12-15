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
package org.neo4j.cypher.internal.logical.generator

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.NotImplementedPlanContext
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults
import org.neo4j.cypher.internal.compiler.test_helpers.TestGraphStatistics
import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ManySeekableArgs
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SemiApply
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.InstrumentedGraphStatistics
import org.neo4j.cypher.internal.planner.spi.MutableGraphStatisticsSnapshot
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.attribution.Default
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.internal.schema.constraints.SchemaValueType

class CardinalityCalculatorTest extends CypherFunSuite with AstConstructionTestSupport {

  private val defaultSourceCardinality = Cardinality(123)

  private val defaultState = LogicalPlanGenerator.State(Map.empty, Map.empty).copy(
    cardinalities = new Cardinalities with Default[LogicalPlan, Cardinality] {
      override protected def defaultValue: Cardinality = defaultSourceCardinality
    }
  )

  implicit private val defaultIdGen: IdGen = defaultState.idGen

  private def notImplementedPlanContext(stats: GraphStatistics) = {
    new NotImplementedPlanContext {
      override def statistics: InstrumentedGraphStatistics = InstrumentedGraphStatistics(
        stats,
        new MutableGraphStatisticsSnapshot()
      )

      override def propertyIndexesGetAll(): Iterator[IndexDescriptor] = Iterator.empty

      override def getNodePropertiesWithExistenceConstraint(labelName: String): Set[String] = Set.empty

      override def getRelationshipPropertiesWithExistenceConstraint(labelName: String): Set[String] = Set.empty

      override def getNodePropertiesWithTypeConstraint(labelName: String): Map[String, Seq[SchemaValueType]] = Map.empty

      override def getRelationshipPropertiesWithTypeConstraint(relTypeName: String): Map[String, Seq[SchemaValueType]] =
        Map.empty
    }
  }

  test("ProduceResult") {
    val plan = ProduceResult(Argument(), Seq.empty)

    val c = CardinalityCalculator.produceResultCardinality(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c should equal(defaultSourceCardinality)
  }

  test("AllNodesScan") {
    val plan = AllNodesScan(varFor("a"), Set.empty)
    val stats = new TestGraphStatistics {
      override def nodesAllCardinality(): Cardinality = Cardinality(321)
    }

    val c =
      CardinalityCalculator.allNodesScanCardinality(plan, defaultState, notImplementedPlanContext(stats), Map.empty)
    c should equal(stats.nodesAllCardinality())
  }

  test("AllNodesScan under Apply") {
    val plan = AllNodesScan(varFor("a"), Set.empty)
    val multiplier = Cardinality(10)
    val state = defaultState.pushLeafCardinalityMultiplier(multiplier)
    val stats = new TestGraphStatistics {
      override def nodesAllCardinality(): Cardinality = Cardinality(321)
    }

    val c = CardinalityCalculator.allNodesScanCardinality(plan, state, notImplementedPlanContext(stats), Map.empty)
    c should equal(stats.nodesAllCardinality() * multiplier)
  }

  test("NodeByLabelScan") {
    val plan = NodeByLabelScan(varFor("a"), LabelName("Label")(pos), Set.empty, IndexOrderNone)
    val labelCardinality = Cardinality(321)
    val labelIds = Map("Label" -> 1)
    val stats = new TestGraphStatistics {
      override def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality = {
        labelId.get.id should be(labelIds("Label"))
        labelCardinality
      }
    }

    val c =
      CardinalityCalculator.nodeByLabelScanCardinality(plan, defaultState, notImplementedPlanContext(stats), labelIds)
    c should equal(labelCardinality)
  }

  test("NodeByLabelScan under Apply") {
    val plan = NodeByLabelScan(varFor("a"), LabelName("Label")(pos), Set.empty, IndexOrderNone)
    val multiplier = Cardinality(10)
    val labelCardinality = Cardinality(321)
    val state = defaultState.pushLeafCardinalityMultiplier(multiplier)
    val labelIds = Map("Label" -> 1)
    val stats = new TestGraphStatistics {
      override def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality = {
        labelId.get.id should be(labelIds("Label"))
        labelCardinality
      }
    }

    val c = CardinalityCalculator.nodeByLabelScanCardinality(plan, state, notImplementedPlanContext(stats), labelIds)
    c should equal(labelCardinality * multiplier)
  }

  test("Argument") {
    val plan = Argument()
    val c = CardinalityCalculator.argumentCardinality(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c should equal(Cardinality.SINGLE)
  }

  test("Argument under Apply") {
    val plan = Argument()
    val multiplier = Cardinality(10)
    val state = defaultState.pushLeafCardinalityMultiplier(multiplier)

    val c = CardinalityCalculator.argumentCardinality(
      plan,
      state,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c should equal(multiplier)
  }

  test("NodeCountFromCountStore") {
    val plan = NodeCountFromCountStore(varFor(""), List.empty, Set.empty)
    val c = CardinalityCalculator.nodeCountFromCountStoreCardinality(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c should equal(Cardinality.SINGLE)
  }

  test("NodeCountFromCountStore under Apply") {
    val plan = NodeCountFromCountStore(varFor(""), List.empty, Set.empty)
    val multiplier = Cardinality(10)
    val state = defaultState.pushLeafCardinalityMultiplier(multiplier)

    val c = CardinalityCalculator.nodeCountFromCountStoreCardinality(
      plan,
      state,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c should equal(multiplier)
  }

  test("Expand (from)") {
    val rels = Seq("A", "B", "C")
    val relIds = rels.zipWithIndex.toMap

    val allNodesCount = 100
    val individualRelCount = 42

    val state = LogicalPlanGenerator.State(Map.empty, relIds).copy(
      arguments = Set(varFor("from")),
      cardinalities = new Cardinalities with Default[LogicalPlan, Cardinality] {
        override protected def defaultValue: Cardinality = defaultSourceCardinality
      }
    )

    val stats = new TestGraphStatistics {
      override def nodesAllCardinality(): Cardinality =
        Cardinality(allNodesCount)

      override def patternStepCardinality(
        fromLabel: Option[LabelId],
        relTypeId: Option[RelTypeId],
        toLabel: Option[LabelId]
      ): Cardinality = {
        fromLabel should be(empty)
        toLabel should be(empty)

        relTypeId match {
          case Some(_) => Cardinality(individualRelCount)
          case None    => Cardinality(individualRelCount * rels.size)
        }
      }
    }

    for (relNames <- Seq(Seq.empty, Seq("A"), Seq("A", "B")))
      withClue(s"Expand (from)-[$relNames]->(to)") {
        val relTypes = relNames.map(r => RelTypeName(r)(pos))

        val relCount = if (relTypes.isEmpty) rels.size else relTypes.size
        val avgRelsPerNode = (relCount * individualRelCount) / allNodesCount.toDouble

        val plan = Expand(Argument(), varFor("from"), SemanticDirection.OUTGOING, relTypes, varFor("to"), varFor("rel"))

        val expectedAmountApprox = avgRelsPerNode * defaultSourceCardinality.amount
        val Cardinality(actualAmount) =
          CardinalityCalculator.expandCardinality(plan, state, notImplementedPlanContext(stats), Map.empty)

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
      arguments = Set(varFor("from")),
      cardinalities = new Cardinalities with Default[LogicalPlan, Cardinality] {
        override protected def defaultValue: Cardinality = defaultSourceCardinality
      },
      labelInfo = Map(v"from" -> Set(LabelName("Label")(pos)))
    )

    val stats = new TestGraphStatistics {
      override def nodesAllCardinality(): Cardinality =
        Cardinality(allNodesCount)

      override def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality = {
        labelId.get.id should be(labelIds("Label"))
        Cardinality(labeledNodesCount)
      }

      override def patternStepCardinality(
        fromLabel: Option[LabelId],
        relTypeId: Option[RelTypeId],
        toLabel: Option[LabelId]
      ): Cardinality = {
        fromLabel.get.id should be(labelIds("Label"))
        toLabel should be(empty)

        relTypeId match {
          case Some(_) => Cardinality(individualRelCount)
          case None    => Cardinality(individualRelCount * rels.size)
        }
      }
    }

    for (relNames <- Seq(Seq.empty, Seq("A"), Seq("A", "B")))
      withClue(s"Expand (from:Label)-[$relNames]->(to)") {
        val relTypes = relNames.map(r => RelTypeName(r)(pos))

        val relCount = if (relTypes.isEmpty) rels.size else relTypes.size
        val avgRelsPerLabeledNode = (relCount * individualRelCount) / labeledNodesCount.toDouble

        val plan = Expand(Argument(), varFor("from"), SemanticDirection.OUTGOING, relTypes, varFor("to"), varFor("rel"))

        val expectedAmountApprox = avgRelsPerLabeledNode * defaultSourceCardinality.amount
        val Cardinality(actualAmount) =
          CardinalityCalculator.expandCardinality(plan, state, notImplementedPlanContext(stats), Map.empty)

        val marginOfError = expectedAmountApprox * 0.01
        actualAmount should equal(expectedAmountApprox +- marginOfError)
      }
  }

  test("DirectedRelationshipByIdSeek with no relationship ids") {
    val relIds = ManySeekableArgs(ListLiteral(Seq.empty)(pos))
    val plan = DirectedRelationshipByIdSeek(varFor("idName"), relIds, varFor("left"), varFor("right"), Set.empty)

    val c = CardinalityCalculator.directedRelationshipByIdSeek(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c should equal(Cardinality.EMPTY)
  }

  test("DirectedRelationshipByIdSeek with non empty list of relationship ids") {
    val relIdsSize = 100
    val relIds = (1 to relIdsSize).map(i => SignedDecimalIntegerLiteral(i.toString)(pos))
    val seekableArgs = ManySeekableArgs(ListLiteral(relIds)(pos))
    val plan = DirectedRelationshipByIdSeek(varFor("idName"), seekableArgs, varFor("left"), varFor("right"), Set.empty)

    val c = CardinalityCalculator.directedRelationshipByIdSeek(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c should equal(Cardinality(relIdsSize))
  }

  test("UndirectedRelationshipByIdSeek with no relationship ids") {
    val relIds = ManySeekableArgs(ListLiteral(Seq.empty)(pos))
    val plan = UndirectedRelationshipByIdSeek(varFor("idName"), relIds, varFor("left"), varFor("right"), Set.empty)

    val c = CardinalityCalculator.undirectedRelationshipByIdSeek(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c should equal(Cardinality.EMPTY)
  }

  test("UndirectedRelationshipByIdSeek with non empty list of relationship ids") {
    val relIdsSize = 100
    val relIds = (1 to relIdsSize).map(i => SignedDecimalIntegerLiteral(i.toString)(pos))
    val seekableArgs = ManySeekableArgs(ListLiteral(relIds)(pos))
    val plan =
      UndirectedRelationshipByIdSeek(varFor("idName"), seekableArgs, varFor("left"), varFor("right"), Set.empty)

    val c = CardinalityCalculator.undirectedRelationshipByIdSeek(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c should equal(Cardinality(relIdsSize * 2))
  }

  test("UndirectedRelationshipByIdSeek under apply") {
    val relIdsSize = 100
    val relIds = (1 to relIdsSize).map(i => SignedDecimalIntegerLiteral(i.toString)(pos))
    val seekableArgs = ManySeekableArgs(ListLiteral(relIds)(pos))
    val plan =
      UndirectedRelationshipByIdSeek(varFor("idName"), seekableArgs, varFor("left"), varFor("right"), Set.empty)
    val leafCardinalityMultiplier = Cardinality(5)

    val state = defaultState.pushLeafCardinalityMultiplier(leafCardinalityMultiplier)
    val c = CardinalityCalculator.undirectedRelationshipByIdSeek(
      plan,
      state,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c should equal(Cardinality(relIdsSize * 2) * leafCardinalityMultiplier)
  }

  test("Limit amount < node count") {
    val limitAmount = 100
    val plan = Limit(Argument(), SignedDecimalIntegerLiteral(limitAmount.toString)(pos))

    val c = CardinalityCalculator.limitCardinality(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c should equal(Cardinality(limitAmount))
  }

  test("Limit amount > node count") {
    val limitAmount = 1000
    val plan = Limit(Argument(), SignedDecimalIntegerLiteral(limitAmount.toString)(pos))

    val c = CardinalityCalculator.limitCardinality(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c should equal(defaultSourceCardinality)
  }

  test("Skip amount < node count") {
    val skipAmount = 100
    val plan = Skip(Argument(), SignedDecimalIntegerLiteral(skipAmount.toString)(pos))

    val c = CardinalityCalculator.skipCardinality(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c should equal(defaultSourceCardinality.map(_ - skipAmount))
  }

  test("Skip amount > node count") {
    val skipAmount = 1000
    val plan = Skip(Argument(), SignedDecimalIntegerLiteral(skipAmount.toString)(pos))

    val c = CardinalityCalculator.skipCardinality(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c should equal(Cardinality.EMPTY)
  }

  test("Projection") {
    val plan = Projection(Argument(), Map.empty)

    val c = CardinalityCalculator.projectionCardinality(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c should equal(defaultSourceCardinality)
  }

  test("Aggregation without grouping") {
    val plan = Aggregation(Argument(), Map.empty, Map.empty, None)

    val c = CardinalityCalculator.aggregationCardinality(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c should equal(Cardinality.SINGLE)
  }

  test("Aggregation with grouping") {
    val plan = Aggregation(Argument(), Map(varFor("x") -> CountStar()(pos)), Map.empty, None)

    val c = CardinalityCalculator.aggregationCardinality(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c should equal(Cardinality.sqrt(defaultSourceCardinality))
  }

  test("Apply") {
    val plan = Apply(Argument(), Argument())

    val c = CardinalityCalculator.applyCardinality(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c should equal(defaultSourceCardinality)
  }

  test("SemiApply lhs empty") {
    val plan = SemiApply(Argument(), Argument())
    defaultState.cardinalities.set(plan.lhs.get.id, Cardinality.EMPTY)

    val c = CardinalityCalculator.semiApplyCardinality(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c should equal(Cardinality.EMPTY)
  }

  test("SemiApply lhs non empty") {
    val plan = SemiApply(Argument(), Argument())

    val c = CardinalityCalculator.semiApplyCardinality(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c should equal(defaultSourceCardinality * PlannerDefaults.DEFAULT_PREDICATE_SELECTIVITY)
  }

  test("AntiSemiApply lhs empty") {
    val plan = AntiSemiApply(Argument(), Argument())
    defaultState.cardinalities.set(plan.lhs.get.id, Cardinality.EMPTY)

    val c = CardinalityCalculator.antiSemiApplyCardinality(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c should equal(Cardinality.EMPTY)
  }

  test("AntiSemiApply lhs non empty") {
    val plan = AntiSemiApply(Argument(), Argument())

    val c = CardinalityCalculator.antiSemiApplyCardinality(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c should equal(defaultSourceCardinality * PlannerDefaults.DEFAULT_PREDICATE_SELECTIVITY)
  }

  test("CartesianProduct") {
    val plan = CartesianProduct(Argument(), Argument())

    val c = CardinalityCalculator.cartesianProductCardinality(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c should equal(defaultSourceCardinality * defaultSourceCardinality)
  }

  test("Distinct non-empty source") {
    val plan = Distinct(Argument(), Map.empty)

    val c = CardinalityCalculator.distinctCardinality(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c should be < defaultSourceCardinality
  }

  test("Distinct empty source") {
    val plan = Distinct(Argument(), Map.empty)

    defaultState.cardinalities.set(plan.source.id, Cardinality.EMPTY)
    val c = CardinalityCalculator.distinctCardinality(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c shouldBe Cardinality.EMPTY
  }

  test("Optional non-empty source") {
    val plan = Optional(Argument(), Set.empty)

    val c = CardinalityCalculator.optionalCardinality(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c shouldBe defaultSourceCardinality
  }

  test("Optional empty source") {
    val plan = Optional(Argument(), Set.empty)

    defaultState.cardinalities.set(plan.source.id, Cardinality.EMPTY)
    val c = CardinalityCalculator.optionalCardinality(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c shouldBe Cardinality.SINGLE
  }

  test("Top amount < node count") {
    val topAmount = 100
    val plan = Top(Argument(), Seq.empty, SignedDecimalIntegerLiteral(topAmount.toString)(pos))
    val state = defaultState.copy(leafCardinalityMultipliersStack = List.empty)

    val c =
      CardinalityCalculator.topCardinality(plan, state, notImplementedPlanContext(new TestGraphStatistics), Map.empty)
    c should equal(Cardinality(topAmount))
  }

  test("Union") {
    val plan = Union(Argument(), Argument())

    val c = CardinalityCalculator.unionCardinality(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c should equal(defaultSourceCardinality + defaultSourceCardinality)
  }

  test("Top amount * cardinality from LHS of apply > node count") {
    val topAmount = 100
    val plan = Top(Argument(), Seq.empty, SignedDecimalIntegerLiteral(topAmount.toString)(pos))
    val state = defaultState.pushLeafCardinalityMultiplier(Cardinality(10))

    val c =
      CardinalityCalculator.topCardinality(plan, state, notImplementedPlanContext(new TestGraphStatistics), Map.empty)
    c should equal(defaultSourceCardinality)
  }

  test("Selection non-empty source") {
    val plan = Selection(Seq(Variable("x")(InputPosition.NONE)), Argument())

    val c = CardinalityCalculator.selectionCardinality(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c should be < defaultSourceCardinality
  }

  test("Selection empty source") {
    val plan = Selection(Seq(Variable("x")(InputPosition.NONE)), Argument())

    defaultState.cardinalities.set(plan.source.id, Cardinality.EMPTY)
    val c = CardinalityCalculator.selectionCardinality(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c shouldBe Cardinality.EMPTY
  }

  test("UnwindCollection non-empty source") {
    val plan = UnwindCollection(Argument(), varFor("n"), Variable("x")(InputPosition.NONE))

    val c = CardinalityCalculator.unwindCollectionCardinality(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c should be > defaultSourceCardinality
  }

  test("UnwindCollection empty source") {
    val plan = UnwindCollection(Argument(), varFor("n"), Variable("x")(InputPosition.NONE))

    defaultState.cardinalities.set(plan.source.id, Cardinality.EMPTY)
    val c = CardinalityCalculator.unwindCollectionCardinality(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c shouldBe Cardinality.EMPTY
  }

  test("ValueHashJoin") {
    val plan = ValueHashJoin(Argument(), Argument(), equals(trueLiteral, trueLiteral))

    val c = CardinalityCalculator.valueHashJoinCardinality(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c should be > defaultSourceCardinality
    c should be < defaultSourceCardinality * defaultSourceCardinality
  }

  test("ValueHashJoin empty LHS") {
    val plan = ValueHashJoin(Argument(), Argument(), equals(trueLiteral, trueLiteral))
    defaultState.cardinalities.set(plan.lhs.get.id, Cardinality.EMPTY)

    val c = CardinalityCalculator.valueHashJoinCardinality(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c shouldBe Cardinality.EMPTY
  }

  test("ValueHashJoin empty RHS") {
    val plan = ValueHashJoin(Argument(), Argument(), equals(trueLiteral, trueLiteral))
    defaultState.cardinalities.set(plan.rhs.get.id, Cardinality.EMPTY)

    val c = CardinalityCalculator.valueHashJoinCardinality(
      plan,
      defaultState,
      notImplementedPlanContext(new TestGraphStatistics),
      Map.empty
    )
    c shouldBe Cardinality.EMPTY
  }
}
