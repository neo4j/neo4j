/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner.logical.plans

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.ASTAnnotationMap
import org.neo4j.cypher.internal.ast.semantics.ExpressionTypeInfo
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.CostModelMonitor
import org.neo4j.cypher.internal.compiler.planner.logical.ExpressionEvaluator
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.idSeekLeafPlanner
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ManySeekableArgs
import org.neo4j.cypher.internal.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.Cost
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.helpers.NameDeduplicator.removeGeneratedNamesAndParamsOnTree
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class IdSeekLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private val planContext = notImplementedPlanContext(hardcodedStatistics)
  // NOTE: rewriters make sure that all EQUALS will be rewritten to IN so here only the latter should be tested

  private val evaluator = mock[ExpressionEvaluator]
  test("simple node by id seek with a collection of node ids") {
    // given
    val expr = in(id(varFor("n")), listOfInt(42, 43, 43))
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set("n"), expr))),
      patternNodes = Set("n")
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(ExecutionModel.default)).thenReturn((plan: LogicalPlan, _: QueryGraphSolverInput, _: SemanticTable, _: Cardinalities, _: ProvidedOrders, _: CostModelMonitor) => plan match {
      case _: NodeByIdSeek => Cost(1)
      case _               => Cost(Double.MaxValue)
    })
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = factory.newMetrics(planContext, evaluator, ExecutionModel.default, planningTextIndexesEnabled = false))
    when(context.semanticTable.isNode(varFor("n"))).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should equal(
      Set(NodeByIdSeek("n", ManySeekableArgs(listOfInt(42, 43, 43)), Set.empty))
    )
  }

  test("simple node by id seek with a collection of node ids and skipped ids") {
    // given
    val expr = in(id(varFor("n")), listOfInt(42, 43, 43))
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set("n"), expr))),
      patternNodes = Set("n")
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(ExecutionModel.default)).thenReturn((plan: LogicalPlan, _: QueryGraphSolverInput, _: SemanticTable, _: Cardinalities, _: ProvidedOrders, _: CostModelMonitor) => plan match {
      case _: NodeByIdSeek => Cost(1)
      case _               => Cost(Double.MaxValue)
    })
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = factory.newMetrics(planContext, evaluator, ExecutionModel.default, planningTextIndexesEnabled = false))
    when(context.semanticTable.isNode(varFor("n"))).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(Set("n"))(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should be(empty)
  }


  test("node by id seek with a collection of node ids via previous variable") {
    // given
    val expr = in(id(varFor("n")), varFor("arr"))
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set("n"), expr))),
      patternNodes = Set("n"),
      argumentIds = Set("arr")
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(ExecutionModel.default)).thenReturn((plan: LogicalPlan, _: QueryGraphSolverInput, _: SemanticTable, _: Cardinalities, _: ProvidedOrders, _: CostModelMonitor) => plan match {
      case _: NodeByIdSeek => Cost(1)
      case _               => Cost(Double.MaxValue)
    })
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = factory.newMetrics(planContext, evaluator, ExecutionModel.default, planningTextIndexesEnabled = false))
    when(context.semanticTable.isNode(varFor("n"))).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should equal(
      Set(NodeByIdSeek("n", ManySeekableArgs(varFor("arr")), Set("arr")))
    )
  }

  test("node by id seek should not be produced when the argument expression is an unbound variable") {
    // given match (n) where id(n) in arr
    val expr = in(id(varFor("n")), varFor("arr"))
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set("n"), expr))),
      patternNodes = Set("n"),
      argumentIds = Set()
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(ExecutionModel.default)).thenReturn((plan: LogicalPlan, _: QueryGraphSolverInput, _: SemanticTable, _: Cardinalities, _: ProvidedOrders, _: CostModelMonitor) => plan match {
      case _: NodeByIdSeek => Cost(1)
      case _               => Cost(Double.MaxValue)
    })
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = factory.newMetrics(planContext, evaluator, ExecutionModel.default, planningTextIndexesEnabled = false))
    when(context.semanticTable.isNode(varFor("n"))).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans shouldBe empty
  }

  test("node by id seek should not be produced when the node variable is an argument") {
    // given match (n) where id(n) in arr
    val expr = in(id(varFor("n")), varFor("arr"))
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set("n"), expr))),
      patternNodes = Set("n"),
      argumentIds = Set("arr", "n")
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(ExecutionModel.default)).thenReturn((plan: LogicalPlan, _: QueryGraphSolverInput, _: SemanticTable, _: Cardinalities, _: ProvidedOrders, _: CostModelMonitor) => plan match {
      case _: NodeByIdSeek => Cost(1)
      case _               => Cost(Double.MaxValue)
    })
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = factory.newMetrics(planContext, evaluator, ExecutionModel.default, planningTextIndexesEnabled = false))
    when(context.semanticTable.isNode(varFor("n"))).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans shouldBe empty
  }

  test("simple directed relationship by id seek with a collection of relationship ids") {
    // given
    val expr = in(id(varFor("r")), listOfInt(42, 43, 43))
    val from = "from"
    val end = "to"
    val patternRel = PatternRelationship("r", (from, end), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set("r"), expr))),
      patternNodes = Set(from, end),
      patternRelationships = Set(patternRel)
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(ExecutionModel.default)).thenReturn((plan: LogicalPlan, _: QueryGraphSolverInput, _: SemanticTable, _: Cardinalities, _: ProvidedOrders, _: CostModelMonitor) => plan match {
      case _: DirectedRelationshipByIdSeek => Cost(1)
      case _                               => Cost(Double.MaxValue)
    })
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = factory.newMetrics(planContext, evaluator, ExecutionModel.default, planningTextIndexesEnabled = false))
    when(context.semanticTable.isRelationship(varFor("r"))).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should equal(
      Set(DirectedRelationshipByIdSeek("r", ManySeekableArgs(listOfInt(42, 43, 43)), from, end, Set.empty))
    )
  }

  test("simple undirected relationship by id seek with a collection of relationship ids") {
    // given
    val expr = in(id(varFor("r")), listOfInt(42, 43, 43))
    val from = "from"
    val end = "to"
    val patternRel = PatternRelationship("r", (from, end), SemanticDirection.BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set("r"), expr))),
      patternNodes = Set(from, end),
      patternRelationships = Set(patternRel))

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(ExecutionModel.default)).thenReturn((plan: LogicalPlan, _: QueryGraphSolverInput, _: SemanticTable, _: Cardinalities, _: ProvidedOrders, _: CostModelMonitor) => plan match {
      case _: UndirectedRelationshipByIdSeek => Cost(2)
      case _                                 => Cost(Double.MaxValue)
    })
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = factory.newMetrics(planContext, evaluator, ExecutionModel.default, planningTextIndexesEnabled = false))
    when(context.semanticTable.isRelationship(varFor("r"))).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should equal(
      Set(UndirectedRelationshipByIdSeek("r", ManySeekableArgs(listOfInt(42, 43, 43)), from, end, Set.empty))
    )
  }

  test("simple undirected typed relationship by id seek with a collection of relationship ids") {
    // given
    val expr = in(id(varFor("r")), listOfInt(42))
    val from = "from"
    val end = "to"
    val relTypeX = RelTypeName("X")(pos)

    val semanticTable =
      new SemanticTable(ASTAnnotationMap(
        varFor("r") -> ExpressionTypeInfo(symbols.CTRelationship)
      ))
    semanticTable.resolvedRelTypeNames += "X" -> RelTypeId(1)

    val patternRel = PatternRelationship(
      "r", (from, end), SemanticDirection.BOTH,
      Seq(relTypeX),
      SimplePatternLength
    )
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set("r"), expr))),
      patternNodes = Set(from, end),
      patternRelationships = Set(patternRel))

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(ExecutionModel.default)).thenReturn((plan: LogicalPlan, _: QueryGraphSolverInput, _: SemanticTable, _: Cardinalities, _: ProvidedOrders, _: CostModelMonitor) => plan match {
      case _: UndirectedRelationshipByIdSeek => Cost(2)
      case _                                 => Cost(Double.MaxValue)
    })
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = factory.newMetrics(planContext, evaluator, ExecutionModel.default, planningTextIndexesEnabled = false), semanticTable = semanticTable)

    // when
    val resultPlans = idSeekLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should equal(
      Set(Selection(
        ands(in(function("type", varFor("r")), literalString("X"))),
        UndirectedRelationshipByIdSeek("r", ManySeekableArgs(listOfInt(42)), from, end, Set.empty)
      ))
    )
  }

  test("simple undirected multi-typed relationship by id seek with a collection of relationship ids") {
    // given
    val expr = in(id(varFor("r")), listOfInt(42))
    val from = "from"
    val end = "to"
    val relTypeX = RelTypeName("X") _
    val relTypeY = RelTypeName("Y") _

    val semanticTable =
      new SemanticTable(ASTAnnotationMap(
        varFor("r") -> ExpressionTypeInfo(symbols.CTRelationship)
      ))
    semanticTable.resolvedRelTypeNames += "X" -> RelTypeId(1)
    semanticTable.resolvedRelTypeNames += "Y" -> RelTypeId(2)

    val patternRel = PatternRelationship(
      "r", (from, end), SemanticDirection.BOTH,
      Seq[RelTypeName](relTypeX, relTypeY),
      SimplePatternLength
    )
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set("r"), expr))),
      patternNodes = Set(from, end),
      patternRelationships = Set(patternRel))

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(ExecutionModel.default)).thenReturn((plan: LogicalPlan, _: QueryGraphSolverInput, _: SemanticTable, _: Cardinalities, _: ProvidedOrders, _: CostModelMonitor) => plan match {
      case _: UndirectedRelationshipByIdSeek => Cost(2)
      case _                                 => Cost(Double.MaxValue)
    })
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = factory.newMetrics(planContext, evaluator, ExecutionModel.default, planningTextIndexesEnabled = false), semanticTable = semanticTable)

    // when
    val resultPlans = idSeekLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should equal(
      Set(Selection(
        ands(
          in(function("type", varFor("r")), listOf(literalString("X"), literalString("Y")))
        ),
        UndirectedRelationshipByIdSeek("r", ManySeekableArgs(listOfInt(42)), from, end, Set.empty)
      )))
  }

  test("simple directed relationship by id seek with a collection of relationship ids, start node already bound") {
    // given
    val rel = varFor("r")
    val expr = in(id(rel), listOfInt(42, 43, 43))
    val from = "from"
    val end = "to"
    val patternRel = PatternRelationship("r", (from, end), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set("r"), expr))),
      patternNodes = Set(from, end),
      patternRelationships = Set(patternRel),
      argumentIds = Set(from)
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(ExecutionModel.default)).thenReturn((plan: LogicalPlan, _: QueryGraphSolverInput, _: SemanticTable, _: Cardinalities, _: ProvidedOrders, _: CostModelMonitor) => plan match {
      case _: DirectedRelationshipByIdSeek => Cost(1)
      case _                               => Cost(Double.MaxValue)
    })
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = factory.newMetrics(planContext, evaluator, ExecutionModel.default, planningTextIndexesEnabled = false))
    when(context.semanticTable.isRelationship(rel)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context).map(removeGeneratedNamesAndParamsOnTree)

    // then
    val newFrom = "anon_0"
    val expectedPlan = new LogicalPlanBuilder(wholePlan = false)
      .filter(s"from = `$newFrom`")
      .directedRelationshipByIdSeek("r", newFrom, end, Set(from), 42, 43, 43)
      .build()

    resultPlans shouldEqual Set(expectedPlan)
  }

  test("simple directed relationship by id seek with a collection of relationship ids, start and end nodes already bound") {
    // given
    val rel = varFor("r")
    val expr = in(id(rel), listOfInt(42, 43, 43))
    val from = "from"
    val end = "to"
    val patternRel = PatternRelationship("r", (from, end), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set("r"), expr))),
      patternNodes = Set(from, end),
      patternRelationships = Set(patternRel),
      argumentIds = Set(from, end)
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(ExecutionModel.default)).thenReturn((plan: LogicalPlan, _: QueryGraphSolverInput, _: SemanticTable, _: Cardinalities, _: ProvidedOrders, _: CostModelMonitor) => plan match {
      case _: DirectedRelationshipByIdSeek => Cost(1)
      case _                               => Cost(Double.MaxValue)
    })
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = factory.newMetrics(planContext, evaluator, ExecutionModel.default, planningTextIndexesEnabled = false))
    when(context.semanticTable.isRelationship(rel)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context).map(removeGeneratedNamesAndParamsOnTree)

    // then
    val newFrom = "anon_0"
    val newTo = "anon_1"
    val expectedPlan = new LogicalPlanBuilder(wholePlan = false)
      .filter(s"$from = `$newFrom`", s"$end = `$newTo``")
      .directedRelationshipByIdSeek("r", newFrom, newTo, Set(from, end), 42, 43, 43)
      .build()

    resultPlans shouldEqual Set(expectedPlan)
  }

  test("self-loop directed relationship by id seek single relationship id, start and end node already bound") {
    // given
    val expr = in(id(varFor("r")), listOfInt(42))
    val from = "n"
    val to = from
    val patternRel = PatternRelationship("r", (from, to), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set("r"), expr))),
      patternNodes = Set(from, to),
      patternRelationships = Set(patternRel)
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(ExecutionModel.default)).thenReturn((plan: LogicalPlan, _: QueryGraphSolverInput, _: SemanticTable, _: Cardinalities, _: ProvidedOrders, _: CostModelMonitor) => plan match {
      case _: DirectedRelationshipByIdSeek => Cost(1)
      case _                               => Cost(Double.MaxValue)
    })
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = factory.newMetrics(planContext, evaluator, ExecutionModel.default, planningTextIndexesEnabled = false))
    when(context.semanticTable.isRelationship(varFor("r"))).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    val newTo = "  UNNAMED0"
    // then
    val expectedPlan = new LogicalPlanBuilder(wholePlan = false)
      .filter(s"$to = `$newTo``")
      .directedRelationshipByIdSeek("r", from, newTo, Set.empty, 42)
      .build()

    resultPlans shouldEqual Set(expectedPlan)
  }

  test("self-loop directed relationship by id seek with a collections of relationship ids, start and end node already bound") {
    // given
    val rel = varFor("r")
    val expr = in(id(rel), listOfInt(42, 43, 43))
    val from = "n"
    val to = from
    val patternRel = PatternRelationship("r", (from, to), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set("r"), expr))),
      patternNodes = Set(from, to),
      patternRelationships = Set(patternRel)
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(ExecutionModel.default)).thenReturn((plan: LogicalPlan, _: QueryGraphSolverInput, _: SemanticTable, _: Cardinalities, _: ProvidedOrders, _: CostModelMonitor) => plan match {
      case _: DirectedRelationshipByIdSeek => Cost(1)
      case _                               => Cost(Double.MaxValue)
    })
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = factory.newMetrics(planContext, evaluator, ExecutionModel.default, planningTextIndexesEnabled = false))
    when(context.semanticTable.isRelationship(varFor("r"))).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    val newTo = "  UNNAMED0"
    // then
    val expectedPlan = new LogicalPlanBuilder(wholePlan = false)
      .filter(s"$to = `$newTo``")
      .directedRelationshipByIdSeek("r", from, newTo, Set.empty, 42, 43, 43)
      .build()

    resultPlans shouldEqual Set(expectedPlan)
  }
}
