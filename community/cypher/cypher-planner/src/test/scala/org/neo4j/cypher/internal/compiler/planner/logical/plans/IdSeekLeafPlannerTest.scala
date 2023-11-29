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
package org.neo4j.cypher.internal.compiler.planner.logical.plans

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.ASTAnnotationMap
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.semantics.ExpressionTypeInfo
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper.PropertyAccess
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.CostModelMonitor
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CostModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.planner.logical.MetricsFactory
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.simpleExpressionEvaluator
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
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.Cost
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.helpers.NameDeduplicator.removeGeneratedNamesAndParamsOnTree
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class IdSeekLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private def newMockedMetrics(factory: MetricsFactory): Metrics =
    factory.newMetrics(planContext, evaluator, ExecutionModel.default)

  private val planContext = notImplementedPlanContext(hardcodedStatistics)
  // NOTE: rewriters make sure that all EQUALS will be rewritten to IN so here only the latter should be tested

  private val evaluator = simpleExpressionEvaluator

  test("simple node by id seek with a collection of node ids") {
    // given
    val expr = in(id(varFor("n")), listOfInt(42, 43, 43))
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set(v"n"), expr))),
      patternNodes = Set("n")
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(ExecutionModel.default)).thenReturn((
      (
        plan: LogicalPlan,
        _: QueryGraphSolverInput,
        _: SemanticTable,
        _: Cardinalities,
        _: ProvidedOrders,
        _: Set[PropertyAccess],
        _: GraphStatistics,
        _: CostModelMonitor
      ) =>
        plan match {
          case _: NodeByIdSeek => Cost(1)
          case _               => Cost(Double.MaxValue)
        }
    ): CostModel)
    val context =
      newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = newMockedMetrics(factory))
    when(context.semanticTable.typeFor(varFor("n"))).thenReturn(
      SemanticTable.TypeGetter(Some(CTNode.invariant))
    )

    // when
    val resultPlans = idSeekLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should equal(
      Set(NodeByIdSeek(varFor("n"), ManySeekableArgs(listOfInt(42, 43, 43)), Set.empty))
    )
  }

  test("simple node by id seek with a collection of node ids and skipped ids") {
    // given
    val expr = in(id(varFor("n")), listOfInt(42, 43, 43))
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set(v"n"), expr))),
      patternNodes = Set("n")
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(ExecutionModel.default)).thenReturn((
      (
        plan: LogicalPlan,
        _: QueryGraphSolverInput,
        _: SemanticTable,
        _: Cardinalities,
        _: ProvidedOrders,
        _: Set[PropertyAccess],
        _: GraphStatistics,
        _: CostModelMonitor
      ) =>
        plan match {
          case _: NodeByIdSeek => Cost(1)
          case _               => Cost(Double.MaxValue)
        }
    ): CostModel)
    val context =
      newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = newMockedMetrics(factory))
    when(context.semanticTable.typeFor(varFor("n"))).thenReturn(
      SemanticTable.TypeGetter(Some(CTNode.invariant))
    )

    // when
    val resultPlans = idSeekLeafPlanner(Set("n"))(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should be(empty)
  }

  test("node by id seek with a collection of node ids via previous variable") {
    // given
    val expr = in(id(varFor("n")), varFor("arr"))
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set(v"n"), expr))),
      patternNodes = Set("n"),
      argumentIds = Set("arr")
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(ExecutionModel.default)).thenReturn((
      (
        plan: LogicalPlan,
        _: QueryGraphSolverInput,
        _: SemanticTable,
        _: Cardinalities,
        _: ProvidedOrders,
        _: Set[PropertyAccess],
        _: GraphStatistics,
        _: CostModelMonitor
      ) =>
        plan match {
          case _: NodeByIdSeek => Cost(1)
          case _               => Cost(Double.MaxValue)
        }
    ): CostModel)
    val context =
      newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = newMockedMetrics(factory))
    when(context.semanticTable.typeFor(varFor("n"))).thenReturn(
      SemanticTable.TypeGetter(Some(CTNode.invariant))
    )

    // when
    val resultPlans = idSeekLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should equal(
      Set(NodeByIdSeek(varFor("n"), ManySeekableArgs(varFor("arr")), Set(varFor("arr"))))
    )
  }

  test("node by id seek should not be produced when the argument expression is an unbound variable") {
    // given match (n) where id(n) in arr
    val expr = in(id(varFor("n")), varFor("arr"))
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set(v"n"), expr))),
      patternNodes = Set("n"),
      argumentIds = Set()
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(ExecutionModel.default)).thenReturn((
      (
        plan: LogicalPlan,
        _: QueryGraphSolverInput,
        _: SemanticTable,
        _: Cardinalities,
        _: ProvidedOrders,
        _: Set[PropertyAccess],
        _: GraphStatistics,
        _: CostModelMonitor
      ) =>
        plan match {
          case _: NodeByIdSeek => Cost(1)
          case _               => Cost(Double.MaxValue)
        }
    ): CostModel)
    val context =
      newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = newMockedMetrics(factory))
    when(context.semanticTable.typeFor(varFor("n"))).thenReturn(
      SemanticTable.TypeGetter(Some(CTNode.invariant))
    )

    // when
    val resultPlans = idSeekLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans shouldBe empty
  }

  test("node by id seek should not be produced when the node variable is an argument") {
    // given match (n) where id(n) in arr
    val expr = in(id(varFor("n")), varFor("arr"))
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set(v"n"), expr))),
      patternNodes = Set("n"),
      argumentIds = Set("arr", "n")
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(ExecutionModel.default)).thenReturn((
      (
        plan: LogicalPlan,
        _: QueryGraphSolverInput,
        _: SemanticTable,
        _: Cardinalities,
        _: ProvidedOrders,
        _: Set[PropertyAccess],
        _: GraphStatistics,
        _: CostModelMonitor
      ) =>
        plan match {
          case _: NodeByIdSeek => Cost(1)
          case _               => Cost(Double.MaxValue)
        }
    ): CostModel)
    val context =
      newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = newMockedMetrics(factory))
    when(context.semanticTable.typeFor(varFor("n"))).thenReturn(
      SemanticTable.TypeGetter(Some(CTNode.invariant))
    )

    // when
    val resultPlans = idSeekLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans shouldBe empty
  }

  test("simple directed relationship by id seek with a collection of relationship ids") {
    // given
    val expr = in(id(varFor("r")), listOfInt(42, 43, 43))
    val from = v"from"
    val end = v"to"
    val patternRel = PatternRelationship(v"r", (from, end), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set(v"r"), expr))),
      patternNodes = Set(from, end).map(_.name),
      patternRelationships = Set(patternRel)
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(ExecutionModel.default)).thenReturn((
      (
        plan: LogicalPlan,
        _: QueryGraphSolverInput,
        _: SemanticTable,
        _: Cardinalities,
        _: ProvidedOrders,
        _: Set[PropertyAccess],
        _: GraphStatistics,
        _: CostModelMonitor
      ) =>
        plan match {
          case _: DirectedRelationshipByIdSeek => Cost(1)
          case _                               => Cost(Double.MaxValue)
        }
    ): CostModel)
    val context =
      newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = newMockedMetrics(factory))
    when(context.semanticTable.typeFor(varFor("r"))).thenReturn(
      SemanticTable.TypeGetter(Some(CTRelationship.invariant))
    )

    // when
    val resultPlans = idSeekLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should equal(
      Set(DirectedRelationshipByIdSeek(
        varFor("r"),
        ManySeekableArgs(listOfInt(42, 43, 43)),
        from,
        end,
        Set.empty
      ))
    )
  }

  test("simple undirected relationship by id seek with a collection of relationship ids") {
    // given
    val expr = in(id(varFor("r")), listOfInt(42, 43, 43))
    val from = v"from"
    val end = v"to"
    val patternRel = PatternRelationship(v"r", (from, end), SemanticDirection.BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set(v"r"), expr))),
      patternNodes = Set(from, end).map(_.name),
      patternRelationships = Set(patternRel)
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(ExecutionModel.default)).thenReturn((
      (
        plan: LogicalPlan,
        _: QueryGraphSolverInput,
        _: SemanticTable,
        _: Cardinalities,
        _: ProvidedOrders,
        _: Set[PropertyAccess],
        _: GraphStatistics,
        _: CostModelMonitor
      ) =>
        plan match {
          case _: UndirectedRelationshipByIdSeek => Cost(2)
          case _                                 => Cost(Double.MaxValue)
        }
    ): CostModel)
    val context =
      newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = newMockedMetrics(factory))
    when(context.semanticTable.typeFor(varFor("r"))).thenReturn(
      SemanticTable.TypeGetter(Some(CTRelationship.invariant))
    )

    // when
    val resultPlans = idSeekLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should equal(
      Set(UndirectedRelationshipByIdSeek(
        varFor("r"),
        ManySeekableArgs(listOfInt(42, 43, 43)),
        from,
        end,
        Set.empty
      ))
    )
  }

  test("simple undirected typed relationship by id seek with a collection of relationship ids") {
    // given
    val expr = in(id(varFor("r")), listOfInt(42))
    val from = v"from"
    val end = v"to"
    val relTypeX = RelTypeName("X")(pos)

    val semanticTable =
      new SemanticTable(ASTAnnotationMap(
        varFor("r") -> ExpressionTypeInfo(symbols.CTRelationship)
      ))
    semanticTable.resolvedRelTypeNames += "X" -> RelTypeId(1)

    val patternRel = PatternRelationship(
      v"r",
      (from, end),
      SemanticDirection.BOTH,
      Seq(relTypeX),
      SimplePatternLength
    )
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set(v"r"), expr))),
      patternNodes = Set(from, end).map(_.name),
      patternRelationships = Set(patternRel)
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(ExecutionModel.default)).thenReturn((
      (
        plan: LogicalPlan,
        _: QueryGraphSolverInput,
        _: SemanticTable,
        _: Cardinalities,
        _: ProvidedOrders,
        _: Set[PropertyAccess],
        _: GraphStatistics,
        _: CostModelMonitor
      ) =>
        plan match {
          case _: UndirectedRelationshipByIdSeek => Cost(2)
          case _                                 => Cost(Double.MaxValue)
        }
    ): CostModel)
    val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext(),
      metrics = newMockedMetrics(factory),
      semanticTable = semanticTable
    )

    // when
    val resultPlans = idSeekLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should equal(
      Set(Selection(
        ands(in(function("type", varFor("r")), literalString("X"))),
        UndirectedRelationshipByIdSeek(
          varFor("r"),
          ManySeekableArgs(listOfInt(42)),
          from,
          end,
          Set.empty
        )
      ))
    )
  }

  test("simple undirected multi-typed relationship by id seek with a collection of relationship ids") {
    // given
    val expr = in(id(varFor("r")), listOfInt(42))
    val from = v"from"
    val end = v"to"
    val relTypeX = RelTypeName("X") _
    val relTypeY = RelTypeName("Y") _

    val semanticTable =
      new SemanticTable(ASTAnnotationMap(
        varFor("r") -> ExpressionTypeInfo(symbols.CTRelationship)
      ))
    semanticTable.resolvedRelTypeNames += "X" -> RelTypeId(1)
    semanticTable.resolvedRelTypeNames += "Y" -> RelTypeId(2)

    val patternRel = PatternRelationship(
      v"r",
      (from, end),
      SemanticDirection.BOTH,
      Seq[RelTypeName](relTypeX, relTypeY),
      SimplePatternLength
    )
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set(v"r"), expr))),
      patternNodes = Set(from, end).map(_.name),
      patternRelationships = Set(patternRel)
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(ExecutionModel.default)).thenReturn((
      (
        plan: LogicalPlan,
        _: QueryGraphSolverInput,
        _: SemanticTable,
        _: Cardinalities,
        _: ProvidedOrders,
        _: Set[PropertyAccess],
        _: GraphStatistics,
        _: CostModelMonitor
      ) =>
        plan match {
          case _: UndirectedRelationshipByIdSeek => Cost(2)
          case _                                 => Cost(Double.MaxValue)
        }
    ): CostModel)
    val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext(),
      metrics = newMockedMetrics(factory),
      semanticTable = semanticTable
    )

    // when
    val resultPlans = idSeekLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should equal(
      Set(Selection(
        ands(
          in(function("type", varFor("r")), listOf(literalString("X"), literalString("Y")))
        ),
        UndirectedRelationshipByIdSeek(
          varFor("r"),
          ManySeekableArgs(listOfInt(42)),
          from,
          end,
          Set.empty
        )
      ))
    )
  }

  test("simple directed relationship by id seek with a collection of relationship ids, start node already bound") {
    // given
    val rel = varFor("r")
    val expr = in(id(rel), listOfInt(42, 43, 43))
    val from = v"from"
    val end = v"to"
    val patternRel = PatternRelationship(v"r", (from, end), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set(v"r"), expr))),
      patternNodes = Set(from, end).map(_.name),
      patternRelationships = Set(patternRel),
      argumentIds = Set(from.name)
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(ExecutionModel.default)).thenReturn((
      (
        plan: LogicalPlan,
        _: QueryGraphSolverInput,
        _: SemanticTable,
        _: Cardinalities,
        _: ProvidedOrders,
        _: Set[PropertyAccess],
        _: GraphStatistics,
        _: CostModelMonitor
      ) =>
        plan match {
          case _: DirectedRelationshipByIdSeek => Cost(1)
          case _                               => Cost(Double.MaxValue)
        }
    ): CostModel)
    val context =
      newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = newMockedMetrics(factory))
    when(context.semanticTable.typeFor(rel)).thenReturn(
      SemanticTable.TypeGetter(Some(CTRelationship.invariant))
    )

    // when
    val resultPlans =
      idSeekLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context).map(removeGeneratedNamesAndParamsOnTree)

    // then
    val newFrom = "anon_0"
    val expectedPlan = new LogicalPlanBuilder(wholePlan = false)
      .filter(s"from = `$newFrom`")
      .directedRelationshipByIdSeek("r", newFrom, end.name, Set(from.name), 42, 43, 43)
      .build()

    resultPlans shouldEqual Set(expectedPlan)
  }

  test(
    "simple directed relationship by id seek with a collection of relationship ids, start and end nodes already bound"
  ) {
    // given
    val rel = varFor("r")
    val expr = in(id(rel), listOfInt(42, 43, 43))
    val from = v"from"
    val end = v"to"
    val patternRel = PatternRelationship(v"r", (from, end), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set(v"r"), expr))),
      patternNodes = Set(from, end).map(_.name),
      patternRelationships = Set(patternRel),
      argumentIds = Set(from, end).map(_.name)
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(ExecutionModel.default)).thenReturn((
      (
        plan: LogicalPlan,
        _: QueryGraphSolverInput,
        _: SemanticTable,
        _: Cardinalities,
        _: ProvidedOrders,
        _: Set[PropertyAccess],
        _: GraphStatistics,
        _: CostModelMonitor
      ) =>
        plan match {
          case _: DirectedRelationshipByIdSeek => Cost(1)
          case _                               => Cost(Double.MaxValue)
        }
    ): CostModel)
    val context =
      newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = newMockedMetrics(factory))
    when(context.semanticTable.typeFor(rel)).thenReturn(
      SemanticTable.TypeGetter(Some(CTRelationship.invariant))
    )

    // when
    val resultPlans =
      idSeekLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context).map(removeGeneratedNamesAndParamsOnTree)

    // then
    val newFrom = "anon_0"
    val newTo = "anon_1"
    val expectedPlan = new LogicalPlanBuilder(wholePlan = false)
      .filter(s"${from.name} = `$newFrom`", s"${end.name} = `$newTo`")
      .directedRelationshipByIdSeek("r", newFrom, newTo, Set(from, end).map(_.name), 42, 43, 43)
      .build()

    resultPlans shouldEqual Set(expectedPlan)
  }

  test("self-loop directed relationship by id seek single relationship id, start and end node already bound") {
    // given
    val expr = in(id(varFor("r")), listOfInt(42))
    val from = v"n"
    val to = from
    val patternRel = PatternRelationship(v"r", (from, to), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set(v"r"), expr))),
      patternNodes = Set(from, to).map(_.name),
      patternRelationships = Set(patternRel)
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(ExecutionModel.default)).thenReturn((
      (
        plan: LogicalPlan,
        _: QueryGraphSolverInput,
        _: SemanticTable,
        _: Cardinalities,
        _: ProvidedOrders,
        _: Set[PropertyAccess],
        _: GraphStatistics,
        _: CostModelMonitor
      ) =>
        plan match {
          case _: DirectedRelationshipByIdSeek => Cost(1)
          case _                               => Cost(Double.MaxValue)
        }
    ): CostModel)
    val context =
      newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = newMockedMetrics(factory))
    when(context.semanticTable.typeFor(varFor("r"))).thenReturn(
      SemanticTable.TypeGetter(Some(CTRelationship.invariant))
    )

    // when
    val resultPlans = idSeekLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    val newTo = "  UNNAMED0"
    // then
    val expectedPlan = new LogicalPlanBuilder(wholePlan = false)
      .filter(s"${to.name} = `$newTo`")
      .directedRelationshipByIdSeek("r", from.name, newTo, Set.empty, 42)
      .build()

    resultPlans shouldEqual Set(expectedPlan)
  }

  test(
    "self-loop directed relationship by id seek with a collections of relationship ids, start and end node already bound"
  ) {
    // given
    val rel = varFor("r")
    val expr = in(id(rel), listOfInt(42, 43, 43))
    val from = v"n"
    val to = from
    val patternRel = PatternRelationship(v"r", (from, to), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set(v"r"), expr))),
      patternNodes = Set(from, to).map(_.name),
      patternRelationships = Set(patternRel)
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(ExecutionModel.default)).thenReturn((
      (
        plan: LogicalPlan,
        _: QueryGraphSolverInput,
        _: SemanticTable,
        _: Cardinalities,
        _: ProvidedOrders,
        _: Set[PropertyAccess],
        _: GraphStatistics,
        _: CostModelMonitor
      ) =>
        plan match {
          case _: DirectedRelationshipByIdSeek => Cost(1)
          case _                               => Cost(Double.MaxValue)
        }
    ): CostModel)
    val context =
      newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = newMockedMetrics(factory))
    when(context.semanticTable.typeFor(varFor("r"))).thenReturn(
      SemanticTable.TypeGetter(Some(CTRelationship.invariant))
    )

    // when
    val resultPlans = idSeekLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    val newTo = "  UNNAMED0"
    // then
    val expectedPlan = new LogicalPlanBuilder(wholePlan = false)
      .filter(s"${to.name} = `$newTo`")
      .directedRelationshipByIdSeek("r", from.name, newTo, Set.empty, 42, 43, 43)
      .build()

    resultPlans shouldEqual Set(expectedPlan)
  }
}
