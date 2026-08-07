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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.ExecutionModel.Volcano
import org.neo4j.cypher.internal.compiler.helpers.FakeLeafPlan
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.logical.plans.CachedProperties
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrder
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.invariantTypeSpec

class ShardPredicatePushdownPartitionTest extends CypherPlannerTestSuite with LogicalPlanningTestSupport
    with LogicalPlanningTestSupport2 with AstConstructionTestSupport {

  private def mockedSemanticTable: SemanticTable = {
    val m = mock[SemanticTable]
    when(m.typeFor(any[Expression])).thenReturn(SemanticTable.TypeGetter(Some(CTNode)))
    m
  }

  private val context: LogicalPlanningContext =
    newMockedLogicalPlanningContext(newMockedPlanContext(), semanticTable = mockedSemanticTable)

  private def fakePlanWithAvailableSymbolsForPredicates(exprs: Set[Expression]): FakeLeafPlan =
    fakeLogicalPlanFor(context.staticComponents.planningAttributes, exprs.flatMap(_.dependencies).map(_.name).toSeq: _*)

  test("should not allow expressions with multiple dependencies") {
    val predicates: Set[Expression] = Set(greaterThan(prop("var1", "prop1"), prop("var2", "prop2")))

    ShardPredicatePushdownPartition(
      fakePlanWithAvailableSymbolsForPredicates(predicates),
      context,
      predicates
    ) shouldEqual ShardPredicatePushdownPartition.withFilterOnMainWithRemoteProperties(predicates)
  }

  test("should pre-filter expressions with no property access") {
    val predicates: Set[Expression] = Set(
      equals(varFor("a"), literal(5)),
      hasLabels("a", "A"),
      hasTypes("r", "R"),
      listComprehension(
        v"n",
        nodes(varLengthPathExpression(v"a", v"anon_3", v"b")),
        None,
        None
      )
    )
    ShardPredicatePushdownPartition(
      fakePlanWithAvailableSymbolsForPredicates(predicates),
      context,
      predicates
    ) shouldEqual ShardPredicatePushdownPartition.withPreFilterBeforePushdown(predicates)
  }

  test("should allow contains with only parameters, properties and literals") {
    val supportedForPushdown: Set[Expression] = Set(
      contains(prop("a", "subStr"), prop("a", "str")),
      contains(prop("a", "str"), literalString("42")),
      contains(prop("a", "str"), parameter("str", CTString))
    )
    val unsupportedForPushdown: Set[Expression] = Set(contains(prop("a", "str"), collect(prop("a", "str2"))))
    ShardPredicatePushdownPartition(
      fakePlanWithAvailableSymbolsForPredicates(supportedForPushdown ++ unsupportedForPushdown),
      context,
      supportedForPushdown ++ unsupportedForPushdown
    ) shouldEqual new ShardPredicatePushdownPartition(
      preFilterBeforePushdown = Set.empty,
      filterOnShards = Option(PushedPredicatesDetails(
        varFor("a"),
        supportedForPushdown,
        Set.empty,
        Set.empty
      )),
      filterOnMainWithRemoteProperties = unsupportedForPushdown
    )
  }

  test("should allow endsWith with only parameters, properties and literals") {
    val supportedForPushdown: Set[Expression] = Set(
      endsWith(prop("a", "subStr"), prop("a", "str")),
      endsWith(prop("a", "str"), literalString("42")),
      endsWith(prop("a", "str"), parameter("str", CTString))
    )
    val unsupportedForPushdown: Set[Expression] = Set(endsWith(prop("a", "str"), collect(prop("a", "str2"))))
    ShardPredicatePushdownPartition(
      fakePlanWithAvailableSymbolsForPredicates(supportedForPushdown ++ unsupportedForPushdown),
      context,
      supportedForPushdown ++ unsupportedForPushdown
    ) shouldEqual new ShardPredicatePushdownPartition(
      preFilterBeforePushdown = Set.empty,
      filterOnShards = Option(PushedPredicatesDetails(
        varFor("a"),
        supportedForPushdown,
        Set.empty,
        Set.empty
      )),
      filterOnMainWithRemoteProperties = unsupportedForPushdown
    )
  }

  test("should allow startsWith with") {
    val supportedForPushdown: Set[Expression] = Set(
      startsWith(prop("a", "subStr"), prop("a", "str")),
      startsWith(prop("a", "str"), literalString("42")),
      startsWith(prop("a", "str"), parameter("str", CTString))
    )
    val unsupportedForPushdown: Set[Expression] = Set(startsWith(prop("a", "str"), collect(prop("a", "str2"))))
    ShardPredicatePushdownPartition(
      fakePlanWithAvailableSymbolsForPredicates(supportedForPushdown ++ unsupportedForPushdown),
      context,
      supportedForPushdown ++ unsupportedForPushdown
    ) shouldEqual new ShardPredicatePushdownPartition(
      preFilterBeforePushdown = Set.empty,
      filterOnShards = Option(PushedPredicatesDetails(
        varFor("a"),
        supportedForPushdown,
        Set.empty,
        Set.empty
      )),
      filterOnMainWithRemoteProperties = unsupportedForPushdown
    )
  }

  test("should allow less than") {
    val supportedForPushdown: Set[Expression] = Set(
      lessThan(prop("a", "num"), prop("a", "num")),
      lessThan(prop("a", "num"), literalInt(42)),
      lessThan(prop("a", "num"), parameter("num", CTInteger))
    )
    val unsupportedForPushdown: Set[Expression] = Set(lessThan(prop("a", "num"), countStar()))
    ShardPredicatePushdownPartition(
      fakePlanWithAvailableSymbolsForPredicates(supportedForPushdown ++ unsupportedForPushdown),
      context,
      supportedForPushdown ++ unsupportedForPushdown
    ) shouldEqual new ShardPredicatePushdownPartition(
      preFilterBeforePushdown = Set.empty,
      filterOnShards = Option(PushedPredicatesDetails(
        varFor("a"),
        supportedForPushdown,
        Set.empty,
        Set.empty
      )),
      filterOnMainWithRemoteProperties = unsupportedForPushdown
    )
  }

  test("should allow lessThanOrEqual") {
    val supportedForPushdown: Set[Expression] = Set(
      lessThanOrEqual(prop("a", "num"), prop("a", "num")),
      lessThanOrEqual(prop("a", "str"), literalInt(42)),
      lessThanOrEqual(prop("a", "str"), parameter("str", CTInteger))
    )
    val unsupportedForPushdown: Set[Expression] = Set(lessThanOrEqual(prop("a", "num"), countStar()))
    ShardPredicatePushdownPartition(
      fakePlanWithAvailableSymbolsForPredicates(supportedForPushdown ++ unsupportedForPushdown),
      context,
      supportedForPushdown ++ unsupportedForPushdown
    ) shouldEqual new ShardPredicatePushdownPartition(
      preFilterBeforePushdown = Set.empty,
      filterOnShards = Option(PushedPredicatesDetails(
        varFor("a"),
        supportedForPushdown,
        Set.empty,
        Set.empty
      )),
      filterOnMainWithRemoteProperties = unsupportedForPushdown
    )
  }

  test("should allow greater than") {
    val supportedForPushdown: Set[Expression] = Set(
      greaterThan(prop("a", "num"), prop("a", "num")),
      greaterThan(prop("a", "str"), literalInt(42)),
      greaterThan(prop("a", "str"), parameter("str", CTInteger))
    )
    val unsupportedForPushdown: Set[Expression] = Set(greaterThan(prop("a", "num"), countStar()))
    ShardPredicatePushdownPartition(
      fakePlanWithAvailableSymbolsForPredicates(supportedForPushdown ++ unsupportedForPushdown),
      context,
      supportedForPushdown ++ unsupportedForPushdown
    ) shouldEqual new ShardPredicatePushdownPartition(
      preFilterBeforePushdown = Set.empty,
      filterOnShards = Option(PushedPredicatesDetails(
        varFor("a"),
        supportedForPushdown,
        Set.empty,
        Set.empty
      )),
      filterOnMainWithRemoteProperties = unsupportedForPushdown
    )
  }

  test("should allow greaterThanOrEqual") {
    val supportedForPushdown: Set[Expression] = Set(
      greaterThanOrEqual(prop("a", "num"), prop("a", "num")),
      greaterThanOrEqual(prop("a", "str"), literalInt(42)),
      greaterThanOrEqual(prop("a", "str"), parameter("str", CTInteger))
    )
    val unsupportedForPushdown: Set[Expression] = Set(greaterThanOrEqual(prop("a", "num"), countStar()))
    ShardPredicatePushdownPartition(
      fakePlanWithAvailableSymbolsForPredicates(supportedForPushdown ++ unsupportedForPushdown),
      context,
      supportedForPushdown ++ unsupportedForPushdown
    ) shouldEqual new ShardPredicatePushdownPartition(
      preFilterBeforePushdown = Set.empty,
      filterOnShards = Option(PushedPredicatesDetails(
        varFor("a"),
        supportedForPushdown,
        Set.empty,
        Set.empty
      )),
      filterOnMainWithRemoteProperties = unsupportedForPushdown
    )
  }

  test("should allow equals") {
    val supportedForPushdown: Set[Expression] = Set(
      equals(prop("a", "num"), prop("a", "num")),
      equals(prop("a", "str"), literalInt(42)),
      equals(prop("a", "str"), parameter("str", CTInteger))
    )
    val unsupportedForPushdown: Set[Expression] = Set(equals(prop("a", "num"), countStar()))
    ShardPredicatePushdownPartition(
      fakePlanWithAvailableSymbolsForPredicates(supportedForPushdown ++ unsupportedForPushdown),
      context,
      supportedForPushdown ++ unsupportedForPushdown
    ) shouldEqual new ShardPredicatePushdownPartition(
      preFilterBeforePushdown = Set.empty,
      filterOnShards = Option(PushedPredicatesDetails(
        varFor("a"),
        supportedForPushdown,
        Set.empty,
        Set.empty
      )),
      filterOnMainWithRemoteProperties = unsupportedForPushdown
    )
  }

  test("should allow in") {
    val supportedForPushdown: Set[Expression] = Set(
      in(prop("a", "num"), prop("a", "num")),
      in(prop("a", "str"), literalInt(42)),
      in(prop("a", "str"), parameter("str", CTInteger))
    )
    val unsupportedForPushdown: Set[Expression] = Set(in(prop("a", "num"), countStar()))
    ShardPredicatePushdownPartition(
      fakePlanWithAvailableSymbolsForPredicates(supportedForPushdown ++ unsupportedForPushdown),
      context,
      supportedForPushdown ++ unsupportedForPushdown
    ) shouldEqual new ShardPredicatePushdownPartition(
      preFilterBeforePushdown = Set.empty,
      filterOnShards = Option(PushedPredicatesDetails(
        varFor("a"),
        supportedForPushdown,
        Set.empty,
        Set.empty
      )),
      filterOnMainWithRemoteProperties = unsupportedForPushdown
    )
  }

  test("should allow not equals") {
    val supportedForPushdown: Set[Expression] = Set(
      notEquals(prop("a", "num"), prop("a", "num")),
      notEquals(prop("a", "str"), literalInt(42)),
      notEquals(prop("a", "str"), parameter("str", CTInteger))
    )
    val unsupportedForPushdown: Set[Expression] = Set(notEquals(prop("a", "num"), countStar()))
    ShardPredicatePushdownPartition(
      fakePlanWithAvailableSymbolsForPredicates(supportedForPushdown ++ unsupportedForPushdown),
      context,
      supportedForPushdown ++ unsupportedForPushdown
    ) shouldEqual new ShardPredicatePushdownPartition(
      preFilterBeforePushdown = Set.empty,
      filterOnShards = Option(PushedPredicatesDetails(
        varFor("a"),
        supportedForPushdown,
        Set.empty,
        Set.empty
      )),
      filterOnMainWithRemoteProperties = unsupportedForPushdown
    )
  }

  test("should allow valid listliterals") {
    val listLiteralExpr = in(listOf(prop("n", "prop")), listOfInt(1))
    ShardPredicatePushdownPartition(
      fakePlanWithAvailableSymbolsForPredicates(Set(listLiteralExpr)),
      context,
      Set(listLiteralExpr)
    ) shouldEqual ShardPredicatePushdownPartition.withPredicatesOnShards(
      varFor("n"),
      Set(listLiteralExpr),
      Set.empty,
      Set.empty
    )
  }

  test("should allow anded property inequalities") {
    val andedPropertyInequality = andedPropertyInequalities(
      greaterThan(prop("n", "prop"), literalInt(10)),
      lessThan(prop("n", "prop"), literalInt(100))
    )

    ShardPredicatePushdownPartition(
      fakePlanWithAvailableSymbolsForPredicates(Set(andedPropertyInequality)),
      context,
      Set(andedPropertyInequality)
    ) shouldEqual ShardPredicatePushdownPartition.withPredicatesOnShards(
      varFor("n"),
      Set(andedPropertyInequality),
      Set.empty,
      Set.empty
    )
  }

  test("should use the most selective variable to pushdown to shards") {
    val selectivePredicate1 = equals(prop("a", "str"), literalInt(42))
    val selectivePredicate2 = greaterThan(prop("a", "prop"), literalInt(10))
    val otherPredicate = greaterThan(prop("b", "prop2"), literalInt(10))
    new givenConfig {
      cardinality = mapCardinality {
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _)
          if queryGraph.selections.flatPredicatesSet == Set(selectivePredicate1, selectivePredicate2) => 5.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _)
          if queryGraph.selections.flatPredicatesSet == Set(otherPredicate) => 7.0
      }

      override def updateSemanticTableWithTokens(table: SemanticTable): SemanticTable = mockedSemanticTable
    }.withLogicalPlanningContext { (_, context) =>
      val predicates: Set[Expression] = Set(selectivePredicate1, selectivePredicate2, otherPredicate)

      // let the incoming plan have a cardinality of 10
      val plan = fakePlanWithAvailableSymbolsForPredicates(predicates)
      context.staticComponents.planningAttributes.solveds.set(plan.id, SinglePlannerQuery.empty)
      context.staticComponents.planningAttributes.cardinalities.set(plan.id, 10.0)
      context.staticComponents.planningAttributes.providedOrders.set(plan.id, ProvidedOrder.empty)

      // the selected predicates to push down should be for variable "a", i.e., selectivePredicate1 and selectivePredicate2.

      ShardPredicatePushdownPartition(
        plan,
        context,
        predicates
      ) shouldEqual new ShardPredicatePushdownPartition(
        preFilterBeforePushdown = Set.empty,
        filterOnShards = Option(PushedPredicatesDetails(
          varFor("a"),
          Set(selectivePredicate1, selectivePredicate2),
          Set.empty,
          Set.empty
        )),
        filterOnMainWithRemoteProperties = Set(otherPredicate)
      )
    }
  }

  test("should not push down predicates in the slotted runtime") {
    val pushdownablePredicate = equals(prop("a", "str"), literalInt(42))
    val slottedContext = context.copy(settings = context.settings.copy(executionModel = Volcano))

    val plan = fakePlanWithAvailableSymbolsForPredicates(Set(pushdownablePredicate))

    // the otherwise pushdownable predicate should not have been pushed down.
    ShardPredicatePushdownPartition(
      plan,
      slottedContext,
      Set(pushdownablePredicate)
    ) shouldEqual ShardPredicatePushdownPartition.withFilterOnMainWithRemoteProperties(Set(pushdownablePredicate))
  }

  test("should support pre-filtering in the slotted runtime") {
    val context: LogicalPlanningContext =
      newMockedLogicalPlanningContext(newMockedPlanContext(), semanticTable = mockedSemanticTable)
    val slottedContext = context.copy(settings = context.settings.copy(executionModel = Volcano))
    val fakePlan: FakeLeafPlan = fakeLogicalPlanFor(slottedContext.staticComponents.planningAttributes, "x")
    // given the property num is already cached for variable a
    slottedContext.staticComponents.planningAttributes.cachedPropertiesPerPlan.set(
      fakePlan.id,
      CachedProperties.singleton(varFor("a"), varFor("a"), NODE_TYPE, Set(PropertyKeyName("num")(InputPosition.NONE)))
    )

    val predicates: Set[Expression] = Set(
      equals(prop("a", "num"), prop("a", "num"))
    )
    ShardPredicatePushdownPartition(
      fakePlan,
      slottedContext,
      predicates
    ) shouldEqual ShardPredicatePushdownPartition.withPreFilterBeforePushdown(predicates)
  }

  test("should support predicate pushdown for relationships") {
    val mockedTableWithRelationships = mock[SemanticTable]
    when(mockedTableWithRelationships.typeFor(any[Expression])).thenReturn(
      SemanticTable.TypeGetter(Some(CTRelationship))
    )

    val context: LogicalPlanningContext =
      newMockedLogicalPlanningContext(newMockedPlanContext(), semanticTable = mockedTableWithRelationships)
    val predicates: Set[Expression] = Set(
      equals(prop("a", "num"), prop("a", "num"))
    )
    val fakePlan: FakeLeafPlan = fakePlanWithAvailableSymbolsForPredicates(predicates)

    ShardPredicatePushdownPartition(
      fakePlan,
      context,
      predicates
    ) shouldEqual ShardPredicatePushdownPartition.withPredicatesOnShards(
      varFor("a"),
      predicates,
      Set.empty,
      Set.empty
    )
  }

  test("should not support predicate pushdown for property accesses on other entity types") {
    val mockedTableWithMaps = mock[SemanticTable]
    when(mockedTableWithMaps.typeFor(any[Expression])).thenReturn(
      SemanticTable.TypeGetter(Some(CTMap))
    )

    val context: LogicalPlanningContext =
      newMockedLogicalPlanningContext(newMockedPlanContext(), semanticTable = mockedTableWithMaps)
    val fakePlan: FakeLeafPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")

    val predicates: Set[Expression] = Set(
      equals(prop("a", "num"), prop("a", "num"))
    )
    ShardPredicatePushdownPartition(
      fakePlan,
      context,
      predicates
    ) shouldEqual ShardPredicatePushdownPartition.withFilterOnMainWithRemoteProperties(
      predicates
    )
  }

  test("should prefilter with already cached properties") {
    val context: LogicalPlanningContext =
      newMockedLogicalPlanningContext(newMockedPlanContext(), semanticTable = mockedSemanticTable)
    val fakePlan: FakeLeafPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")
    // given the property num is already cached for variable a
    context.staticComponents.planningAttributes.cachedPropertiesPerPlan.set(
      fakePlan.id,
      CachedProperties.singleton(varFor("a"), varFor("a"), NODE_TYPE, Set(PropertyKeyName("num")(InputPosition.NONE)))
    )

    val predicates: Set[Expression] = Set(
      equals(prop("a", "num"), prop("a", "num"))
    )
    ShardPredicatePushdownPartition(
      fakePlan,
      context,
      predicates
    ) shouldEqual ShardPredicatePushdownPartition.withPreFilterBeforePushdown(predicates)
  }

  test("should not push down predicates when their dependencies are not available") {
    val predicates: Set[Expression] = Set(
      equals(prop("a", "num"), prop("a", "num")),
      equals(prop("a", "str"), literalInt(42)),
      equals(prop("a", "str"), parameter("str", CTInteger))
    )
    ShardPredicatePushdownPartition(
      fakePlanWithAvailableSymbolsForPredicates(Set.empty),
      context,
      predicates
    ) shouldEqual ShardPredicatePushdownPartition(
      preFilterBeforePushdown = Set.empty,
      filterOnShards = None,
      filterOnMainWithRemoteProperties = predicates
    )
  }

  test("should recorded imported cached properties from different variables in pushdown") {
    val context: LogicalPlanningContext =
      newMockedLogicalPlanningContext(newMockedPlanContext(), semanticTable = mockedSemanticTable)

    val predicates: Set[Expression] = Set(
      equals(prop("b", "num"), prop("a", "num"))
    )
    val fakePlan: FakeLeafPlan = fakePlanWithAvailableSymbolsForPredicates(predicates)

    // given the property num is already cached for variable a
    context.staticComponents.planningAttributes.cachedPropertiesPerPlan.set(
      fakePlan.id,
      CachedProperties.singleton(varFor("a"), varFor("a"), NODE_TYPE, Set(PropertyKeyName("num")(InputPosition.NONE)))
    )

    ShardPredicatePushdownPartition(
      fakePlan,
      context,
      predicates
    ) shouldEqual ShardPredicatePushdownPartition.withPredicatesOnShards(
      variable = varFor("b"),
      exprs = predicates,
      importedPerRowValues = Set(prop("a", "num")),
      importedConstantValues = Set.empty
    )
  }

  test("should recorded imported cached properties for the same variable being pushed down") {
    val context: LogicalPlanningContext =
      newMockedLogicalPlanningContext(newMockedPlanContext(), semanticTable = mockedSemanticTable)

    val predicates: Set[Expression] = Set(
      equals(prop("a", "num2"), prop("a", "num1"))
    )
    val fakePlan: FakeLeafPlan = fakePlanWithAvailableSymbolsForPredicates(predicates)

    // given the property num1 is already cached for variable a
    context.staticComponents.planningAttributes.cachedPropertiesPerPlan.set(
      fakePlan.id,
      CachedProperties.singleton(varFor("a"), varFor("a"), NODE_TYPE, Set(PropertyKeyName("num1")(InputPosition.NONE)))
    )

    ShardPredicatePushdownPartition(
      fakePlan,
      context,
      predicates
    ) shouldEqual ShardPredicatePushdownPartition.withPredicatesOnShards(
      variable = varFor("a"),
      exprs = predicates,
      importedPerRowValues = Set(prop("a", "num1")),
      importedConstantValues = Set.empty
    )
  }
}
