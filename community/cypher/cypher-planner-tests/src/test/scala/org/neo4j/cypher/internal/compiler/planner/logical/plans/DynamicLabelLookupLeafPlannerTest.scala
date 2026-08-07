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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.DynamicLabelLookupLeafPlanner
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.ir.ordering.RequiredOrderCandidate
import org.neo4j.cypher.internal.logical.plans.DynamicElement
import org.neo4j.cypher.internal.logical.plans.DynamicLabelNodeLookup
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

class DynamicLabelLookupLeafPlannerTest extends CypherPlannerTestSuite with LogicalPlanningTestSupport {

  test("simple dynamic label scan") {
    val n = v"n"

    val queryGraph = QueryGraph(
      selections = Selections.from(hasDynamicLabels(n, literalString("A"))),
      patternNodes = Set(n)
    )

    dynamicLabelScanLeafPlans(queryGraph) shouldEqual Set(
      DynamicLabelNodeLookup(
        idName = n,
        labelExpr = DynamicElement.Simple(literalString("A"), DynamicElement.All),
        argumentIds = Set.empty,
        propertyPredicates = Map.empty
      )
    )
  }

  test("dynamic label scan for the empty list") {
    val n = v"n"

    val queryGraph = QueryGraph(
      selections = Selections.from(hasDynamicLabels(n, listOf())),
      patternNodes = Set(n)
    )

    dynamicLabelScanLeafPlans(queryGraph) shouldEqual Set(
      DynamicLabelNodeLookup(
        idName = n,
        labelExpr = DynamicElement.Simple(listOf(), DynamicElement.All),
        argumentIds = Set.empty,
        propertyPredicates = Map.empty
      )
    )
  }

  test("dynamic label scan – conjunction") {
    val n = v"n"

    val queryGraph = QueryGraph(
      selections = Selections.from(hasDynamicLabels(n, listOfString("A", "B"))),
      patternNodes = Set(n)
    )

    dynamicLabelScanLeafPlans(queryGraph) shouldEqual Set(
      DynamicLabelNodeLookup(
        n,
        DynamicElement.Simple(listOfString("A", "B"), DynamicElement.All),
        Set.empty,
        propertyPredicates = Map.empty
      )
    )
  }

  test("dynamic label scan – disjunction") {
    val n = v"n"

    val queryGraph = QueryGraph(
      selections = Selections.from(hasAnyDynamicLabel(n, listOfString("A", "B"))),
      patternNodes = Set(n)
    )

    dynamicLabelScanLeafPlans(queryGraph) shouldEqual Set(
      DynamicLabelNodeLookup(
        n,
        DynamicElement.Simple(listOfString("A", "B"), DynamicElement.Any),
        Set.empty,
        propertyPredicates = Map.empty
      )
    )
  }

  test("no dynamic label scan in the absence of dynamic labels") {
    val n = v"n"

    val queryGraph = QueryGraph(
      selections = Selections.from(hasLabels(n, "A")),
      patternNodes = Set(n)
    )

    dynamicLabelScanLeafPlans(queryGraph) shouldEqual Set.empty
  }

  test("no dynamic label scan in the presence of multiples labels lists – currently never created") {
    val n = v"n"

    val queryGraph = QueryGraph(
      selections = Selections.from(hasAnyDynamicLabel(n, listOfString("A", "B"), literalString("C"))),
      patternNodes = Set(n)
    )

    dynamicLabelScanLeafPlans(queryGraph) shouldEqual Set.empty
  }

  test("no dynamic label scan if the variable is an argument") {
    val n = v"n"

    val queryGraph = QueryGraph(
      selections = Selections.from(hasAnyDynamicLabel(n, listOfString("A", "B"))),
      patternNodes = Set(n),
      argumentIds = Set(n)
    )

    dynamicLabelScanLeafPlans(queryGraph) shouldEqual Set.empty
  }

  test("no dynamic label scan if the variable is not in the query graph") {
    val queryGraph = QueryGraph(
      selections = Selections.from(hasAnyDynamicLabel(v"n", listOfString("A", "B"))),
      patternNodes = Set(v"m")
    )

    dynamicLabelScanLeafPlans(queryGraph) shouldEqual Set.empty
  }

  test("dynamic label scan with no order") {
    val n = v"n"

    val queryGraph = QueryGraph(
      selections = Selections.from(hasDynamicLabels(n, literalString("A"))),
      patternNodes = Set(n)
    )

    val interestingOrderConfig = InterestingOrderConfig(InterestingOrder.required(RequiredOrderCandidate.asc(n)))

    dynamicLabelScanLeafPlans(queryGraph, interestingOrderConfig = interestingOrderConfig) shouldEqual Set(
      DynamicLabelNodeLookup(
        idName = n,
        labelExpr = DynamicElement.Simple(literalString("A"), DynamicElement.All),
        argumentIds = Set.empty,
        propertyPredicates = Map.empty
      )
    )
  }

  test("dynamic label scan ignoring irrelevant order") {
    val n = v"n"

    val queryGraph = QueryGraph(
      selections = Selections.from(hasDynamicLabels(n, literalString("A"))),
      patternNodes = Set(n)
    )

    val interestingOrderConfig = InterestingOrderConfig(InterestingOrder.required(RequiredOrderCandidate.desc(v"m")))

    dynamicLabelScanLeafPlans(queryGraph, interestingOrderConfig = interestingOrderConfig) shouldEqual Set(
      DynamicLabelNodeLookup(
        idName = n,
        labelExpr = DynamicElement.Simple(literalString("A"), DynamicElement.All),
        argumentIds = Set.empty,
        propertyPredicates = Map.empty
      )
    )
  }

  final private def dynamicLabelScanLeafPlans(
    queryGraph: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig = InterestingOrderConfig.empty
  ): Set[LogicalPlan] = {
    val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext(),
      semanticTable = new SemanticTable()
    )
    DynamicLabelLookupLeafPlanner(queryGraph, interestingOrderConfig, context)
  }
}
