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
import org.neo4j.cypher.internal.compiler.planner.logical.steps.DynamicRelationshipTypeLookupLeafPlanner
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.ir.ordering.RequiredOrderCandidate
import org.neo4j.cypher.internal.logical.plans.DynamicDirectedRelationshipTypeLookup
import org.neo4j.cypher.internal.logical.plans.DynamicElement
import org.neo4j.cypher.internal.logical.plans.DynamicUndirectedRelationshipTypeLookup
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Selection

class DynamicRelationshipTypeLookupLeafPlannerTest extends CypherPlannerTestSuite with LogicalPlanningTestSupport {

  test("simple dynamic outgoing relationship type scan") {
    val r = v"r"
    val a = v"a"
    val b = v"b"

    val queryGraph = QueryGraph(
      selections = Selections.from(hasDynamicType(r, literalString("R"))),
      patternRelationships = Set(PatternRelationship(r, (a, b), OUTGOING, Nil, SimplePatternLength))
    )

    dynamicRelationshipTypeScanLeafPlans(queryGraph) shouldEqual Set(
      DynamicDirectedRelationshipTypeLookup(
        idName = Some(r),
        startNode = Some(a),
        relType = DynamicElement.Simple(literalString("R"), DynamicElement.All),
        endNode = Some(b),
        argumentIds = Set.empty,
        indexOrder = IndexOrderNone,
        propertyPredicates = Map.empty
      )
    )
  }

  test("simple dynamic incoming relationship type scan") {
    val r = v"r"
    val a = v"a"
    val b = v"b"

    val queryGraph = QueryGraph(
      selections = Selections.from(hasDynamicType(r, literalString("R"))),
      patternRelationships = Set(PatternRelationship(r, (a, b), INCOMING, Nil, SimplePatternLength))
    )

    dynamicRelationshipTypeScanLeafPlans(queryGraph) shouldEqual Set(
      DynamicDirectedRelationshipTypeLookup(
        idName = Some(r),
        startNode = Some(b),
        relType = DynamicElement.Simple(literalString("R"), DynamicElement.All),
        endNode = Some(a),
        argumentIds = Set.empty,
        indexOrder = IndexOrderNone,
        propertyPredicates = Map.empty
      )
    )
  }

  test("simple dynamic undirected relationship type scan") {
    val r = v"r"
    val a = v"a"
    val b = v"b"

    val queryGraph = QueryGraph(
      selections = Selections.from(hasDynamicType(r, literalString("R"))),
      patternRelationships = Set(PatternRelationship(r, (a, b), BOTH, Nil, SimplePatternLength))
    )

    dynamicRelationshipTypeScanLeafPlans(queryGraph) shouldEqual Set(
      DynamicUndirectedRelationshipTypeLookup(
        idName = Some(r),
        leftNode = Some(a),
        relType = DynamicElement.Simple(literalString("R"), DynamicElement.All),
        rightNode = Some(b),
        argumentIds = Set.empty,
        indexOrder = IndexOrderNone,
        propertyPredicates = Map.empty
      )
    )
  }

  test("dynamic relationship type scan for the empty list") {
    val r = v"r"
    val a = v"a"
    val b = v"b"

    val queryGraph = QueryGraph(
      selections = Selections.from(hasDynamicType(r, listOf())),
      patternRelationships = Set(PatternRelationship(r, (a, b), BOTH, Nil, SimplePatternLength))
    )

    dynamicRelationshipTypeScanLeafPlans(queryGraph) shouldEqual Set(
      DynamicUndirectedRelationshipTypeLookup(
        idName = Some(r),
        leftNode = Some(a),
        relType = DynamicElement.Simple(listOf(), DynamicElement.All),
        rightNode = Some(b),
        argumentIds = Set.empty,
        indexOrder = IndexOrderNone,
        propertyPredicates = Map.empty
      )
    )
  }

  test("dynamic relationship type scan – conjunction (no results but valid)") {
    val r = v"r"
    val a = v"a"
    val b = v"b"

    val queryGraph = QueryGraph(
      selections = Selections.from(hasDynamicType(r, listOfString("R", "S"))),
      patternRelationships = Set(PatternRelationship(r, (a, b), BOTH, Nil, SimplePatternLength))
    )

    dynamicRelationshipTypeScanLeafPlans(queryGraph) shouldEqual Set(
      DynamicUndirectedRelationshipTypeLookup(
        idName = Some(r),
        leftNode = Some(a),
        relType = DynamicElement.Simple(listOfString("R", "S"), DynamicElement.All),
        rightNode = Some(b),
        argumentIds = Set.empty,
        indexOrder = IndexOrderNone,
        propertyPredicates = Map.empty
      )
    )
  }

  test("dynamic relationship type scan – disjunction") {
    val r = v"r"
    val a = v"a"
    val b = v"b"

    val queryGraph = QueryGraph(
      selections = Selections.from(hasAnyDynamicType(r, listOfString("R", "S"))),
      patternRelationships = Set(PatternRelationship(r, (a, b), BOTH, Nil, SimplePatternLength))
    )

    dynamicRelationshipTypeScanLeafPlans(queryGraph) shouldEqual Set(
      DynamicUndirectedRelationshipTypeLookup(
        idName = Some(r),
        leftNode = Some(a),
        relType = DynamicElement.Simple(listOfString("R", "S"), DynamicElement.Any),
        rightNode = Some(b),
        argumentIds = Set.empty,
        indexOrder = IndexOrderNone,
        propertyPredicates = Map.empty
      )
    )
  }

  test("dynamic relationship type scan with expression as argument") {
    val r = v"r"
    val a = v"a"
    val b = v"b"
    val types = v"types"

    val queryGraph = QueryGraph(
      selections = Selections.from(hasDynamicType(r, types)),
      patternRelationships = Set(PatternRelationship(r, (a, b), BOTH, Nil, SimplePatternLength)),
      argumentIds = Set(types)
    )

    dynamicRelationshipTypeScanLeafPlans(queryGraph) shouldEqual Set(
      DynamicUndirectedRelationshipTypeLookup(
        idName = Some(r),
        leftNode = Some(a),
        relType = DynamicElement.Simple(types, DynamicElement.All),
        rightNode = Some(b),
        argumentIds = Set(types),
        indexOrder = IndexOrderNone,
        propertyPredicates = Map.empty
      )
    )
  }

  test("no dynamic relationship type scan if the variable is not in the query graph") {
    val queryGraph = QueryGraph(
      selections = Selections.from(hasAnyDynamicType(v"s", listOfString("R", "S"))),
      patternRelationships = Set(PatternRelationship(v"r", (v"a", v"b"), BOTH, Nil, SimplePatternLength))
    )

    dynamicRelationshipTypeScanLeafPlans(queryGraph) shouldEqual Set.empty
  }

  test("dynamic relationship type scan when an endpoint is bound") {
    val r = v"r"
    val a = v"a"
    val b = v"b"

    val queryGraph = QueryGraph(
      selections = Selections.from(hasDynamicType(r, literalString("R"))),
      patternRelationships = Set(PatternRelationship(r, (a, b), OUTGOING, Nil, SimplePatternLength)),
      argumentIds = Set(a)
    )

    val unnamed0 = v"  UNNAMED0"

    dynamicRelationshipTypeScanLeafPlans(queryGraph) shouldEqual Set(
      Selection(
        predicate = ands(equals(a, unnamed0)),
        source = DynamicDirectedRelationshipTypeLookup(
          idName = Some(r),
          startNode = Some(unnamed0),
          relType = DynamicElement.Simple(literalString("R"), DynamicElement.All),
          endNode = Some(b),
          argumentIds = Set(a),
          indexOrder = IndexOrderNone,
          propertyPredicates = Map.empty
        )
      )
    )
  }

  test("dynamic relationship type scan when both endpoints are bound") {
    val r = v"r"
    val a = v"a"
    val b = v"b"

    val queryGraph = QueryGraph(
      selections = Selections.from(hasDynamicType(r, literalString("R"))),
      patternRelationships = Set(PatternRelationship(r, (a, b), BOTH, Nil, SimplePatternLength)),
      argumentIds = Set(a, b)
    )

    val unnamed0 = v"  UNNAMED0"
    val unnamed1 = v"  UNNAMED1"

    dynamicRelationshipTypeScanLeafPlans(queryGraph) shouldEqual Set(
      Selection(
        predicate = ands(
          equals(a, unnamed0),
          equals(b, unnamed1)
        ),
        source = DynamicUndirectedRelationshipTypeLookup(
          idName = Some(r),
          leftNode = Some(unnamed0),
          relType = DynamicElement.Simple(literalString("R"), DynamicElement.All),
          rightNode = Some(unnamed1),
          argumentIds = Set(a, b),
          indexOrder = IndexOrderNone,
          propertyPredicates = Map.empty
        )
      )
    )
  }

  test("dynamic relationship type scan when both endpoints are equal") {
    val r = v"r"
    val a = v"a"

    val queryGraph = QueryGraph(
      selections = Selections.from(hasDynamicType(r, literalString("R"))),
      patternRelationships = Set(PatternRelationship(r, (a, a), BOTH, Nil, SimplePatternLength)),
      argumentIds = Set.empty
    )

    val unnamed0 = v"  UNNAMED0"

    dynamicRelationshipTypeScanLeafPlans(queryGraph) shouldEqual Set(
      Selection(
        predicate = ands(
          equals(a, unnamed0)
        ),
        source = DynamicUndirectedRelationshipTypeLookup(
          idName = Some(r),
          leftNode = Some(a),
          relType = DynamicElement.Simple(literalString("R"), DynamicElement.All),
          rightNode = Some(unnamed0),
          argumentIds = Set.empty,
          indexOrder = IndexOrderNone,
          propertyPredicates = Map.empty
        )
      )
    )
  }

  test("dynamic relationship type scan when both endpoints are equal and bound") {
    val r = v"r"
    val a = v"a"

    val queryGraph = QueryGraph(
      selections = Selections.from(hasDynamicType(r, literalString("R"))),
      patternRelationships = Set(PatternRelationship(r, (a, a), INCOMING, Nil, SimplePatternLength)),
      argumentIds = Set(a)
    )

    val unnamed0 = v"  UNNAMED0"
    val unnamed1 = v"  UNNAMED1"

    dynamicRelationshipTypeScanLeafPlans(queryGraph) shouldEqual Set(
      Selection(
        predicate = ands(
          equals(a, unnamed0),
          equals(a, unnamed1)
        ),
        source = DynamicDirectedRelationshipTypeLookup(
          idName = Some(r),
          startNode = Some(unnamed1),
          relType = DynamicElement.Simple(literalString("R"), DynamicElement.All),
          endNode = Some(unnamed0),
          argumentIds = Set(a),
          indexOrder = IndexOrderNone,
          propertyPredicates = Map.empty
        )
      )
    )
  }

  test("dynamic relationship type scan in ascending order") {
    val r = v"r"
    val a = v"a"
    val b = v"b"

    val queryGraph = QueryGraph(
      selections = Selections.from(hasDynamicType(r, literalString("R"))),
      patternRelationships = Set(PatternRelationship(r, (a, b), OUTGOING, Nil, SimplePatternLength))
    )

    val interestingOrderConfig = InterestingOrderConfig(InterestingOrder.required(RequiredOrderCandidate.asc(r)))

    dynamicRelationshipTypeScanLeafPlans(queryGraph, interestingOrderConfig = interestingOrderConfig) shouldEqual Set(
      DynamicDirectedRelationshipTypeLookup(
        idName = Some(r),
        startNode = Some(a),
        relType = DynamicElement.Simple(literalString("R"), DynamicElement.All),
        endNode = Some(b),
        argumentIds = Set.empty,
        indexOrder = IndexOrderAscending,
        propertyPredicates = Map.empty
      )
    )
  }

  test("dynamic relationship type scan in ascending order renamed") {
    val r = v"r"
    val a = v"a"
    val b = v"b"

    val queryGraph = QueryGraph(
      selections = Selections.from(hasDynamicType(r, literalString("R"))),
      patternRelationships = Set(PatternRelationship(r, (a, b), OUTGOING, Nil, SimplePatternLength))
    )

    val s = v"s"
    val candidate = RequiredOrderCandidate.asc(s, Map(s -> r))
    val interestingOrderConfig = InterestingOrderConfig(InterestingOrder.required(candidate))

    dynamicRelationshipTypeScanLeafPlans(queryGraph, interestingOrderConfig = interestingOrderConfig) shouldEqual Set(
      DynamicDirectedRelationshipTypeLookup(
        idName = Some(r),
        startNode = Some(a),
        relType = DynamicElement.Simple(literalString("R"), DynamicElement.All),
        endNode = Some(b),
        argumentIds = Set.empty,
        indexOrder = IndexOrderAscending,
        propertyPredicates = Map.empty
      )
    )
  }

  test("dynamic relationship type scan in descending order") {
    val r = v"r"
    val a = v"a"
    val b = v"b"

    val queryGraph = QueryGraph(
      selections = Selections.from(hasDynamicType(r, literalString("R"))),
      patternRelationships = Set(PatternRelationship(r, (a, b), INCOMING, Nil, SimplePatternLength))
    )

    val interestingOrderConfig = InterestingOrderConfig(InterestingOrder.required(RequiredOrderCandidate.desc(r)))

    dynamicRelationshipTypeScanLeafPlans(queryGraph, interestingOrderConfig = interestingOrderConfig) shouldEqual Set(
      DynamicDirectedRelationshipTypeLookup(
        idName = Some(r),
        startNode = Some(b),
        relType = DynamicElement.Simple(literalString("R"), DynamicElement.All),
        endNode = Some(a),
        argumentIds = Set.empty,
        indexOrder = IndexOrderDescending,
        propertyPredicates = Map.empty
      )
    )
  }

  test("dynamic relationship type scan ignoring irrelevant order") {
    val r = v"r"
    val a = v"a"
    val b = v"b"

    val queryGraph = QueryGraph(
      selections = Selections.from(hasDynamicType(r, literalString("R"))),
      patternRelationships = Set(PatternRelationship(r, (a, b), BOTH, Nil, SimplePatternLength))
    )

    val interestingOrderConfig = InterestingOrderConfig(InterestingOrder.required(RequiredOrderCandidate.asc(a)))

    dynamicRelationshipTypeScanLeafPlans(queryGraph, interestingOrderConfig = interestingOrderConfig) shouldEqual Set(
      DynamicUndirectedRelationshipTypeLookup(
        idName = Some(r),
        leftNode = Some(a),
        relType = DynamicElement.Simple(literalString("R"), DynamicElement.All),
        rightNode = Some(b),
        argumentIds = Set.empty,
        indexOrder = IndexOrderNone,
        propertyPredicates = Map.empty
      )
    )
  }

  final private def dynamicRelationshipTypeScanLeafPlans(
    queryGraph: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig = InterestingOrderConfig.empty
  ): Set[LogicalPlan] = {
    val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext(),
      semanticTable = new SemanticTable()
    )
    DynamicRelationshipTypeLookupLeafPlanner(queryGraph, interestingOrderConfig, context)
  }
}
