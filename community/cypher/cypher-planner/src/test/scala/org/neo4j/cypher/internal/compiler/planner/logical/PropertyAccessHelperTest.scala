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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper.PropertyAccess
import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper.findAggregationPropertyAccesses
import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper.findLocalPropertyAccesses
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.frontend.phases.FieldSignature
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.QualifiedName
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class PropertyAccessHelperTest extends CypherFunSuite with LogicalPlanningTestSupport {
  val context: LogicalPlanningContext = newMockedLogicalPlanningContext(newMockedPlanContext())

  test("should return input context if no aggregation in horizon") {
    val plannerQuery = buildSinglePlannerQuery("MATCH (n) RETURN n.prop")
    val result =
      context.withModifiedPlannerState(_.withAggregationProperties(findAggregationPropertyAccesses(plannerQuery)))

    assertContextNotUpdated(result)
  }

  test("should return input context if aggregation with grouping in horizon") {
    val plannerQuery = buildSinglePlannerQuery("MATCH (n) RETURN min(n.prop), n.prop")
    val result =
      context.withModifiedPlannerState(_.withAggregationProperties(findAggregationPropertyAccesses(plannerQuery)))

    assertContextNotUpdated(result)
  }

  test("should return input context if no properties could be extracted") {
    val plannerQuery = buildSinglePlannerQuery("MATCH (n) RETURN min(size(n.prop))")
    val result =
      context.withModifiedPlannerState(_.withAggregationProperties(findAggregationPropertyAccesses(plannerQuery)))

    assertContextNotUpdated(result)
  }

  test("should return input context if query has mutating patterns before aggregation") {
    val plannerQuery = buildSinglePlannerQuery("MATCH (n:Label) CREATE (:NewLabel) RETURN min(n.prop)")
    val result =
      context.withModifiedPlannerState(_.withAggregationProperties(findAggregationPropertyAccesses(plannerQuery)))

    assertContextNotUpdated(result)
  }

  test("should return input context if query has mutating patterns before renaming") {
    val plannerQuery =
      buildSinglePlannerQuery("MATCH (n:Label) CREATE (:NewLabel) WITH n.prop AS prop RETURN min(prop)")
    val result =
      context.withModifiedPlannerState(_.withAggregationProperties(findAggregationPropertyAccesses(plannerQuery)))

    assertContextNotUpdated(result)
  }

  test("should return input context for merge before aggregation") {
    val plannerQuery = buildSinglePlannerQuery("MERGE (n:Label) RETURN min(n.prop)")
    val result =
      context.withModifiedPlannerState(_.withAggregationProperties(findAggregationPropertyAccesses(plannerQuery)))

    assertContextNotUpdated(result)
  }

  test("should return updated context if no mutating patterns before aggregation") {
    val plannerQuery = buildSinglePlannerQuery("MATCH (n) RETURN min(n.prop)")
    val result =
      context.withModifiedPlannerState(_.withAggregationProperties(findAggregationPropertyAccesses(plannerQuery)))

    assertContextUpdated(result, Set(PropertyAccess(v"n", "prop")))
  }

  test("should return updated context for two aggregation functions") {
    val plannerQuery = buildSinglePlannerQuery("MATCH (n) RETURN min(n.prop), max(n.foo)")
    val result =
      context.withModifiedPlannerState(_.withAggregationProperties(findAggregationPropertyAccesses(plannerQuery)))

    assertContextUpdated(result, Set(PropertyAccess(v"n", "prop"), PropertyAccess(v"n", "foo")))
  }

  test("should return input context for two aggregation functions, but one without property access") {
    val plannerQuery = buildSinglePlannerQuery("MATCH (n) RETURN min(n.prop), count(n)")
    val result =
      context.withModifiedPlannerState(_.withAggregationProperties(findAggregationPropertyAccesses(plannerQuery)))

    assertContextNotUpdated(result)
  }

  test("addAggregatedPropertiesToContext should be usable for different queries") {
    val plannerQuery1 = buildSinglePlannerQuery("MATCH (n) WITH n.prop AS prop RETURN min(prop)")
    context.withModifiedPlannerState(_.withAggregationProperties(findAggregationPropertyAccesses(plannerQuery1)))
    val plannerQuery2 = buildSinglePlannerQuery("MATCH (prop) RETURN min(prop)")
    val result2 =
      context.withModifiedPlannerState(_.withAggregationProperties(findAggregationPropertyAccesses(plannerQuery2)))

    assertContextNotUpdated(result2)
  }

  test("should return updated context if no mutating patterns before projection followed by aggregation") {
    val plannerQuery = buildSinglePlannerQuery("MATCH (n) WITH n.prop AS prop RETURN min(prop)")
    val result =
      context.withModifiedPlannerState(_.withAggregationProperties(findAggregationPropertyAccesses(plannerQuery)))

    assertContextUpdated(result, Set(PropertyAccess(v"n", "prop")))
  }

  test("should return updated context if mutating patterns after aggregation") {
    val plannerQuery = buildSinglePlannerQuery("MATCH (n:Label) WITH min(n.prop) AS min CREATE (:NewLabel) RETURN min")
    val result =
      context.withModifiedPlannerState(_.withAggregationProperties(findAggregationPropertyAccesses(plannerQuery)))

    assertContextUpdated(result, Set(PropertyAccess(v"n", "prop")))
  }

  test("should return updated context for unwind before aggregation") {
    val plannerQuery = buildSinglePlannerQuery("UNWIND [1,2,3] AS i MATCH (n) RETURN min(n.prop)")
    val result =
      context.withModifiedPlannerState(_.withAggregationProperties(findAggregationPropertyAccesses(plannerQuery)))

    assertContextUpdated(result, Set(PropertyAccess(v"n", "prop")))
  }

  test("should return updated context for distinct before aggregation") {
    val plannerQuery = buildSinglePlannerQuery("MATCH (n) WITH DISTINCT n.prop AS prop RETURN min(prop)")
    val result =
      context.withModifiedPlannerState(_.withAggregationProperties(findAggregationPropertyAccesses(plannerQuery)))

    assertContextUpdated(result, Set(PropertyAccess(v"n", "prop")))
  }

  test("should return updated context for LOAD CSV before aggregation") {
    val plannerQuery = buildSinglePlannerQuery(
      "LOAD CSV WITH HEADERS FROM '$url' AS row MATCH (n) WHERE toInteger(row.Value) > 20 RETURN count(n.prop)"
    )
    val result =
      context.withModifiedPlannerState(_.withAggregationProperties(findAggregationPropertyAccesses(plannerQuery)))

    assertContextUpdated(result, Set(PropertyAccess(v"n", "prop")))
  }

  test("should return updated context for procedure call before aggregation") {
    val qualifiedName = QualifiedName(Seq("db"), "labels")
    val lookup = Some(Map(qualifiedName -> ProcedureSignature(
      name = qualifiedName,
      inputSignature = Array.empty[FieldSignature],
      outputSignature = Some(IndexedSeq(FieldSignature("label", CTString))),
      deprecationInfo = None,
      accessMode = ProcedureReadOnlyAccess,
      id = 8
    )))
    val plannerQuery =
      buildSinglePlannerQuery("MATCH (n) CALL db.labels() YIELD label RETURN count(n.prop)", procedureLookup = lookup)
    val result =
      context.withModifiedPlannerState(_.withAggregationProperties(findAggregationPropertyAccesses(plannerQuery)))

    assertContextUpdated(result, Set(PropertyAccess(v"n", "prop")))
  }

  test("should find no property accesses") {
    val plannerQuery = buildSinglePlannerQuery("MATCH (n) RETURN n")
    val result = context.withModifiedPlannerState(_
      .withAccessedProperties(findLocalPropertyAccesses(plannerQuery)))

    assertContextNotUpdated(result)
  }

  test("should find property accesses in projection") {
    val plannerQuery = buildSinglePlannerQuery("MATCH (n) RETURN n.prop, n.foo")
    val result = context.withModifiedPlannerState(_
      .withAccessedProperties(findLocalPropertyAccesses(plannerQuery)))

    assertContextUpdated(result, Set(), Set(PropertyAccess(v"n", "prop"), PropertyAccess(v"n", "foo")))
  }

  test("should find property accesses nested in expressions") {
    val plannerQuery = buildSinglePlannerQuery("MATCH (n)-[r]-() RETURN n.prop + 1, count(r.foo)")
    val result = context.withModifiedPlannerState(_
      .withAccessedProperties(findLocalPropertyAccesses(plannerQuery)))

    assertContextUpdated(result, Set(), Set(PropertyAccess(v"n", "prop"), PropertyAccess(v"r", "foo")))
  }

  test("should find property accesses in selection") {
    val plannerQuery = buildSinglePlannerQuery("MATCH (n) WHERE n.prop = n.foo RETURN n")
    val result = context.withModifiedPlannerState(_
      .withAccessedProperties(findLocalPropertyAccesses(plannerQuery)))

    assertContextUpdated(result, Set(), Set(PropertyAccess(v"n", "prop"), PropertyAccess(v"n", "foo")))
  }

  test("should find property accesses from pattern component") {
    val plannerQuery = buildSinglePlannerQuery("MATCH (n), (m {foo: n.prop}) RETURN m.bar")
    val result = context.withModifiedPlannerState(_
      .withAccessedProperties(findLocalPropertyAccesses(plannerQuery)))

    assertContextUpdated(
      result,
      Set(),
      Set(PropertyAccess(v"n", "prop"), PropertyAccess(v"m", "foo"), PropertyAccess(v"m", "bar"))
    )
  }

  test("should find property accesses from same pattern") {
    val plannerQuery = buildSinglePlannerQuery("MATCH (n)-[r {foo: n.prop}]-() RETURN r.bar")
    val result = context.withModifiedPlannerState(_
      .withAccessedProperties(findLocalPropertyAccesses(plannerQuery)))

    assertContextUpdated(
      result,
      Set(),
      Set(PropertyAccess(v"n", "prop"), PropertyAccess(v"r", "foo"), PropertyAccess(v"r", "bar"))
    )
  }

  test("does not find property accesses from beyond horizon") {
    val plannerQuery = buildSinglePlannerQuery("MATCH (n) WITH 1 AS a, n MATCH (x) WHERE x.prop = n.prop RETURN n.prop")
    val result = context.withModifiedPlannerState(_
      .withAccessedProperties(findLocalPropertyAccesses(plannerQuery)))

    assertContextNotUpdated(result)
  }

  private def assertContextNotUpdated(newContext: LogicalPlanningContext): Unit = {
    newContext.plannerState.indexCompatiblePredicatesProviderContext.aggregatingProperties should be(empty)
    newContext should equal(context)
  }

  private def assertContextUpdated(
    newContext: LogicalPlanningContext,
    expectedAggregatingProperties: Set[PropertyAccess],
    expectedAccessedProperties: Set[PropertyAccess] = Set()
  ): Unit = {
    newContext.plannerState.indexCompatiblePredicatesProviderContext.aggregatingProperties should equal(
      expectedAggregatingProperties
    )
    newContext.plannerState.accessedProperties should equal(expectedAccessedProperties)
    newContext should not equal context
  }
}
