/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.runtime.planDescription

import org.neo4j.cypher.internal.ir.v3_4.{CardinalityEstimation, PlannerQuery}
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.cypher.internal.runtime.planDescription.PlanDescriptionArgumentSerializer.serialize
import org.neo4j.cypher.internal.util.v3_4.symbols.{CTBoolean, CTList, CTNode, CTString}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.v3_4.{Cardinality, DummyPosition}
import org.neo4j.cypher.internal.v3_4.expressions.{DummyExpression, SemanticDirection, SignedDecimalIntegerLiteral}
import org.neo4j.cypher.internal.v3_4.logical.plans.{LogicalPlan, NestedPlanExpression, SingleRow}

class PlanDescriptionArgumentSerializerTests extends CypherFunSuite {
  val solved = CardinalityEstimation.lift(PlannerQuery.empty, Cardinality(1))
  private val pos = DummyPosition(0)

  test("serialization should leave numeric arguments as numbers") {
    serialize(DbHits(12)) shouldBe a [java.lang.Number]
    serialize(Rows(12)) shouldBe a [java.lang.Number]
    serialize(EstimatedRows(12)) shouldBe a [java.lang.Number]
  }

  test("ExpandExpression should look like Cypher syntax") {
    serialize(ExpandExpression("a", "r", Seq("LIKES", "LOVES"), "b", SemanticDirection.OUTGOING, 1, Some(1))) should
      equal("(a)-[r:LIKES|:LOVES]->(b)")
    serialize(ExpandExpression("a", "r", Seq("LIKES", "LOVES"), "b", SemanticDirection.OUTGOING, 1, Some(5))) should
      equal("(a)-[r:LIKES|:LOVES*..5]->(b)")
    serialize(ExpandExpression("a", "r", Seq("LIKES", "LOVES"), "b", SemanticDirection.OUTGOING, 1, None)) should
      equal("(a)-[r:LIKES|:LOVES*]->(b)")
    serialize(ExpandExpression("a", "r", Seq("LIKES", "LOVES"), "b", SemanticDirection.OUTGOING, 3, Some(5))) should
      equal("(a)-[r:LIKES|:LOVES*3..5]->(b)")
    serialize(ExpandExpression("a", "r", Seq("LIKES", "LOVES"), "b", SemanticDirection.OUTGOING, 3, None)) should
      equal("(a)-[r:LIKES|:LOVES*3..]->(b)")
  }

  test("serialize nested plan expression") {
    val argument: LogicalPlan = SingleRow(Set.empty)(solved)(Map.empty)
    val expression = DummyExpression(CTList(CTNode) | CTBoolean | CTList(CTString), DummyPosition(5))

    val nested = NestedPlanExpression(argument, expression)(pos)

    serialize(Expression(nested)) should equal("NestedPlanExpression(SingleRow)")
  }

  test("projection should show multiple expressions") {
    serialize(Expressions(Map("1" -> SignedDecimalIntegerLiteral("42")(pos), "2" -> SignedDecimalIntegerLiteral("56")(pos)))) should equal(
      "{1 : 42, 2 : 56}")
  }

  test("serialize something that includes a regex should work") {
    val value = "GenericCase(Vector((any(  x@40 in n.values where   x@40 =~ Literal(^T-?\\d+$)),SubstringFunction(ContainerIndex(FilterFunction(n.values,  x@106,  x@106 =~ Literal(^T-?\\d+$)),Literal(0)),Literal(1),None))),Some(Literal(1))) == {p0}"
    serialize(KeyNames(Seq(value))) should equal (
      "GenericCase(Vector((any(x in n.values where x =~ Literal(^T-?\\d+$)),SubstringFunction(ContainerIndex(FilterFunction(n.values,x,x =~ Literal(^T-?\\d+$)),Literal(0)),Literal(1),None))),Some(Literal(1))) == {p0}"
    )
  }

}
