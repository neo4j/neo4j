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
package org.neo4j.cypher.internal.plandescription

import org.neo4j.cypher.internal.ir.ProvidedOrder
import org.neo4j.cypher.internal.plandescription.Arguments.{Expression => argExpression, _}
import org.neo4j.cypher.internal.plandescription.PlanDescriptionArgumentSerializer.serialize
import org.neo4j.cypher.internal.v4_0.util.{DummyPosition, InputPosition}
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.{LogicalPlan, NestedPlanExpression}
import org.neo4j.cypher.internal.v4_0.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.v4_0.util.symbols.{CTBoolean, CTList, CTNode, CTString}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class PlanDescriptionArgumentSerializerTests extends CypherFunSuite {
  implicit val idGen = new SequentialIdGen()

  test("serialization should leave numeric arguments as numbers") {
    serialize(DbHits(12)) shouldBe a [java.lang.Number]
    serialize(Rows(12)) shouldBe a [java.lang.Number]
    serialize(EstimatedRows(12)) shouldBe a [java.lang.Number]
  }

  test("ExpandExpression should look like Cypher syntax") {
    serialize(ExpandExpression("a", "r", Seq("LIKES", "LOVES"), "b", SemanticDirection.OUTGOING, 1, Some(1))) should
      equal("(a)-[r:LIKES|LOVES]->(b)")
    serialize(ExpandExpression("a", "r", Seq("LIKES", "LOVES"), "b", SemanticDirection.OUTGOING, 1, Some(5))) should
      equal("(a)-[r:LIKES|LOVES*..5]->(b)")
    serialize(ExpandExpression("a", "r", Seq("LIKES", "LOVES"), "b", SemanticDirection.OUTGOING, 1, None)) should
      equal("(a)-[r:LIKES|LOVES*]->(b)")
    serialize(ExpandExpression("a", "r", Seq("LIKES", "LOVES"), "b", SemanticDirection.OUTGOING, 3, Some(5))) should
      equal("(a)-[r:LIKES|LOVES*3..5]->(b)")
    serialize(ExpandExpression("a", "r", Seq("LIKES", "LOVES"), "b", SemanticDirection.OUTGOING, 3, None)) should
      equal("(a)-[r:LIKES|LOVES*3..]->(b)")
  }

  test("serialize nested plan expression") {
    val argument: LogicalPlan = plans.Argument(Set.empty)
    val expression = DummyExpression(CTList(CTNode) | CTBoolean | CTList(CTString), DummyPosition(5))

    val nested = NestedPlanExpression(argument, expression)(pos)

    serialize(argExpression(nested)) should equal("NestedPlanExpression(Argument)")
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

  test("serialize and deduplicate variable names with regexy symbols") {
    serialize(KeyNames(Seq("1 >=   version$@40, 2 <=   version$@352"))) should equal (
      "1 >= version$, 2 <= version$"
    )
    serialize(KeyNames(Seq("1 >=   version\\@40, 2 <=   version\\@352"))) should equal (
      "1 >= version\\, 2 <= version\\"
    )
  }

  test("should serialize point distance index seeks") {
    serialize(PointDistanceIndex("L", "location", "p", "300", inclusive = false, Seq.empty)) should equal(":L(location) WHERE distance(_,p) < 300")
    serialize(PointDistanceIndex("L", "location", "p", "300", inclusive = true, Seq(CachedProperty("p", Variable("p")(pos), PropertyKeyName("location")(pos), NODE_TYPE)(pos)))) should equal(":L(location) WHERE distance(_,p) <= 300, cache[p.location]")
  }

  test("should serialize provided order") {
    serialize(Order(ProvidedOrder(List(ProvidedOrder.Asc(varFor("a")), ProvidedOrder.Desc(varFor("b")), ProvidedOrder.Asc(prop("c","foo")))))) should be("a ASC, b DESC, c.foo ASC")
    serialize(Order(ProvidedOrder.empty)) should be("")
    serialize(Order(ProvidedOrder(List(ProvidedOrder.Asc(varFor("  FRESHID42")))))) should be("anon[42] ASC")
  }

  private val pos: InputPosition = DummyPosition(0)
  private def varFor(name: String): Variable = Variable(name)(pos)
  private def prop(varName: String, propName: String): Property = Property(varFor(varName), PropertyKeyName(propName)(pos))(pos)
}
