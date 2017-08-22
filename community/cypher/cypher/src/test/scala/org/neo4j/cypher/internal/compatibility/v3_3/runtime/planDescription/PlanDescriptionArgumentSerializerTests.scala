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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.PlanDescriptionArgumentSerializer.serialize
import org.neo4j.cypher.internal.compiler.v3_3.ast.NestedPlanExpression
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.{Argument => LPArgument}
import org.neo4j.cypher.internal.frontend.v3_3.ast.DummyExpression
import org.neo4j.cypher.internal.frontend.v3_3.ast.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.frontend.v3_3.symbols.CTBoolean
import org.neo4j.cypher.internal.frontend.v3_3.symbols.CTList
import org.neo4j.cypher.internal.frontend.v3_3.symbols.CTNode
import org.neo4j.cypher.internal.frontend.v3_3.symbols.CTString
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_3.DummyPosition
import org.neo4j.cypher.internal.frontend.v3_3.SemanticDirection
import org.neo4j.cypher.internal.ir.v3_3.Cardinality
import org.neo4j.cypher.internal.ir.v3_3.CardinalityEstimation
import org.neo4j.cypher.internal.ir.v3_3.PlannerQuery

class PlanDescriptionArgumentSerializerTests extends CypherFunSuite {
  val solved = CardinalityEstimation.lift(PlannerQuery.empty, Cardinality(1))
  private val pos = DummyPosition(0)

  test("serialization should leave numeric arguments as numbers") {
    serialize(DbHits(12)) shouldBe a[java.lang.Number]
    serialize(Rows(12)) shouldBe a[java.lang.Number]
    serialize(EstimatedRows(12)) shouldBe a[java.lang.Number]
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
    val argument: LogicalPlan = LPArgument(Set.empty)(solved)(Map.empty)
    val expression = DummyExpression(CTList(CTNode) | CTBoolean | CTList(CTString), DummyPosition(5))

    val nested = NestedPlanExpression(argument, expression)(pos)

    serialize(Expression(nested)) should equal("NestedPlanExpression(Argument)")
  }

  test("projection should show multiple expressions") {
    serialize(Expressions(
      Map("1" -> SignedDecimalIntegerLiteral("42")(pos), "2" -> SignedDecimalIntegerLiteral("56")(pos)))) should equal(
      "{1 : 42, 2 : 56}")
  }
}
