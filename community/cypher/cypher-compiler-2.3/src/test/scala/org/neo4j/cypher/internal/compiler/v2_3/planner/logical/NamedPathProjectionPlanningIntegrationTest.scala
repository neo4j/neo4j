/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical

import org.neo4j.cypher.internal.compiler.v2_3.pipes.LazyLabel
import org.neo4j.cypher.internal.compiler.v2_3.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class NamedPathProjectionPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should build plans containing outgoing path projections") {
    planFor("MATCH p = (a:X)-[r]->(b) RETURN p").plan should equal(
      Projection(
        Expand( NodeByLabelScan("a",  LazyLabel("X"), Set.empty)(solved), "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r")(solved),
        expressions = Map(
          "p" -> PathExpression(NodePathStep(Identifier("a")_,SingleRelationshipPathStep(Identifier("r")_, SemanticDirection.OUTGOING, NilPathStep)))_
        )
      )(solved)
    )
  }

  test("should build plans containing path projections and path selections") {
    val pathExpr = PathExpression(NodePathStep(Identifier("a")_,SingleRelationshipPathStep(Identifier("r")_, SemanticDirection.OUTGOING, NilPathStep)))_

    val result = planFor("MATCH p = (a:X)-[r]->(b) WHERE head(nodes(p)) = a RETURN b").plan

    result should equal(
      Selection(
        Seq(Equals(
          FunctionInvocation(FunctionName("head") _, FunctionInvocation(FunctionName("nodes") _, ident("p")) _) _,
          ident("a")
        ) _),
        Projection(
          Expand(NodeByLabelScan("a", LazyLabel("X"), Set.empty)(solved), "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r")(solved),
          expressions = Map("a" -> ident("a"), "b" -> ident("b"), "p" -> pathExpr, "r" -> ident("r")))(solved)
      )(solved)
    )
  }

  test("should build plans containing multiple path projections and path selections") {
    val pathExpr = PathExpression(NodePathStep(Identifier("a")_,SingleRelationshipPathStep(Identifier("r")_, SemanticDirection.OUTGOING, NilPathStep)))_

    val result = planFor("MATCH p = (a:X)-[r]->(b) WHERE head(nodes(p)) = a AND length(p) > 10 RETURN b").plan

    result should equal(
      Selection(
        Seq(
          Equals(
            FunctionInvocation(FunctionName("head") _, FunctionInvocation(FunctionName("nodes") _, ident("p")) _) _,
            Identifier("a") _
          ) _,
          GreaterThan(
            FunctionInvocation(FunctionName("length") _, ident("p")) _,
            SignedDecimalIntegerLiteral("10") _
          ) _
        ),
        Projection(
          Expand(NodeByLabelScan("a", LazyLabel("X"), Set.empty)(solved), "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r")(solved),
          expressions = Map("a" -> ident("a"), "b" -> ident("b"), "p" -> pathExpr, "r" -> ident("r"))
        )(solved)
      )(solved)
    )
  }
}
