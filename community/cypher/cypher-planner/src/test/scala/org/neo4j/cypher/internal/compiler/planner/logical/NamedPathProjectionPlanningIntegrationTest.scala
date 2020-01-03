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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.logical.plans.{Expand, NodeByLabelScan, Projection, Selection}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v4_0.expressions.{NodePathStep, PathExpression, SemanticDirection, SingleRelationshipPathStep, NilPathStep}

class NamedPathProjectionPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should build plans containing outgoing path projections") {
    planFor("MATCH p = (a:X)-[r]->(b) RETURN p")._2 should equal(
      Projection(
        Expand( NodeByLabelScan("a",  labelName("X"), Set.empty), "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r"),
        projectExpressions = Map(
          "p" -> PathExpression(NodePathStep(varFor("a"),SingleRelationshipPathStep(varFor("r"), SemanticDirection.OUTGOING, Some(varFor("b")), NilPathStep)))_
        )
      )
    )
  }

  test("should build plans containing path projections and path selections") {
    val pathExpr = PathExpression(NodePathStep(varFor("a"),SingleRelationshipPathStep(varFor("r"), SemanticDirection.OUTGOING, Some(varFor("b")), NilPathStep)))_

    val result = planFor("MATCH p = (a:X)-[r]->(b) WHERE head(nodes(p)) = a RETURN b")._2

    result should equal(
      Selection(
        ands(equals(
          function("head", function("nodes", pathExpr)),
          varFor("a")
        )),
          Expand(NodeByLabelScan("a", labelName("X"), Set.empty), "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r")
      )
    )
  }

  test("should build plans containing multiple path projections and path selections") {
    val pathExpr = PathExpression(NodePathStep(varFor("a"),SingleRelationshipPathStep(varFor("r"), SemanticDirection.OUTGOING, Some(varFor("b")), NilPathStep)))_

    val result = planFor("MATCH p = (a:X)-[r]->(b) WHERE head(nodes(p)) = a AND length(p) > 10 RETURN b")._2

    result should equal(
      Selection(
        ands(
          equals(
            function("head", function("nodes", pathExpr)),
            varFor("a")
          ),
          greaterThan(
            function("length", pathExpr),
            literalInt(10)
          )
        ),
          Expand(NodeByLabelScan("a", labelName("X"), Set.empty), "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r")
      )
    )
  }
}
