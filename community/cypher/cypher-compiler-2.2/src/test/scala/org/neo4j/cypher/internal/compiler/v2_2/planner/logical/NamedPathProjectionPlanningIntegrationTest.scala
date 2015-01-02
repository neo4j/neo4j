/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.pipes.LazyLabel
import org.neo4j.cypher.internal.compiler.v2_2.planner.{PlannerQuery, LogicalPlanningTestSupport2}
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._

class NamedPathProjectionPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should build plans containing outgoing path projections") {
    planFor("MATCH p = (a:X)-[r]->(b) RETURN p").plan should equal(
      Projection(
        Expand( NodeByLabelScan("a",  LazyLabel("X"), Set.empty)(PlannerQuery.empty), "a", Direction.OUTGOING, Seq.empty, "b", "r")(PlannerQuery.empty),
        expressions = Map(
          "p" -> PathExpression(NodePathStep(Identifier("a")_,SingleRelationshipPathStep(Identifier("r")_, Direction.OUTGOING, NilPathStep)))_
        )
      )(PlannerQuery.empty)
    )
  }

  test("should build plans containing path projections and path selections") {
    val pathExpr = PathExpression(NodePathStep(Identifier("a")_,SingleRelationshipPathStep(Identifier("r")_, Direction.OUTGOING, NilPathStep)))_

    planFor("MATCH p = (a:X)-[r]->(b) WHERE head(nodes(p)) = a RETURN b").plan should equal(
      Projection(
        Selection(
          Seq(Equals(
            FunctionInvocation(FunctionName("head")_, FunctionInvocation(FunctionName("nodes")_, pathExpr)_)_,
            Identifier("a")_
          )_),
          Expand( NodeByLabelScan("a",  LazyLabel("X"), Set.empty)(PlannerQuery.empty), "a", Direction.OUTGOING,  Seq.empty, "b", "r")(PlannerQuery.empty)
        )(PlannerQuery.empty),
        expressions = Map("b" -> Identifier("b") _)
      )(PlannerQuery.empty)
    )
  }
}
