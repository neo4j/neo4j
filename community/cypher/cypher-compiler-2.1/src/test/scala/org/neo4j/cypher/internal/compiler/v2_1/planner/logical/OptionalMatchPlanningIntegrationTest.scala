/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.ast.RelTypeName

class OptionalMatchPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should build plans containing joins") {
    (new given {
      cardinality = mapCardinality {
        case _: AllNodesScan => 2000000
        case _: NodeByLabelScan => 20
        case _: Expand => 10
        case _: OuterHashJoin => 20
        case _: SingleRow => 1
        case _ => Double.MaxValue
      }
    } planFor "MATCH (a:X)-[r1]->(b) OPTIONAL MATCH (b)-[r2]->(c:Y) RETURN b").plan.plan should equal(
      Projection(
        OuterHashJoin("b",
          Expand(NodeByLabelScan("a", Left("X")), "a", Direction.OUTGOING, Seq(), "b", "r1", SimplePatternLength),
          Expand(NodeByLabelScan("c", Left("Y")), "c", Direction.INCOMING, Seq(), "b", "r2", SimplePatternLength)
        ),
        expressions = Map("b" -> ident("b"))
      )
    )
  }

  test("should build simple optional match plans") {
    planFor("OPTIONAL MATCH a RETURN a").plan.plan should equal(
      Optional(AllNodesScan("a"))
    )
  }

  test("should solve multiple optional matches") {
    planFor("MATCH a OPTIONAL MATCH (a)-[:R1]->(x1) OPTIONAL MATCH (a)-[:R2]->(x2) RETURN a, x1, x2").plan.plan should equal(
      Projection(
        OptionalExpand(
          OptionalExpand(
            AllNodesScan(IdName("a")),
            IdName("a"), Direction.OUTGOING, List(RelTypeName("R1") _), IdName("x1"), IdName("  UNNAMED26"), SimplePatternLength, Seq.empty),
          IdName("a"), Direction.OUTGOING, List(RelTypeName("R2") _), IdName("x2"), IdName("  UNNAMED57"), SimplePatternLength, Seq.empty),
        Map("a" -> ident("a"), "x1" -> ident("x1"), "x2" -> ident("x2")
        )
      )
    )
  }
}
