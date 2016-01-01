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

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.ast.{Identifier, NotEquals}
import org.neo4j.cypher.internal.compiler.v2_1.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.graphdb.Direction

class FindShortestPathsPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps.QueryPlanProducer._

  test("finds shortest paths") {
    planFor("MATCH a, b, shortestPath(a-[r]->b) RETURN b").plan should equal(
      planRegularProjection(
        planShortestPaths(
          planCartesianProduct(
            planAllNodesScan("a"),
            planAllNodesScan("b")
          ),
          ShortestPathPattern(
            None,
            PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
            single = true
          )(null)
        ),
        Map("b" -> ident("b"))
      )
    )
  }

  test("finds all shortest paths") {
    planFor("MATCH a, b, allShortestPaths(a-[r]->b) RETURN b").plan should equal(
      planRegularProjection(
        planShortestPaths(
          planCartesianProduct(
            planAllNodesScan("a"),
            planAllNodesScan("b")
          ),
          ShortestPathPattern(
            None,
            PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
            single = false
          )(null)
        ),
        Map("b" -> ident("b"))
      )
    )
  }

  test("find shortest paths on top of hash joins") {
    val r1 = PatternRelationship("r1", ("a", "b"), Direction.INCOMING, Seq(), SimplePatternLength)
    val r2 = PatternRelationship("r2", ("b", "c"), Direction.OUTGOING, Seq(), SimplePatternLength)

    (new given {
      cardinality = mapCardinality {
        case _: AllNodesScan => 200
        case Expand(_, IdName("b"), _, _, _, _, _) => 10000
        case _: Expand => 10
        case _: NodeHashJoin => 20
        case _ => Double.MaxValue
      }
    } planFor "MATCH (a)<-[r1]-(b)-[r2]->(c), p = shortestPath((a)-[r]->(c)) RETURN p").plan should equal(
      planRegularProjection(
        planShortestPaths(
          planSelection(
            Vector(NotEquals(Identifier("r1") _, Identifier("r2") _) _),
            planNodeHashJoin("b",
              planExpand(planAllNodesScan("a"), "a", Direction.INCOMING, Seq(), "b", "r1", SimplePatternLength, r1),
              planExpand(planAllNodesScan("c"), "c", Direction.INCOMING, Seq(), "b", "r2", SimplePatternLength, r2)
            )
          ),
          ShortestPathPattern(
            Some("p"),
            PatternRelationship("r", ("a", "c"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
            single = true
          )(null)
        ),
        expressions = Map("p" -> Identifier("p") _)
      )
    )
  }
}
