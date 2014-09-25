/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import org.neo4j.cypher.internal.compiler.v2_2.ast.{Identifier, NotEquals}
import org.neo4j.cypher.internal.compiler.v2_2.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.graphdb.Direction

class FindShortestPathsPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._

  test("finds shortest paths") {
    planFor("MATCH a, b, shortestPath(a-[r]->b) RETURN b").plan should equal(
      planRegularProjection(
        planShortestPaths(
          planCartesianProduct(
            planAllNodesScan("b", Set.empty),
            planAllNodesScan("a", Set.empty)
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
            planAllNodesScan("b", Set.empty),
            planAllNodesScan("a", Set.empty)
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
    def myCardinality(plan: LogicalPlan): Cardinality = Cardinality(plan match {
      case _: NodeIndexSeek                    => 10.0
      case _: AllNodesScan                     => 10000
      case _: NodeHashJoin                     => 42
      case Expand(lhs, _, _, _, _, _, _, _, _) => (myCardinality(lhs) * Multiplier(10)).amount
      case _: Selection                        => 100.04
      case _: NodeByLabelScan                  => 100
      case _                                   => Double.MaxValue
    })

    (new given {
      cardinality = PartialFunction(myCardinality)
    } planFor "MATCH (a:X)<-[r1]-(b)-[r2]->(c:X), p = shortestPath((a)-[r]->(c)) RETURN p").plan.toString should equal(
      """Projection[p](Map("p" → p))
        |↳ FindShortestPaths[p,r2,r1,a,b,c,r](p = shortestPath((a)-[r]->(c)))
        |↳ Selection[r2,r1,a,b,c](ArrayBuffer(NotEquals(r1, r2)))
        |↳ NodeHashJoin[r2,r1,a,b,c](Set(b))
        |  ↳ left =
        |    Expand[a,r1,b](a, INCOMING, INCOMING, ⬨, b, r1, , ArrayBuffer())
        |    ↳ NodeByLabelScan[a](a, Left("X"), Set())
        |  ↳ right =
        |    Expand[c,r2,b](c, INCOMING, OUTGOING, ⬨, b, r2, , ArrayBuffer())
        |    ↳ NodeByLabelScan[c](c, Left("X"), Set())""".stripMargin
    )
  }
}
