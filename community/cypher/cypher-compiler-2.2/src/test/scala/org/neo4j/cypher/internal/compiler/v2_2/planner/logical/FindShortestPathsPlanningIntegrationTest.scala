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
import org.neo4j.cypher.internal.compiler.v2_2.ast.{Identifier, NotEquals}
import org.neo4j.cypher.internal.compiler.v2_2.pipes.LazyLabel
import org.neo4j.cypher.internal.compiler.v2_2.planner.{PlannerQuery, LogicalPlanningTestSupport2}
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
      case _: NodeIndexSeek              => 10.0
      case _: AllNodesScan               => 10000
      case _: NodeHashJoin               => 42
      case Expand(lhs, _, _, _, _, _, _) => (myCardinality(lhs) * Multiplier(10)).amount
      case _: Selection                  => 100.04
      case _: NodeByLabelScan            => 100
      case _                             => Double.MaxValue
    })

    val result = (new given {
      cardinality = PartialFunction(myCardinality)
    } planFor "MATCH (a:X)<-[r1]-(b)-[r2]->(c:X), p = shortestPath((a)-[r]->(c)) RETURN p").plan

    val expected =
      Projection(
        FindShortestPaths(
        Selection(
          Seq(NotEquals(Identifier("r1") _, Identifier("r2") _) _),
          NodeHashJoin(
            Set(IdName("b")),
            Expand(
              NodeByLabelScan(IdName("a"), LazyLabel("X"), Set.empty)(PlannerQuery.empty),
              IdName("a"), Direction.INCOMING, Seq.empty, IdName("b"), IdName("r1"), ExpandAll)(PlannerQuery.empty),
            Expand(
              NodeByLabelScan(IdName("c"), LazyLabel("X"), Set.empty)(PlannerQuery.empty),
              IdName("c"), Direction.INCOMING, Seq.empty, IdName("b"), IdName("r2"), ExpandAll)(PlannerQuery.empty)
          )(PlannerQuery.empty)
        )(PlannerQuery.empty),
          ShortestPathPattern(Some(IdName("p")), PatternRelationship("r", ("a", "c"), Direction.OUTGOING, Seq.empty, SimplePatternLength), single = true)(null))(PlannerQuery.empty),
        Map("p" -> Identifier("p") _))(PlannerQuery.empty)

    result should equal(expected)
  }
}
