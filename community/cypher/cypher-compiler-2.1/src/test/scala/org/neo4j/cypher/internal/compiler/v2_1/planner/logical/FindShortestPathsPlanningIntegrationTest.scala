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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps.QueryPlanProducer
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{SimplePatternLength, PatternRelationship, ShortestPathPattern}
import org.neo4j.graphdb.Direction

class FindShortestPathsPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  import QueryPlanProducer._

  test("finds shortest paths. duh.") {
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
          )
        ),
        Map("b" -> ident("b"))
      )
    )
  }
}
