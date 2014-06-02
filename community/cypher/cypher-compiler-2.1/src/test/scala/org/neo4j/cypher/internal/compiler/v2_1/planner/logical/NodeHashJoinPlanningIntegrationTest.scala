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

import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.ast.NotEquals
import org.neo4j.cypher.internal.compiler.v2_1.ast.Identifier

class NodeHashJoinPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should build plans containing joins") {

    (new given {
      cardinality = {
        case _: AllNodesScan                      => 200
        case Expand(_, IdName("b"), _, _, _, _,_) => 10000
        case _: Expand                            => 10
        case _: NodeHashJoin                      => 20
        case _                                    => Double.MaxValue
      }
    } planFor "MATCH (a)<-[r1]-(b)-[r2]->(c) RETURN b").plan should equal(
      Projection(
        Selection(
          Seq(NotEquals(Identifier("r1")_,Identifier("r2")_)_),
          NodeHashJoin("b",
            Expand(AllNodesScan("a"), "a", Direction.INCOMING, Seq(), "b", "r1", SimplePatternLength),
            Expand(AllNodesScan("c"), "c", Direction.INCOMING, Seq(), "b", "r2", SimplePatternLength)
          )
        ),
        expressions = Map("b" -> Identifier("b")_)
      )
    )
  }
}
