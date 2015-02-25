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
import org.neo4j.cypher.internal.compiler.v2_2.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{SimplePatternLength, PatternRelationship, IdName}
import org.neo4j.graphdb.Direction

class SolvablesTest extends CypherFunSuite {

  val node1Name = IdName("a")
  val node2Name = IdName("b")

  val relName = IdName("rel")
  val rel = PatternRelationship(relName, (node1Name, node2Name), Direction.OUTGOING, Seq.empty, SimplePatternLength)

  test("should compute solvables from empty query graph") {
    val qg = QueryGraph.empty

    Solvables(qg) should equal(Set.empty)
  }

  test("should compute solvables from query graph with pattern relationships") {
    val qg = QueryGraph.empty.addPatternNodes(node1Name, node2Name).addPatternRelationship(rel)

    Solvables(qg) should equal(Set(SolvableRelationship(rel)))
  }
}
