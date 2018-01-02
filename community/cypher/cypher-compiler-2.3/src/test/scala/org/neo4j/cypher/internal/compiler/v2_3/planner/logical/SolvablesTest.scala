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

import org.neo4j.cypher.internal.compiler.v2_3.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{IdName, PatternRelationship, SimplePatternLength}
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class SolvablesTest extends CypherFunSuite {

  val node1Name = IdName("a")
  val node2Name = IdName("b")

  val relName = IdName("rel")
  val rel = PatternRelationship(relName, (node1Name, node2Name), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

  test("should compute solvables from empty query graph") {
    val qg = QueryGraph.empty

    Solvables(qg) should equal(Set.empty)
  }

  test("should compute solvables from query graph with pattern relationships") {
    val qg = QueryGraph.empty.addPatternNodes(node1Name, node2Name).addPatternRelationship(rel)

    Solvables(qg) should equal(Set(SolvableRelationship(rel)))
  }
}
