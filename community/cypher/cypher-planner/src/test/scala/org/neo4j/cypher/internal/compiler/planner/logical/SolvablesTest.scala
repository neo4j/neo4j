/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SolvablesTest extends CypherFunSuite {

  private val node1 = v"a"
  private val node2 = v"b"

  private val relVar = v"rel"

  private val rel =
    PatternRelationship(relVar, (node1, node2), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

  test("should compute solvables from empty query graph") {
    val qg = QueryGraph.empty

    Solvables(qg) should equal(Set.empty)
  }

  test("should compute solvables from query graph with pattern relationships") {
    val qg = QueryGraph.empty.addPatternNodes(node1.name, node2.name).addPatternRelationship(rel)

    Solvables(qg) should equal(Set(SolvableRelationship(rel)))
  }
}
