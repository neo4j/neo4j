/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.pipes.matching

import org.neo4j.cypher.GraphDatabaseTestBase
import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.symbols.{Identifier, NodeType, SymbolTable}
import org.neo4j.cypher.internal.commands.True


class JoinerBuilderTest extends GraphDatabaseTestBase with Assertions {
  @Test def simplestCase() {
    val pA = new PatternNode("a")
    val pB = new PatternNode("b")
    val pR = pA.relateTo("r", pB, Seq(), Direction.BOTH, false, True())
    val symbols = new SymbolTable(Identifier("a", NodeType()))

    val nodes = Map("a" -> pA, "b" -> pB)
    val rels = Map("r" -> pR)

    val patternGraph = new PatternGraph(nodes, rels, symbols)

    val builder = new JoinerBuilder(patternGraph, Seq(True()))

    val a = createNode()
    val b = createNode()
    val r = relate(a, b)

    assert(builder.getMatches(Map("a" -> a)) === List(Map("a" -> a, "b" -> b, "r" -> r)))
  }
}