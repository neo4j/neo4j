/*
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
package org.neo4j.cypher.internal.compiler.v2_2.executionplan.builders

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.commands.RelatedTo
import org.neo4j.cypher.internal.compiler.v2_2.symbols._
import org.neo4j.graphdb.Direction

class PatternGraphBuilderTest extends CypherFunSuite with PatternGraphBuilder {

  test("should_only_include_connected_patterns") {
    // given MATCH a-[r1]->b, c-[r2]->d
    val symbols = new SymbolTable(Map("a" -> CTNode, "b" -> CTNode, "c" -> CTNode, "d" -> CTNode))
    val r1 = RelatedTo("a", "b", "r1", "FOO", Direction.OUTGOING)
    val r2 = RelatedTo("c", "d", "r2", "FOO", Direction.OUTGOING)

    // when
    val graph = buildPatternGraph(symbols, Seq(r1, r2))

    // then
    graph.patternNodes.keys.toSet should equal(Set("a", "b"))
  }

  test("should_include_connected_patterns") {
    // given MATCH a-[r1]->b-[r2]->c-[r2]->d
    val symbols = new SymbolTable(Map("a" -> CTNode))
    val r1 = RelatedTo("a", "b", "r1", "FOO", Direction.OUTGOING)
    val r2 = RelatedTo("b", "c", "r2", "FOO", Direction.OUTGOING)
    val r3 = RelatedTo("c", "d", "r3", "FOO", Direction.OUTGOING)

    // when
    val graph = buildPatternGraph(symbols, Seq(r1, r2, r3))

    // then
    graph.patternNodes.keys.toSet should equal(Set("a", "b", "c", "d"))
  }
}
