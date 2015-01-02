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
package org.neo4j.cypher.internal.compiler.v2_0.pipes

import org.neo4j.cypher.internal.compiler.v2_0._
import pipes.matching.PatternGraph
import symbols._
import org.junit.Test
import org.scalatest.Assertions

class MatchPipeTest extends Assertions {
  @Test
  def should_yield_nothing_if_it_gets_an_incoming_null() {
    val source = new FakePipe(Iterator(Map("x"->null)), "x"->CTNode)
    val patternGraph = new PatternGraph(Map.empty, Map.empty, Seq.empty, Seq.empty)
    val identifiersInClause = Set("x", "r", "z")
    val matchPipe = new MatchPipe(source, predicates = Seq.empty, patternGraph, identifiersInClause)
    val result: Iterator[ExecutionContext] = matchPipe.createResults(QueryStateHelper.empty)
    assert(result.toList === List.empty)
  }
}
