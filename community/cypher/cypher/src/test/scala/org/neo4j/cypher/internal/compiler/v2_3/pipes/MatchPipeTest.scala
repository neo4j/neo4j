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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.pipes.matching.PatternGraph
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class MatchPipeTest extends CypherFunSuite {

  test("should_yield_nothing_if_it_gets_an_incoming_null") {
    implicit val monitor = mock[PipeMonitor]
    val source = new FakePipe(Iterator(Map("x"->null)), "x"->CTNode)
    val patternGraph = new PatternGraph(Map.empty, Map.empty, Seq.empty, Seq.empty)
    val identifiersInClause = Set("x", "r", "z")
    val matchPipe = new MatchPipe(source, predicates = Seq.empty, patternGraph, identifiersInClause)
    val result: Iterator[ExecutionContext] = matchPipe.createResults(QueryStateHelper.empty)
    result.toList shouldBe empty
  }
}
