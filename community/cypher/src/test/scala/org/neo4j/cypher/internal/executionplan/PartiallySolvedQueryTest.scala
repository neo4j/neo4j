/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.executionplan

import builders.{QueryToken, Solved}
import org.junit.Test
import org.scalatest.Assertions
import org.neo4j.cypher.internal.pipes.matching.{PatternRelationship, PatternGraph, PatternNode}
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commands.{Pattern, RelatedTo}

class PartiallySolvedQueryTest extends Assertions {

  @Test
  def shouldInferConnectedPatternGraph() {
    // Given
    val psq = createQuery( Seq(
      new Solved(new RelatedTo("A", "B", "R", Seq("KNOWS"), Direction.OUTGOING, false))
    ))

    // When
//    val graph = psq.matchPattern

    // Then
    val a: PatternNode = new PatternNode("A")
    val b: PatternNode = new PatternNode("B")

//    assert( graph === new PatternGraph(
//      Map("A" -> a, "B" -> b),
//      Map("R" -> a.relateTo("R", b, Seq("KNOWS"), Direction.OUTGOING, false)),
//      Seq("A", "B")
//    ))
  }

  def createQuery(pattern: Seq[Solved[Pattern]]): PartiallySolvedQuery = {
    new PartiallySolvedQuery(
      returns = Seq(),
      start = Seq(),
      updates = Seq(),
      patterns = pattern,
      where = Seq(),
      aggregation = Seq(),
      sort = Seq(),
      slice = None,
      namedPaths = Seq(),
      aggregateQuery = Solved(false),
      extracted = false,
      tail = None
    )
  }
}