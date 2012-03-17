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
package org.neo4j.cypher.internal.executionplan.builders

import org.neo4j.cypher.internal.pipes.FakePipe
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.executionplan.{PartiallySolvedQuery, Solved, Unsolved}
import org.junit.Test
import org.junit.Assert._
import org.neo4j.cypher.internal.commands.{NamedPath, NodeById, RelatedTo, True}
import org.neo4j.cypher.internal.symbols.{RelationshipType, SymbolTable, Identifier, NodeType}
import collection.mutable.Map

class NamedPathBuilderTest extends PipeBuilder {
  val builder = new NamedPathBuilder

  @Test
  def should_not_accept_if_pattern_is_not_yet_solved() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeById("l", 0))),
      patterns = Seq(Unsolved(RelatedTo("l", "r", "rel", None, Direction.OUTGOING, false, True()))),
      namedPaths = Seq(Unsolved(NamedPath("p", RelatedTo("l", "r", "rel", None, Direction.OUTGOING, false, True()))))
    )

    val p = createPipe(nodes = Seq("l"))

    assertFalse("Builder should not accept this", builder.isDefinedAt(p, q))
  }

  @Test
  def should_accept_if_pattern_is_solved() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeById("l", 0))),
      patterns = Seq(Solved(RelatedTo("l", "r", "rel", None, Direction.OUTGOING, false, True()))),
      namedPaths = Seq(Unsolved(NamedPath("p", RelatedTo("l", "r", "rel", None, Direction.OUTGOING, false, True()))))
    )

    val p = createPipe(nodes = Seq("l", "r"), relationships = Seq("rel"))

    assertTrue("Builder should not accept this", builder.isDefinedAt(p, q))
    
    val (_, resultQ) = builder(p,q)
    
    assert(resultQ.namedPaths == Seq(Solved(NamedPath("p", RelatedTo("l", "r", "rel", None, Direction.OUTGOING, false, True())))))
  }
  
  @Test
  def should_not_accept_unless_all_parts_of_the_named_path_are_solved() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeById("l", 0))),
      patterns = Seq(
        Solved(RelatedTo("l", "r", "rel", None, Direction.OUTGOING, false, True())),
        Unsolved(RelatedTo("r", "x", "rel2", None, Direction.OUTGOING, false, True()))
      ),
      namedPaths = Seq(Unsolved(NamedPath("p",
        RelatedTo("l", "r", "rel", None, Direction.OUTGOING, false, True()),
        RelatedTo("r", "x", "rel2", None, Direction.OUTGOING, false, True()))))
    )

    val p = createPipe(nodes = Seq("l", "r"), relationships = Seq("rel"))

    assertFalse("Builder should not accept this", builder.isDefinedAt(p, q))
  }
}

trait PipeBuilder {
  def createPipe(nodes: Seq[String] = Seq(), relationships: Seq[String] = Seq()) = {
    val nodeIdentifiers = nodes.map(x => Identifier(x, NodeType()))
    val relIdentifiers = relationships.map(x => Identifier(x, RelationshipType()))
    new FakePipe(Seq(Map()), new SymbolTable(nodeIdentifiers ++ relIdentifiers: _*))
  }
}