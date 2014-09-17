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
package org.neo4j.cypher.internal.compiler.v2_2.pipes

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions.{Identifier, Literal}
import org.neo4j.cypher.internal.compiler.v2_2.commands.values.UnresolvedLabel
import org.neo4j.cypher.internal.compiler.v2_2.commands.{HasLabel, ReturnItem, True}
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_2.mutation.{CreateNode, CreateRelationship, MergeNodeAction, RelationshipEndpoint}
import org.neo4j.cypher.internal.compiler.v2_2.pipes.matching.{PatternGraph, Trail, TraversalMatcher}
import org.neo4j.cypher.internal.compiler.v2_2.symbols._
import org.neo4j.graphdb.Node
import org.scalatest.prop.TableDrivenPropertyChecks

class PipeEffectsTest extends CypherFunSuite with TableDrivenPropertyChecks {

  implicit val monitor = mock[PipeMonitor]

  val EFFECTS: Map[Pipe, Effects] = Map(
    NullPipe()
    -> Effects.NONE,

    ExecuteUpdateCommandsPipe(NullPipe(), Seq(CreateNode("n", Map.empty, Seq.empty)))
    -> Effects.WRITES_NODES,

    ExecuteUpdateCommandsPipe(NullPipe(), Seq(CreateRelationship("r", RelationshipEndpoint("a"), RelationshipEndpoint("b"), "TYPE", Map.empty)))
    -> Effects.WRITES_RELATIONSHIPS,

    ExecuteUpdateCommandsPipe(NullPipe(), Seq(
      CreateNode("n", Map.empty, Seq.empty),
      CreateRelationship("r", RelationshipEndpoint("a"), RelationshipEndpoint("b"), "TYPE", Map.empty)))
    -> Effects.WRITES_ENTITIES,

    ExecuteUpdateCommandsPipe(NullPipe(), Seq(MergeNodeAction("n", Map.empty, Seq.empty, Seq.empty, Seq.empty, Seq.empty, None)))
      -> (Effects.READS_NODES | Effects.WRITES_NODES),

    NodeStartPipe(NullPipe(), "n", mock[EntityProducer[Node]])
      -> Effects.READS_NODES,

    LoadCSVPipe(NullPipe(), null, Literal("apa"), "line", None)
      -> Effects.NONE,

    EmptyResultPipe(NullPipe())
      -> Effects.NONE,

    FilterPipe(NullPipe(), True())
      -> Effects.NONE,

    FilterPipe(NullPipe(), HasLabel(Identifier("a"), UnresolvedLabel("Apa")))
      -> Effects.READS_NODES,

    ColumnFilterPipe(NullPipe(), Seq(ReturnItem(Literal(42), "a")))
      -> Effects.NONE,

    TraversalMatchPipe(NullPipe(), mock[TraversalMatcher], mock[Trail])
      -> Effects.READS_ENTITIES,

    SlicePipe(NullPipe(), Some(Literal(10)), None)
      -> Effects.NONE,

    MatchPipe(
      new FakePipe(Iterator(Map("x" -> null)), "x" -> CTNode), Seq.empty,
      new PatternGraph(Map.empty, Map.empty, Seq.empty, Seq.empty),
      Set("x", "r", "z")
    ) -> Effects.READS_ENTITIES,

    EmptyResultPipe(NullPipe())
      -> Effects.NONE,

    EagerPipe(NodeStartPipe(NullPipe(), "n", mock[EntityProducer[Node]]))
      -> Effects.NONE,

    DistinctPipe(NodeStartPipe(NullPipe(), "n", mock[EntityProducer[Node]]), Map.empty)
      -> Effects.READS_NODES,

    DistinctPipe(NullPipe(), Map.empty)
      -> Effects.NONE,

    OptionalMatchPipe(NullPipe(), NodeStartPipe(NullPipe(), "n", mock[EntityProducer[Node]]), SymbolTable())
      -> Effects.READS_NODES
  )

  EFFECTS.foreach { case (pipe: Pipe, effects: Effects) =>
    test(pipe.getClass.getSimpleName + " " + effects) {
      pipe.effects should equal(effects)
    }
  }
}
