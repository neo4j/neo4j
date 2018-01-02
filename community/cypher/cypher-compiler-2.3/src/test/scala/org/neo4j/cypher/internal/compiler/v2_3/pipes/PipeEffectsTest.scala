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

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_3.commands.ReturnItem
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Identifier, Literal}
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.{HasLabel, True}
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.UnresolvedLabel
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{Effects, _}
import org.neo4j.cypher.internal.compiler.v2_3.mutation.{CreateNode, CreateRelationship, MergeNodeAction, RelationshipEndpoint}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.matching.{PatternGraph, Trail, TraversalMatcher}
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Node
import org.scalatest.prop.TableDrivenPropertyChecks

class PipeEffectsTest extends CypherFunSuite with TableDrivenPropertyChecks {

  implicit val monitor = mock[PipeMonitor]

  val EFFECTS: Map[Pipe, Effects] = Map[Pipe, Effects](
    SingleRowPipe()
    -> Effects(),

    ExecuteUpdateCommandsPipe(SingleRowPipe(), Seq(CreateNode("n", Map.empty, Seq.empty)))
    -> Effects(WritesAnyNode),

    ExecuteUpdateCommandsPipe(SingleRowPipe(), Seq(CreateRelationship("r", RelationshipEndpoint("a"), RelationshipEndpoint("b"), "TYPE", Map.empty)))
    -> Effects(WritesRelationships),

    ExecuteUpdateCommandsPipe(SingleRowPipe(), Seq(
      CreateNode("n", Map.empty, Seq.empty),
      CreateRelationship("r", RelationshipEndpoint("a"), RelationshipEndpoint("b"), "TYPE", Map.empty)))
    -> Effects(WritesAnyNode, WritesRelationships),

    ExecuteUpdateCommandsPipe(SingleRowPipe(), Seq(MergeNodeAction("n", Map.empty, Seq.empty, Seq.empty, Seq.empty, Seq.empty, None)))
      -> Effects(ReadsAllNodes, WritesAnyNode),

    NodeStartPipe(SingleRowPipe(), "n", mock[EntityProducer[Node]])()
      -> Effects(ReadsAllNodes),

    LoadCSVPipe(SingleRowPipe(), null, Literal("apa"), "line", None)
      -> Effects(),

    EmptyResultPipe(SingleRowPipe())
      -> Effects(),

    FilterPipe(SingleRowPipe(), True())()
      -> Effects(),

    FilterPipe(SingleRowPipe(), HasLabel(Identifier("a"), UnresolvedLabel("Apa")))()
      -> Effects(ReadsNodesWithLabels("Apa")),

    ColumnFilterPipe(SingleRowPipe(), Seq(ReturnItem(Literal(42), "a")))
      -> Effects(),

    {
      val trail: Trail = mock[Trail]
      when(trail.predicates).thenReturn(Seq.empty)
      TraversalMatchPipe(SingleRowPipe(), mock[TraversalMatcher], trail) -> Effects(ReadsAllNodes, ReadsRelationships)
    },

    SlicePipe(SingleRowPipe(), Some(Literal(10)), None)
      -> Effects(),

    MatchPipe(
      new FakePipe(Iterator(Map("x" -> null)), "x" -> CTNode), Seq.empty,
      new PatternGraph(Map.empty, Map.empty, Seq.empty, Seq.empty),
      Set("x", "r", "z")
    ) -> AllReadEffects,

    EmptyResultPipe(SingleRowPipe())
      -> Effects(),

    EagerPipe(NodeStartPipe(SingleRowPipe(), "n", mock[EntityProducer[Node]])())
      -> Effects(),

    DistinctPipe(NodeStartPipe(SingleRowPipe(), "n", mock[EntityProducer[Node]])(), Map.empty)()
      -> Effects(ReadsAllNodes),

    DistinctPipe(SingleRowPipe(), Map.empty)()
      -> Effects(),

    OptionalMatchPipe(SingleRowPipe(), NodeStartPipe(SingleRowPipe(), "n", mock[EntityProducer[Node]])(), SymbolTable())
      -> Effects(ReadsAllNodes)
  )

  EFFECTS.foreach { case (pipe: Pipe, effects: Effects) =>
    test(pipe.getClass.getSimpleName + " " + effects) {
      pipe.effects should equal(effects)
    }
  }
}
