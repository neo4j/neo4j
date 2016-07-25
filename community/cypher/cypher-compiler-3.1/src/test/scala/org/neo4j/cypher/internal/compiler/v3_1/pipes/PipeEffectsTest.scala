/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.pipes

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v3_1.commands.expressions.{Variable, Literal}
import org.neo4j.cypher.internal.compiler.v3_1.commands.predicates.{HasLabel, True}
import org.neo4j.cypher.internal.compiler.v3_1.commands.values.UnresolvedLabel
import org.neo4j.cypher.internal.compiler.v3_1.commands.{RelatedTo, ReturnItem}
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.{Effects, _}
import org.neo4j.cypher.internal.compiler.v3_1.mutation.{CreateNode, CreateRelationship, MergeNodeAction, RelationshipEndpoint}
import org.neo4j.cypher.internal.compiler.v3_1.pipes.matching.{PatternGraph, Trail, TraversalMatcher}
import org.neo4j.cypher.internal.compiler.v3_1.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v3_1.symbols._
import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Node
import org.scalatest.prop.TableDrivenPropertyChecks

class PipeEffectsTest extends CypherFunSuite with TableDrivenPropertyChecks {

  implicit val monitor = mock[PipeMonitor]

  val EFFECTS: Map[Pipe, Effects] = Map[Pipe, Effects](
  SingleRowPipe()
    -> Effects(),

  ExecuteUpdateCommandsPipe(SingleRowPipe(), Seq(CreateNode("n", Map.empty, Seq.empty)))
    -> Effects(CreatesAnyNode).asLeafEffects,

  ExecuteUpdateCommandsPipe(SingleRowPipe(), Seq(CreateRelationship("r", RelationshipEndpoint("a"), RelationshipEndpoint("b"), "TYPE", Map.empty)))
    -> Effects(CreatesRelationship("TYPE")).asLeafEffects,

  ExecuteUpdateCommandsPipe(SingleRowPipe(), Seq(
    CreateNode("n", Map.empty, Seq.empty),
    CreateRelationship("r", RelationshipEndpoint("a"), RelationshipEndpoint("b"), "TYPE", Map.empty)))
    -> Effects(CreatesAnyNode, CreatesRelationship("TYPE")).asLeafEffects,

  ExecuteUpdateCommandsPipe(SingleRowPipe(), Seq(MergeNodeAction("n", Map.empty, Seq.empty, Seq.empty, Seq.empty, Seq.empty, None)))
    -> Effects(ReadsAllNodes, CreatesAnyNode).asLeafEffects,

  NodeStartPipe(SingleRowPipe(), "n", mock[EntityProducer[Node]])()
    -> Effects(ReadsAllNodes).asLeafEffects,

  LoadCSVPipe(SingleRowPipe(), null, Literal("apa"), "line", None)()
    -> Effects(),

  EmptyResultPipe(SingleRowPipe())
    -> Effects(),

  FilterPipe(SingleRowPipe(), True())()
    -> Effects(),

  FilterPipe(SingleRowPipe(), HasLabel(Variable("a"), UnresolvedLabel("Apa")))()
    -> Effects(ReadsNodesWithLabels("Apa")),

  ColumnFilterPipe(SingleRowPipe(), Seq(ReturnItem(Literal(42), "a")))
    -> Effects(), {
    val trail: Trail = mock[Trail]
    when(trail.predicates).thenReturn(Seq.empty)
    when(trail.typ).thenReturn(Seq("TYPE"))
    TraversalMatchPipe(SingleRowPipe(), mock[TraversalMatcher], trail) -> Effects(ReadsRelationshipBoundNodes, ReadsRelationshipsWithTypes("TYPE")).asLeafEffects
  },

  SlicePipe(SingleRowPipe(), Some(Literal(10)), None)
    -> Effects(),

  {
    val relatedTo = mock[RelatedTo]
    when(relatedTo.relTypes).thenReturn(Seq("TYPE"))
    MatchPipe(
      new FakePipe(Iterator(Map("x" -> null)), "x" -> CTNode), Seq.empty,
      new PatternGraph(Map.empty, Map.empty, Seq.empty, Seq(relatedTo)),
      Set("x", "r", "z")
    ) -> Effects(ReadsRelationshipsWithTypes("TYPE")).asLeafEffects
  },

  EmptyResultPipe(SingleRowPipe())
    -> Effects(),

  EagerPipe(NodeStartPipe(SingleRowPipe(), "n", mock[EntityProducer[Node]])())()
    -> Effects(),

  DistinctPipe(NodeStartPipe(SingleRowPipe(), "n", mock[EntityProducer[Node]])(), Map.empty)()
    -> Effects(ReadsAllNodes).asLeafEffects,

  DistinctPipe(SingleRowPipe(), Map.empty)()
    -> Effects(),

  OptionalMatchPipe(SingleRowPipe(), NodeStartPipe(SingleRowPipe(), "n", mock[EntityProducer[Node]])(), SymbolTable())
    -> Effects(ReadsAllNodes).asLeafEffects.leafEffectsAsOptional
  )

  EFFECTS.foreach { case (pipe: Pipe, effects: Effects) =>
    test(pipe.getClass.getSimpleName + " " + effects) {
      pipe.effects should equal(effects)
    }
  }
}
