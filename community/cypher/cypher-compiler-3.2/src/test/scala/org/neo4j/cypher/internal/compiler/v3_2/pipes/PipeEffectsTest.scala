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
package org.neo4j.cypher.internal.compiler.v3_2.pipes

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v3_2.commands.RelatedTo
import org.neo4j.cypher.internal.compiler.v3_2.commands.expressions.{Literal, Variable}
import org.neo4j.cypher.internal.compiler.v3_2.commands.predicates.{HasLabel, True}
import org.neo4j.cypher.internal.compiler.v3_2.commands.values.UnresolvedLabel
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.{Effects, _}
import org.neo4j.cypher.internal.compiler.v3_2.pipes.matching.PatternGraph
import org.neo4j.cypher.internal.frontend.v3_2.symbols._
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Node
import org.scalatest.prop.TableDrivenPropertyChecks

class PipeEffectsTest extends CypherFunSuite with TableDrivenPropertyChecks {

  implicit val monitor = mock[PipeMonitor]

  val EFFECTS: Map[Pipe, Effects] = Map[Pipe, Effects](
  SingleRowPipe()
    -> Effects(),

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

  {
    val relatedTo = mock[RelatedTo]
    when(relatedTo.relTypes).thenReturn(Seq("TYPE"))
    MatchPipe(
      new FakePipe(Iterator(Map("x" -> null)), "x" -> CTNode), Seq.empty,
      PatternGraph(Map.empty, Map.empty, Seq.empty, Seq(relatedTo)),
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
    -> Effects()
  )

  EFFECTS.foreach { case (pipe: Pipe, effects: Effects) =>
    test(pipe.getClass.getSimpleName + " " + effects) {
      pipe.effects should equal(effects)
    }
  }
}
