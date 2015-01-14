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
package org.neo4j.cypher.internal.compiler.v2_0.executionplan.builders.prepare

import org.junit.Test
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.builders.BuilderTest
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.PlanBuilder
import org.neo4j.cypher.internal.compiler.v2_0.commands._
import org.neo4j.cypher.internal.compiler.v2_0.commands.values.{KeyToken, TokenType, UnresolvedLabel}
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Identifier
import org.neo4j.cypher.internal.compiler.v2_0.spi.PlanContext
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_0.commands.SingleNode
import org.neo4j.cypher.internal.compiler.v2_0.commands.values.KeyToken.Resolved
import scala.Some
import org.neo4j.cypher.internal.compiler.v2_0.commands.HasLabel
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.builders.Unsolved
import org.neo4j.cypher.internal.compiler.v2_0.mutation.{NamedExpectation, UniqueLink, CreateUniqueAction}

class KeyTokenResolverTest extends BuilderTest with MockitoSugar {

  val builder: PlanBuilder = new KeyTokenResolver
  override val context = mock[PlanContext]

  val (unresolvedFoo, resolvedFoo) = labelToken("Foo", 0)
  val (unresolvedBar, resolvedBar) = labelToken("Bar", 1)

  when(context.getOptLabelId("Foo")).thenReturn(Some(0))
  when(context.getOptLabelId("Bar")).thenReturn(Some(1))

  @Test
  def should_not_accept_empty_query() {
    val q = Query.empty

    assertRejects(q)
  }

  @Test
  def should_resolve_label_keytoken_on_label_predicate() {
    val q = Query.
      matches(SingleNode("a")).
      where(HasLabel(Identifier("a"), unresolvedFoo)).
      returns()

    val result = assertAccepts(q)

    assert(result.query.where === Seq(Unsolved(HasLabel(Identifier("a"), resolvedFoo))))
  }

  @Test
  def should_resolve_label_keytoken_on_single_node_pattern() {
    val q = Query.
      matches(SingleNode("a", Seq(unresolvedFoo))).
      returns()

    val result = assertAccepts(q)
    assert(result.query.patterns === Seq(Unsolved(SingleNode("a", Seq(resolvedFoo)))))
  }

  @Test
  def should_resolve_label_keytoken_on_related_to_pattern() {
    val q = Query.
      matches(RelatedTo(SingleNode("a", Seq(unresolvedFoo)), SingleNode("b", Seq(unresolvedBar)), "r", Seq("KNOWS"), Direction.OUTGOING, Map.empty)).
      returns()

    val result = assertAccepts(q)
    assert(result.query.patterns === Seq(Unsolved(RelatedTo(SingleNode("a", Seq(resolvedFoo)), SingleNode("b", Seq(resolvedBar)), "r", Seq("KNOWS"), Direction.OUTGOING, Map.empty))))
  }

  @Test
  def should_resolve_label_keytoken_on_var_length_pattern() {
    val q = Query.
      matches(VarLengthRelatedTo("p", SingleNode("a", Seq(unresolvedFoo)), SingleNode("b", Seq(unresolvedBar)), None, None, Seq.empty, Direction.OUTGOING, None, Map.empty)).
      returns()

    val result = assertAccepts(q)
    assert(result.query.patterns === Seq(Unsolved(VarLengthRelatedTo("p", SingleNode("a", Seq(resolvedFoo)), SingleNode("b", Seq(resolvedBar)), None, None, Seq.empty, Direction.OUTGOING, None, Map.empty))))
  }

  @Test
  def should_resolve_label_keytoken_on_shortest_path_length_pattern() {
    val q = Query.
      matches(ShortestPath("p", SingleNode("a", Seq(unresolvedFoo)), SingleNode("b", Seq(unresolvedBar)), Seq.empty, Direction.OUTGOING, false, None, single = false, relIterator = None)).
      returns()

    val result = assertAccepts(q)
    assert(result.query.patterns === Seq(Unsolved(ShortestPath("p", SingleNode("a", Seq(resolvedFoo)), SingleNode("b", Seq(resolvedBar)), Seq.empty, Direction.OUTGOING, false, None, single = false, relIterator = None))))
  }

  @Test
  def should_resolve_label_keytoken_on_unique_link_pattern() {
    val aNode = NamedExpectation("a", properties = Map.empty, Seq(unresolvedFoo))
    val bNode = NamedExpectation("b", properties = Map.empty, Seq(unresolvedBar))
    val rel = NamedExpectation("r")

    val q = Query.
      start(CreateUniqueStartItem(CreateUniqueAction(UniqueLink(aNode, bNode, rel, "KNOWS", Direction.OUTGOING)))).
      returns()

    val result = assertAccepts(q)
    val resolvedLink = UniqueLink(aNode.copy(labels = Seq(resolvedFoo)), bNode.copy(labels = Seq(resolvedBar)), rel, "KNOWS", Direction.OUTGOING)
    assert(result.query.start === Seq(Unsolved(CreateUniqueStartItem(CreateUniqueAction(resolvedLink)))))
  }

  private def labelToken(name: String, id: Int): (KeyToken, KeyToken) =
    (UnresolvedLabel(name), Resolved(name, id, TokenType.Label))
}


