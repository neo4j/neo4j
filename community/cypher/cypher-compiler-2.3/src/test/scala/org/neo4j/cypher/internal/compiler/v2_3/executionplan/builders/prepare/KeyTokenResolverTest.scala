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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders.prepare

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_3.commands._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Identifier, Literal, Property}
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.{StartsWith, HasLabel, LiteralRegularExpression}
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.KeyToken.Resolved
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.{KeyToken, TokenType, UnresolvedLabel, UnresolvedProperty}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.PlanBuilder
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders.{BuilderTest, Unsolved}
import org.neo4j.cypher.internal.compiler.v2_3.mutation.{CreateUniqueAction, NamedExpectation, UniqueLink}
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection

class KeyTokenResolverTest extends BuilderTest {

  val builder: PlanBuilder = new KeyTokenResolver
  context = mock[PlanContext]

  val (unresolvedFoo, resolvedFoo) = labelToken("Foo", 0)
  val (unresolvedBar, resolvedBar) = labelToken("Bar", 1)

  when(context.getOptLabelId("Foo")).thenReturn(Some(0))
  when(context.getOptLabelId("Bar")).thenReturn(Some(1))
  when(context.getOptPropertyKeyId("APA")).thenReturn(Some(0))

  test("should_not_accept_empty_query") {
    val q = Query.empty

    assertRejects(q)
  }

  test("should_resolve_label_keytoken_on_label_predicate") {
    val q = Query.
      matches(SingleNode("a")).
      where(HasLabel(Identifier("a"), unresolvedFoo)).
      returns()

    val result = assertAccepts(q)

    result.query.where should equal(Seq(Unsolved(HasLabel(Identifier("a"), resolvedFoo))))
  }

  test("should_resolve_label_keytoken_on_single_node_pattern") {
    val q = Query.
      matches(SingleNode("a", Seq(unresolvedFoo))).
      returns()

    val result = assertAccepts(q)
    result.query.patterns should equal(Seq(Unsolved(SingleNode("a", Seq(resolvedFoo)))))
  }

  test("should_resolve_label_keytoken_on_related_to_pattern") {
    val q = Query.
      matches(RelatedTo(SingleNode("a", Seq(unresolvedFoo)), SingleNode("b", Seq(unresolvedBar)), "r", Seq("KNOWS"), SemanticDirection.OUTGOING, Map.empty)).
      returns()

    val result = assertAccepts(q)
    result.query.patterns should equal(Seq(Unsolved(RelatedTo(SingleNode("a", Seq(resolvedFoo)), SingleNode("b", Seq(resolvedBar)), "r", Seq("KNOWS"), SemanticDirection.OUTGOING, Map.empty))))
  }

  test("should_resolve_label_keytoken_on_var_length_pattern") {
    val q = Query.
      matches(VarLengthRelatedTo("p", SingleNode("a", Seq(unresolvedFoo)), SingleNode("b", Seq(unresolvedBar)), None, None, Seq.empty, SemanticDirection.OUTGOING, None, Map.empty)).
      returns()

    val result = assertAccepts(q)
    result.query.patterns should equal(Seq(Unsolved(VarLengthRelatedTo("p", SingleNode("a", Seq(resolvedFoo)), SingleNode("b", Seq(resolvedBar)), None, None, Seq.empty, SemanticDirection.OUTGOING, None, Map.empty))))
  }

  test("should_resolve_label_keytoken_on_shortest_path_length_pattern") {
    val q = Query.
      matches(ShortestPath("p", SingleNode("a", Seq(unresolvedFoo)), SingleNode("b", Seq(unresolvedBar)), Seq.empty, SemanticDirection.OUTGOING, false, None, single = false, relIterator = None)).
      returns()

    val result = assertAccepts(q)
    result.query.patterns should equal(Seq(Unsolved(ShortestPath("p", SingleNode("a", Seq(resolvedFoo)), SingleNode("b", Seq(resolvedBar)), Seq.empty, SemanticDirection.OUTGOING, false, None, single = false, relIterator = None))))
  }

  test("should resolve property key for STARTS WITH expressions") {
    val q = Query.
      matches(SingleNode("n")).
      where(StartsWith(Property(Identifier("x"), UnresolvedProperty("APA")), Literal("A"))).
      returns()

    val result = assertAccepts(q)
    result.query.where should equal(Seq(Unsolved(StartsWith(Property(Identifier("x"), Resolved("APA", 0, TokenType.PropertyKey)), Literal("A")))))
  }

  test("should_resolve_label_keytoken_on_unique_link_pattern") {
    val aNode = NamedExpectation("a", properties = Map.empty, Seq(unresolvedFoo))
    val bNode = NamedExpectation("b", properties = Map.empty, Seq(unresolvedBar))
    val rel = NamedExpectation("r")

    val q = Query.
      start(CreateUniqueStartItem(CreateUniqueAction(UniqueLink(aNode, bNode, rel, "KNOWS", SemanticDirection.OUTGOING)))).
      returns()

    val result = assertAccepts(q)
    val resolvedLink = UniqueLink(aNode.copy(labels = Seq(resolvedFoo)), bNode.copy(labels = Seq(resolvedBar)), rel, "KNOWS", SemanticDirection.OUTGOING)
    result.query.start should equal(Seq(Unsolved(CreateUniqueStartItem(CreateUniqueAction(resolvedLink)))))
  }

  private def labelToken(name: String, id: Int): (KeyToken, KeyToken) =
    (UnresolvedLabel(name), Resolved(name, id, TokenType.Label))
}


