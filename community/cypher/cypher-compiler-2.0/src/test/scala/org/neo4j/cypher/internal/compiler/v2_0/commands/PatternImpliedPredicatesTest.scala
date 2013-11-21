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
package org.neo4j.cypher.internal.compiler.v2_0.commands

import org.junit.Test
import org.scalatest.Assertions
import org.neo4j.cypher.internal.compiler.v2_0.commands.values.TokenType.{PropertyKey, Label}
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.{Property, Identifier, Literal}
import org.neo4j.graphdb.Direction

class PatternImpliedPredicatesTest extends Assertions {

  @Test
  def should_build_implied_node_predicates_for_single_node() {
    // given
    val pattern = SingleNode("a", Seq(Label("Person")), Map("name" -> Literal("Alistair")))

    // when
    val result = pattern.impliedNodePredicates.toSet

    // then
    assert(Set(hasLabel("a", "Person"), hasProperty("a", "name", "Alistair")) === result)
  }

  @Test
  def should_build_implied_node_predicates_for_related_to() {
    // given
    val nodeA = SingleNode("a", Seq(Label("Person")), Map("name" -> Literal("Stefan")))
    val nodeB = SingleNode("b", Seq(Label("Person")), Map("name" -> Literal("Andres")))
    
    val pattern = RelatedTo(nodeA, nodeB, "r", Seq("FRIEND", "LIKES"), Direction.BOTH, Map("since" -> Literal(2013)))

    // when
    val result = pattern.impliedNodePredicates.toSet
    val expected = (nodeA.impliedNodePredicates ++ nodeB.impliedNodePredicates).toSet

    // then
    assert(expected === result)
  }

  @Test
  def should_build_implied_node_predicates_for_var_length() {
    // given
    val nodeA = SingleNode("a", Seq(Label("Person")), Map("name" -> Literal("Stefan")))
    val nodeB = SingleNode("b", Seq(Label("Person")), Map("name" -> Literal("Andres")))

    val pattern = VarLengthRelatedTo("p", nodeA, nodeB, Some(1), Some(2), Seq("FRIEND", "LIKES"), Direction.BOTH, None, Map("since" -> Literal(2013)))

    // when
    val result = pattern.impliedNodePredicates.toSet
    val expected = (nodeA.impliedNodePredicates ++ nodeB.impliedNodePredicates).toSet

    // then
    assert(expected === result)
  }

  @Test
  def should_build_implied_node_predicates_for_shortest_path() {
    // given
    val nodeA = SingleNode("a", Seq(Label("Person")), Map("name" -> Literal("Stefan")))
    val nodeB = SingleNode("b", Seq(Label("Person")), Map("name" -> Literal("Andres")))

    val pattern = ShortestPath("p", nodeA, nodeB, Seq("FRIEND"), Direction.BOTH, None, single = false, None)

    // when
    val result = pattern.impliedNodePredicates.toSet
    val expected = (nodeA.impliedNodePredicates ++ nodeB.impliedNodePredicates).toSet

    // then
    assert(expected === result)
  }

  def hasLabel(name: String, labelName: String) = HasLabel(Identifier(name), Label(labelName))

  def hasProperty(name: String, propertyKeyName: String, value: Any) =
    Equals(Property(Identifier(name), PropertyKey(propertyKeyName)), Literal(value))
}