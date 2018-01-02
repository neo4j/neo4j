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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Identifier, Literal}
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.UnresolvedLabel
import org.neo4j.cypher.internal.compiler.v2_3.mutation._
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class UpdateCommandExpanderTest extends CypherFunSuite with UpdateCommandExpander {
  // (a)-[r1]->(b), (b)-[r2]->(c), (c)-[r3]->(d)
  val bareB = RelationshipEndpoint("b")
  val createRelationship1 = CreateRelationship("r1", RelationshipEndpoint("a"), bareB, "REL", Map.empty)
  val createRelationship2 = CreateRelationship("r2", bareB, RelationshipEndpoint("c"), "REL", Map.empty)
  val createRelationship3 = CreateRelationship("r3", RelationshipEndpoint("c"), RelationshipEndpoint("d"), "REL", Map.empty)
  val createA = CreateNode("a", Map.empty, Seq.empty)
  val createB = CreateNode("b", Map.empty, Seq.empty)
  val createC = CreateNode("c", Map.empty, Seq.empty)
  val createD = CreateNode("d", Map.empty, Seq.empty)

  val lushA = RelationshipEndpoint(Identifier("a"), Map("x"->Literal(42)), Seq(UnresolvedLabel("LABEL")))
  val lushB = RelationshipEndpoint(Identifier("b"), Map("x"->Literal(23)), Seq(UnresolvedLabel("LABEL2")))
  val createRelationshipWithLushNodes = CreateRelationship("r1", lushA, lushB, "REL", Map.empty)
  val lushCreateA = CreateNode("a", Map("x"->Literal(42)), Seq(UnresolvedLabel("LABEL")))
  val lushCreateB = CreateNode("b", Map("x"->Literal(23)), Seq(UnresolvedLabel("LABEL2")))

  test("should_expand_with_nodes_when_asking_for_a_relationship") {
    // given
    val actions = Seq(createRelationship1)

    // when
    val expanded = expandCommands(actions, new SymbolTable()).toList

    // then
    expanded should equal(List(createA, createB, createRelationship1))
  }

  test("should_expand_with_nodes_when_asking_for_two_disconnected_relationships") {
    // given
    val actions = Seq(createRelationship1, createRelationship3)

    // when
    val expanded = expandCommands(actions, new SymbolTable()).toList

    // then
    expanded should equal(List(createA, createB, createRelationship1, createC, createD, createRelationship3))
  }

  test("should_expand_with_nodes_when_asking_for_two_connected_relationships") {
    // given
    val actions = Seq(createRelationship1, createRelationship2)

    // when
    val expanded = expandCommands(actions, new SymbolTable()).toList

    // then
    expanded should equal(List(createA, createB, createRelationship1, createC, createRelationship2))
  }

  test("should_not_create_already_existing_nodes_with_foreach") {
    // given
    val actions = Seq(createA, createForeach(createRelationship1))

    // when
    val expanded = expandCommands(actions, new SymbolTable()).toList

    // then
    expanded should equal(List(createA, createForeach(createB, createRelationship1)))
  }

  test("should_handle_foreach_in_foreach") {
    // given
    val actions = Seq(createA, createForeach(createRelationship1, createForeach(createRelationship2)))

    // when
    val expanded = expandCommands(actions, new SymbolTable()).toList

    // then
    expanded should equal(List(createA, createForeach(createB, createRelationship1, createForeach(createC, createRelationship2))))
  }

  test("should_remove_properties_from_relationship_when_making_create_node_objects") {
    // given
    val actions = Seq(createRelationshipWithLushNodes)

    // when
    val expanded = expandCommands(actions, new SymbolTable()).toList

    // then
    expanded should equal(List(lushCreateA, lushCreateB, createRelationship1))
  }

  test("should_only_remove_properties_from_relationship_when_making_create_node_objects") {
    // given CREATE (a {prop:42}), (a {prop:42})-[:REL]->(b {prop:43}) ==>
    // CREATE (a {prop:42}), (b {prop:43}), (a {prop:42})-[:REL]->(b)
    val actions = Seq(lushCreateA, createRelationshipWithLushNodes)

    // when
    val expanded = expandCommands(actions, new SymbolTable()).toList

    // then
    expanded should equal(List(lushCreateA, lushCreateB, createRelationshipWithLushNodes.copy(to = bareB)))
  }

  private def createForeach(actions: UpdateAction*) = ForeachAction(Literal(Seq(1, 2, 3)), "x", actions)
}

