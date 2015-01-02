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
package org.neo4j.cypher.internal.compiler.v2_0.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_0._
import commands.expressions.{Expression, Identifier, Literal}
import commands.values.{UnresolvedLabel, KeyToken}
import mutation.{CreateNode, RelationshipEndpoint, CreateRelationship}
import symbols._
import org.junit.Test
import org.scalatest.Assertions

class CreateMissingNodesTest extends Assertions {
  @Test def should_do_it_simplest_case() {
    // Given (@a)-[:FOO]->(b)

    val symbolTable = new SymbolTable(Map("a" -> CTNode))
    val relationship = CreateRelationship("r", endPoint("a"), endPoint("b"), "FOO", Map.empty)
    val (symbols, actions) = MergePatternBuilder.createActions(symbolTable, Seq(relationship))

    assert(actions.toList === List(CreateNode("b", Map.empty, Seq.empty), relationship))
    assert(symbols === symbolTable.add("b", CTNode))
  }

  private def endPoint(name: String, props: Map[String, Expression] = Map.empty, labels: Seq[KeyToken] = Seq.empty) =
    RelationshipEndpoint(Identifier(name), props, labels)

  @Test def should_handle_properties() {
    // Given (@a)-[:FOO]->(b {id:42})

    val symbolTable = new SymbolTable(Map("a" -> CTNode))
    val props = Map("id" -> Literal(42))
    val relationship = CreateRelationship("r", endPoint("a"), endPoint("b", props), "FOO", Map.empty)
    val (symbols, actions) = MergePatternBuilder.createActions(symbolTable, Seq(relationship))

    assert(actions.toList === List(CreateNode("b", props, Seq.empty), relationship))
    assert(symbols === symbolTable.add("b", CTNode))
  }

  @Test def should_handle_labels() {
    // Given (@a)-[:FOO]->(b:Foo)

    val symbolTable = new SymbolTable(Map("a" -> CTNode))
    val labels = Seq(UnresolvedLabel("FOO"))
    val relationship = CreateRelationship("r", endPoint("a"), endPoint("b", labels = labels), "FOO", Map.empty)
    val (symbols, actions) = MergePatternBuilder.createActions(symbolTable, Seq(relationship))

    assert(actions.toList === List(CreateNode("b", Map.empty, labels), relationship))
    assert(symbols === symbolTable.add("b", CTNode))
  }

  @Test def should_handle_labels_and_properties() {
    // Given (@a)-[:FOO]->(b:Foo {id:42})

    val symbolTable = new SymbolTable(Map("a" -> CTNode))
    val labels = Seq(UnresolvedLabel("FOO"))
    val props = Map("id" -> Literal(42))
    val relationship = CreateRelationship("r", endPoint("a"), endPoint("b", labels = labels, props = props), "FOO", Map.empty)
    val (symbols, actions) = MergePatternBuilder.createActions(symbolTable, Seq(relationship))

    assert(actions.toList === List(CreateNode("b", props, labels), relationship))
    assert(symbols === symbolTable.add("b", CTNode))
  }

  @Test def should_not_create_nodes() {
    // Given (@a)-[r1:FOO]->(b)-[r2:FOO]->(c)

    val symbolTable = new SymbolTable(Map("a" -> CTNode))
    val r1 = CreateRelationship("r1", endPoint("a"), endPoint("b"), "FOO", Map.empty)
    val r2 = CreateRelationship("r2", endPoint("b"), endPoint("c"), "FOO", Map.empty)
    val (symbols, actions) = MergePatternBuilder.createActions(symbolTable, Seq(r1, r2))

    assert(actions.toList === List(bareNode("b"), r1, bareNode("c"), r2))
    val expectedSymbols = symbolTable.add("b", CTNode).add("c", CTNode)
    assert(symbols === expectedSymbols)
  }

  @Test def should_create_both_nodes()   {
    // Given (a)-[:FOO]->(b)

    val symbolTable = new SymbolTable()
    val relationship = CreateRelationship("r", endPoint("a"), endPoint("b"), "FOO", Map.empty)
    val (symbols, actions) = MergePatternBuilder.createActions(symbolTable, Seq(relationship))

    assert(actions.toList === List(CreateNode("a", Map.empty, Seq.empty), CreateNode("b", Map.empty, Seq.empty), relationship))
    val expectedSymbols = new SymbolTable(Map("a" -> CTNode, "b" -> CTNode))
    assert(symbols === expectedSymbols)
  }

  private def bareNode(name: String) = CreateNode(name, Map.empty, Seq.empty)
}
