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
package org.neo4j.cypher.internal.parser.v2_0

import org.neo4j.cypher.internal.symbols._
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions

class SemanticStateTest extends Assertions {

  @Test
  def shouldDeclareIdentifierOnce() {
    SemanticState.clean.declareIdentifier(ast.Identifier("foo", DummyToken(0,1)), NodeType()) match {
      case Left(_) => fail("Expected first declaration to succeed")
      case Right(state) => {
        state.declareIdentifier(ast.Identifier("foo", DummyToken(2,3)), NodeType()) match {
          case Right(_) => fail("Expected an error from second declaration")
          case Left(error) => {
            assertEquals(DummyToken(2,3), error.token)
            assertEquals(Set(DummyToken(0,1)), error.references)
          }
        }
      }
    }
  }

  @Test
  def shouldCollectAllIdentifiersWhenImplicitlyDeclared() {
    SemanticState.clean.implicitIdentifier(ast.Identifier("foo", DummyToken(0,1)), NodeType()) then
    ((_: SemanticState).implicitIdentifier(ast.Identifier("foo", DummyToken(2,3)), NodeType())) then
    ((_: SemanticState).implicitIdentifier(ast.Identifier("foo", DummyToken(3,6)), NodeType())) match {
      case Left(_) => fail("Expected success")
      case Right(state) => {
        val tokens = for (symbol <- state.symbolTable.get("foo")) yield symbol.tokens
        assertEquals(Set(DummyToken(0,1), DummyToken(2,3), DummyToken(3,6)), tokens.get)
      }
    }
  }

  @Test
  def shouldMergeUpTypesForImplicitIdentifierDeclarations() {
    SemanticState.clean.implicitIdentifier(ast.Identifier("foo", DummyToken(0,1)), NodeType(), RelationshipType()) then
    ((_: SemanticState).implicitIdentifier(ast.Identifier("foo", DummyToken(3,6)), NodeType())) match {
      case Left(_) => fail("Expected success")
      case Right(state) => {
        val types = state.symbolTypes("foo")
        assertEquals(Some(Set(NodeType())), types)
      }
    }
    SemanticState.clean.implicitIdentifier(ast.Identifier("foo", DummyToken(0,1)), RelationshipType()) then
    ((_: SemanticState).implicitIdentifier(ast.Identifier("foo", DummyToken(3,6)), NodeType(), RelationshipType())) match {
      case Left(_) => fail("Expected success")
      case Right(state) => {
        val types = state.symbolTypes("foo")
        assertEquals(Some(Set(RelationshipType())), types)
      }
    }
    SemanticState.clean.implicitIdentifier(ast.Identifier("foo", DummyToken(0,1)), NodeType(), RelationshipType()) then
    ((_: SemanticState).implicitIdentifier(ast.Identifier("foo", DummyToken(3,6)), ScalarType())) match {
      case Left(_) => fail("Expected success")
      case Right(state) => {
        val types = state.symbolTypes("foo")
        assertEquals(Some(Set(NodeType(), RelationshipType())), types)
      }
    }
  }

  @Test
  def shouldFailIfNoPossibleTypesRemainAfterImplicitIdentifierDeclaration() {
    SemanticState.clean.implicitIdentifier(ast.Identifier("foo", DummyToken(0,1)), NodeType(), RelationshipType()) then
    ((_: SemanticState).implicitIdentifier(ast.Identifier("foo", DummyToken(3,6)), NodeType(), IntegerType())) then
    ((_: SemanticState).implicitIdentifier(ast.Identifier("foo", DummyToken(9,12)), IntegerType(), RelationshipType())) match {
      case Right(_) => fail("Expected an error")
      case Left(error) => {
        assertEquals(DummyToken(9,12), error.token)
        assertEquals(Seq(DummyToken(0,1), DummyToken(3,6)), error.references.toSeq)
        assertEquals("Type mismatch: foo already defined with conflicting type Node (expected Integer or Relationship)", error.msg)
      }
    }
  }

  @Test
  def shouldFindSymbolInParent() {
    val s1 = SemanticState.clean.declareIdentifier(ast.Identifier("foo", DummyToken(0, 1)), NodeType()).right.get
    val s2 = s1.newScope
    assertEquals(Some(Set(NodeType())), s2.symbolTypes("foo"))
  }

  @Test
  def shouldOverrideSymbolInParent() {
    val s1 = SemanticState.clean.declareIdentifier(ast.Identifier("foo", DummyToken(0, 1)), NodeType()).right.get
    val s2 = s1.newScope.declareIdentifier(ast.Identifier("foo", DummyToken(0, 1)), StringType()).right.get
    assertEquals(Some(Set(StringType())), s2.symbolTypes("foo"))
  }

  @Test
  def shouldExtendSymbolInParent() {
    val s1 = SemanticState.clean.declareIdentifier(ast.Identifier("foo", DummyToken(0, 1)), NodeType()).right.get
    val s2 = s1.newScope.implicitIdentifier(ast.Identifier("foo", DummyToken(0, 1)), ScalarType()).right.get
    assertEquals(Some(Set(NodeType())), s2.symbolTypes("foo"))
  }

  implicit class ChainableSemanticStateEither(either: Either[SemanticError, SemanticState]) {
    def then(next: SemanticState => Either[SemanticError, SemanticState]) : Either[SemanticError, SemanticState] = {
      either match {
        case Left(_) => either
        case Right(state) => next(state)
      }
    }
  }
}