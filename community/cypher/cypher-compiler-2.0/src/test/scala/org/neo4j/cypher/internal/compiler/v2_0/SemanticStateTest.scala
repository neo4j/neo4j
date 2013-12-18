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
package org.neo4j.cypher.internal.compiler.v2_0

import symbols._
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions

class SemanticStateTest extends Assertions {

  @Test
  def shouldDeclareIdentifierOnce() {
    val identifier1 = ast.Identifier("foo", DummyToken(0,1))
    val identifier2 = ast.Identifier("foo", DummyToken(3,6))
    val state = SemanticState.clean.declareIdentifier(identifier1, NodeType()).right.get

    state.declareIdentifier(identifier2, NodeType()) match {
      case Right(_) => fail("Expected an error from second declaration")
      case Left(error) =>
        assertEquals(identifier2.token, error.token)
        assertEquals(Set(identifier1.token), error.references)
    }
  }

  @Test
  def shouldCollectAllIdentifiersWhenImplicitlyDeclared() {
    val identifier1 = ast.Identifier("foo", DummyToken(0,1))
    val identifier2 = ast.Identifier("foo", DummyToken(2,3))
    val identifier3 = ast.Identifier("foo", DummyToken(3,6))

    SemanticState.clean.implicitIdentifier(identifier1, NodeType()) then
    ((_: SemanticState).implicitIdentifier(identifier2, NodeType())) then
    ((_: SemanticState).implicitIdentifier(identifier3, NodeType())) match {
      case Left(_) => fail("Expected success")
      case Right(state) =>
        val tokens = state.symbolTable.get("foo").map(_.tokens)
        assertEquals(Set(identifier1.token, identifier2.token, identifier3.token), tokens.get)
    }
  }

  @Test
  def shouldConstrainTypesForConsecutiveImplicitIdentifierDeclarations() {
    val identifier1 = ast.Identifier("foo", DummyToken(0,1))
    val identifier2 = ast.Identifier("foo", DummyToken(3,6))

    SemanticState.clean.implicitIdentifier(identifier1, NodeType(), RelationshipType()) then
    ((_: SemanticState).implicitIdentifier(identifier2, NodeType())) match {
      case Left(_) => fail("Expected success")
      case Right(state) =>
        val types = state.symbolTypes("foo")
        assertEquals(TypeSet(NodeType()), types)
    }

    SemanticState.clean.implicitIdentifier(identifier1, RelationshipType()) then
    ((_: SemanticState).implicitIdentifier(identifier2, NodeType(), RelationshipType())) match {
      case Left(_) => fail("Expected success")
      case Right(state) =>
        val types = state.symbolTypes("foo")
        assertEquals(TypeSet(RelationshipType()), types)
    }

    SemanticState.clean.implicitIdentifier(identifier1, NodeType(), RelationshipType()) then
    ((_: SemanticState).implicitIdentifier(identifier2, AnyType())) match {
      case Left(_) => fail("Expected success")
      case Right(state) =>
        val types = state.symbolTypes("foo")
        assertEquals(TypeSet(NodeType(), RelationshipType()), types)
    }

    SemanticState.clean.implicitIdentifier(identifier1, NodeType()) then
    ((_: SemanticState).implicitIdentifier(identifier2, MapType())) match {
      case Left(_) => fail("Expected success")
      case Right(state) =>
        val types = state.symbolTypes("foo")
        assertEquals(TypeSet(NodeType()), types)
    }
  }

  @Test
  def shouldFailIfNoPossibleTypesRemainAfterImplicitIdentifierDeclaration() {
    SemanticState.clean.implicitIdentifier(ast.Identifier("foo", DummyToken(0,1)), MapType()) then
      ((_: SemanticState).implicitIdentifier(ast.Identifier("foo", DummyToken(3,6)), NodeType())) match {
      case Right(_) => fail("Expected an error")
      case Left(error) =>
        assertEquals(DummyToken(3,6), error.token)
        assertEquals(Seq(DummyToken(0,1)), error.references.toSeq)
        assertEquals("Type mismatch: foo already defined with conflicting type Map (expected Node)", error.msg)
    }

    SemanticState.clean.implicitIdentifier(ast.Identifier("foo", DummyToken(0,1)), NodeType(), RelationshipType()) then
    ((_: SemanticState).implicitIdentifier(ast.Identifier("foo", DummyToken(3,6)), NodeType(), IntegerType())) then
    ((_: SemanticState).implicitIdentifier(ast.Identifier("foo", DummyToken(9,12)), IntegerType(), RelationshipType())) match {
      case Right(_) => fail("Expected an error")
      case Left(error) =>
        assertEquals(DummyToken(9,12), error.token)
        assertEquals(Seq(DummyToken(0,1), DummyToken(3,6)), error.references.toSeq)
        assertEquals("Type mismatch: foo already defined with conflicting type Node (expected Integer or Relationship)", error.msg)
    }
  }

  @Test
  def shouldRecordTypeForExpressionWhenSpecifyingType() {
    val expression = DummyExpression(TypeSet(IntegerType(), StringType()), DummyToken(0,1))
    val state = SemanticState.clean.specifyType(expression, expression.possibleTypes).right.get
    assertEquals(expression.possibleTypes, state.expressionType(expression).specified)
    assertEquals(expression.possibleTypes, state.expressionType(expression).actual)
  }

  @Test
  def shouldExpectTypeForExpression() {
    val expression = DummyExpression(TypeSet(IntegerType(), LongType(), StringType(), MapType()), DummyToken(0,1))
    val state = SemanticState.clean.specifyType(expression, expression.possibleTypes).right.get

    state.expectType(expression, NumberType()) match {
      case (s, typ) =>
        assertEquals(TypeSet(IntegerType(), LongType()), typ)
        assertEquals(typ, s.expressionType(expression).actual)
    }

    state.expectType(expression, NodeType(), NumberType()) match {
      case (s, typ) =>
        assertEquals(TypeSet(IntegerType(), LongType()), typ)
        assertEquals(typ, s.expressionType(expression).actual)
    }
  }

  @Test
  def shouldFindSymbolInParent() {
    val s1 = SemanticState.clean.declareIdentifier(ast.Identifier("foo", DummyToken(0, 1)), NodeType()).right.get
    val s2 = s1.newScope
    assertEquals(TypeSet(NodeType()), s2.symbolTypes("foo"))
  }

  @Test
  def shouldOverrideSymbolInParent() {
    val s1 = SemanticState.clean.declareIdentifier(ast.Identifier("foo", DummyToken(0, 1)), NodeType()).right.get
    val s2 = s1.newScope.declareIdentifier(ast.Identifier("foo", DummyToken(0, 1)), StringType()).right.get
    assertEquals(TypeSet(StringType()), s2.symbolTypes("foo"))
  }

  @Test
  def shouldExtendSymbolInParent() {
    val s1 = SemanticState.clean.declareIdentifier(ast.Identifier("foo", DummyToken(0, 1)), NodeType()).right.get
    val s2 = s1.newScope.implicitIdentifier(ast.Identifier("foo", DummyToken(0, 1)), AnyType()).right.get
    assertEquals(TypeSet(NodeType()), s2.symbolTypes("foo"))
  }

  @Test
  def shouldReturnTypesOfIdentifier() {
    val identifier = ast.Identifier("foo", DummyToken(0, 1))
    val s1 = SemanticState.clean.declareIdentifier(identifier, NodeType()).right.get
    assertEquals(TypeSet(NodeType()), s1.expressionType(identifier).actual)
  }

  @Test
  def shouldReturnTypesOfIdentifierAtLaterExpression() {
    val identifier1 = ast.Identifier("foo", DummyToken(0, 1))
    val identifier2 = ast.Identifier("foo", DummyToken(3, 5))
    val s1 = SemanticState.clean.declareIdentifier(identifier1, NodeType()).right.get
    val s2 = s1.implicitIdentifier(identifier2, NodeType()).right.get
    assertEquals(TypeSet(NodeType()), s2.expressionType(identifier2).actual)
  }

  @Test
  def shouldReturnTypesOfIdentifierAfterClear() {
    val identifier = ast.Identifier("foo", DummyToken(0, 1))
    val s1 = SemanticState.clean.declareIdentifier(identifier, NodeType()).right.get
    assertEquals(TypeSet(NodeType()), s1.clearSymbols.expressionType(identifier).actual)
  }

  @Test
  def shouldReturnTypesOfLaterImplicitIdentifierAfterClear() {
    val identifier1 = ast.Identifier("foo", DummyToken(0, 1))
    val identifier2 = ast.Identifier("foo", DummyToken(3, 5))
    val s1 = SemanticState.clean.declareIdentifier(identifier1, NodeType()).right.get
    val s2 = s1.implicitIdentifier(identifier2, NodeType()).right.get
    assertEquals(TypeSet(NodeType()), s2.clearSymbols.expressionType(identifier2).actual)
  }

  @Test
  def shouldReturnTypesOfLaterEnsuredIdentifierAfterClear() {
    val identifier1 = ast.Identifier("foo", DummyToken(0, 1))
    val identifier2 = ast.Identifier("foo", DummyToken(3, 5))
    val s1 = SemanticState.clean.declareIdentifier(identifier1, NodeType()).right.get
    val s2 = s1.ensureIdentifierDefined(identifier2).right.get
    assertEquals(TypeSet(NodeType()), s2.clearSymbols.expressionType(identifier2).actual)
  }

  @Test
  def shouldNotReturnSymbolOfIdentifierAfterClear() {
    val s1 = SemanticState.clean.declareIdentifier(ast.Identifier("foo", DummyToken(0, 1)), NodeType()).right.get
    assertEquals(None, s1.clearSymbols.symbol("foo"))
    assertEquals(TypeSet.all, s1.clearSymbols.symbolTypes("foo"))
  }

  implicit class ChainableSemanticStateEither(either: Either[SemanticError, SemanticState]) {
    def then(next: SemanticState => Either[SemanticError, SemanticState]): Either[SemanticError, SemanticState] = {
      either match {
        case Left(_)      => either
        case Right(state) => next(state)
      }
    }
  }
}
