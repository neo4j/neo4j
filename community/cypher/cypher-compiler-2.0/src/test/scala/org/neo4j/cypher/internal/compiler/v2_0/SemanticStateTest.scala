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
      case Left(error) => {
        assertEquals(identifier2.token, error.token)
        assertEquals(Set(identifier1.token), error.references)
      }
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
      case Right(state) => {
        val tokens = state.symbolTable.get("foo").map(_.tokens)
        assertEquals(Set(identifier1.token, identifier2.token, identifier3.token), tokens.get)
      }
    }
  }

  @Test
  def shouldConstrainTypesForConsecutiveImplicitIdentifierDeclarations() {
    val identifier1 = ast.Identifier("foo", DummyToken(0,1))
    val identifier2 = ast.Identifier("foo", DummyToken(3,6))

    SemanticState.clean.implicitIdentifier(identifier1, NodeType(), RelationshipType()) then
    ((_: SemanticState).implicitIdentifier(identifier2, NodeType())) match {
      case Left(_) => fail("Expected success")
      case Right(state) => {
        val types = state.symbolTypes("foo")
        assertEquals(TypeSet(NodeType()), types)
      }
    }

    SemanticState.clean.implicitIdentifier(identifier1, RelationshipType()) then
    ((_: SemanticState).implicitIdentifier(identifier2, NodeType(), RelationshipType())) match {
      case Left(_) => fail("Expected success")
      case Right(state) => {
        val types = state.symbolTypes("foo")
        assertEquals(TypeSet(RelationshipType()), types)
      }
    }

    SemanticState.clean.implicitIdentifier(identifier1, NodeType(), RelationshipType()) then
    ((_: SemanticState).implicitIdentifier(identifier2, AnyType())) match {
      case Left(_) => fail("Expected success")
      case Right(state) => {
        val types = state.symbolTypes("foo")
        assertEquals(TypeSet(NodeType(), RelationshipType()), types)
      }
    }

    SemanticState.clean.implicitIdentifier(identifier1, NodeType()) then
    ((_: SemanticState).implicitIdentifier(identifier2, MapType())) match {
      case Left(_) => fail("Expected success")
      case Right(state) => {
        val types = state.symbolTypes("foo")
        assertEquals(TypeSet(NodeType()), types)
      }
    }
  }

  @Test
  def constrainTypeOnIdentifierShouldLimitSymbolTypes() {
    val identifier = ast.Identifier("foo", DummyToken(0,1))
    val state = SemanticState.clean.declareIdentifier(identifier, NodeType(), RelationshipType(), NumberType()).right.get

    state.constrainType(identifier, NodeType(), NumberType()) match {
      case Left(_) => fail("Expected success")
      case Right(state) => {
        val types = state.symbolTypes("foo")
        assertEquals(TypeSet(NodeType(), NumberType()), types)
      }
    }

    state.constrainType(identifier, MapType()) match {
      case Left(_) => fail("Expected success")
      case Right(state) => {
        val types = state.symbolTypes("foo")
        assertEquals(TypeSet(NodeType(), RelationshipType()), types)
      }
    }

    state.constrainType(identifier, MapType(), LongType()) match {
      case Left(_) => fail("Expected success")
      case Right(state) => {
        val types = state.symbolTypes("foo")
        assertEquals(TypeSet(NodeType(), RelationshipType()), types)
      }
    }
  }

  @Test
  def shouldConstrainTypesForConsecutiveImplicitIdentifierDeclarationsOnlyUsingPreviousDeclarations() {
    val identifier1 = ast.Identifier("foo", DummyToken(0,1))
    val identifier2 = ast.Identifier("foo", DummyToken(3,6))

    SemanticState.clean.declareIdentifier(identifier1, NodeType(), RelationshipType()) then
    ((_: SemanticState).constrainType(identifier1, MapType())) then
    ((_: SemanticState).implicitIdentifier(identifier2, NodeType())) match {
      case Left(_) => fail("Expected success")
      case Right(state) => {
        val types = state.symbolTypes("foo")
        assertEquals(TypeSet(NodeType()), types)
      }
    }
  }

  @Test
  def shouldFailIfNoPossibleTypesRemainAfterImplicitIdentifierDeclaration() {
    SemanticState.clean.implicitIdentifier(ast.Identifier("foo", DummyToken(0,1)), MapType()) then
      ((_: SemanticState).implicitIdentifier(ast.Identifier("foo", DummyToken(3,6)), NodeType())) match {
      case Right(_) => fail("Expected an error")
      case Left(error) => {
        assertEquals(DummyToken(3,6), error.token)
        assertEquals(Seq(DummyToken(0,1)), error.references.toSeq)
        assertEquals("Type mismatch: foo already defined with conflicting type Map (expected Node)", error.msg)
      }
    }

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
  def shouldConstrainTypeForIdentifier() {
    val identifier = ast.Identifier("n", DummyToken(0,1))
    val state = SemanticState.clean.declareIdentifier(identifier, IntegerType(), LongType(), StringType(), MapType()).right.get

    state.constrainType(identifier, NumberType()) match {
      case Left(_) => fail("Expected success")
      case Right(s) =>
        assertEquals(TypeSet(IntegerType(), LongType()), s.expressionTypes(identifier))
    }

    state.constrainType(identifier, NodeType(), NumberType()) match {
      case Left(_) => fail("Expected success")
      case Right(s) =>
        assertEquals(TypeSet(IntegerType(), LongType()), s.expressionTypes(identifier))
    }
  }

  @Test
  def shouldFailIfNoPossibleTypesRemainAfterConstrainingIdentifier() {
    val identifier = ast.Identifier("n", DummyToken(0,1))
    val state = SemanticState.clean.declareIdentifier(identifier, NumberType()).right.get

    state.constrainType(identifier, StringType()) match {
      case Right(s) => fail("Expected an error, but types are: " + s.expressionTypes(identifier))
      case Left(error) =>
        assertEquals(DummyToken(0,1), error.token)
        assertEquals(Seq(), error.references.toSeq)
        assertEquals("Type mismatch: n already defined with conflicting type Number (expected String)", error.msg)
    }

    state.constrainType(identifier, LongType()) match {
      case Right(s) => fail("Expected an error, but types are: " + s.expressionTypes(identifier))
      case Left(error) =>
        assertEquals(DummyToken(0,1), error.token)
        assertEquals(Seq(), error.references.toSeq)
        assertEquals("Type mismatch: n already defined with conflicting type Number (expected Long)", error.msg)
    }
  }

  @Test
  def shouldRecordTypeForExpressionWhenSpecifyingType() {
    val expression = DummyExpression(TypeSet(IntegerType(), StringType()), DummyToken(0,1))
    val state = SemanticState.clean.specifyType(expression, expression.possibleTypes).right.get
    assertEquals(expression.possibleTypes, state.expressionTypes(expression))
  }

  @Test
  def shouldConstrainTypeForExpression() {
    val expression = DummyExpression(TypeSet(IntegerType(), LongType(), StringType(), MapType()), DummyToken(0,1))
    val state = SemanticState.clean.specifyType(expression, expression.possibleTypes).right.get

    state.constrainType(expression, NumberType()) match {
      case Left(_) => fail("Expected success")
      case Right(s) =>
        assertEquals(TypeSet(IntegerType(), LongType()), s.expressionTypes(expression))
    }

    state.constrainType(expression, NodeType(), NumberType()) match {
      case Left(_) => fail("Expected success")
      case Right(s) =>
        assertEquals(TypeSet(IntegerType(), LongType()), s.expressionTypes(expression))
    }
  }

  @Test
  def shouldFailIfNoPossibleTypesRemainAfterConstrainingExpression() {
    val expression = DummyExpression(TypeSet(NumberType()), DummyToken(0,1))
    val state = SemanticState.clean.specifyType(expression, expression.possibleTypes).right.get

    state.constrainType(expression, StringType()) match {
      case Right(s) => fail("Expected an error, but types are: " + s.expressionTypes(expression))
      case Left(error) =>
        assertEquals(DummyToken(0,1), error.token)
        assertEquals(Seq(), error.references.toSeq)
        assertEquals("Type mismatch: expected String but was Number", error.msg)
    }

    state.constrainType(expression, LongType()) match {
      case Right(s) => fail("Expected an error, but types are: " + s.expressionTypes(expression))
      case Left(error) =>
        assertEquals(DummyToken(0,1), error.token)
        assertEquals(Seq(), error.references.toSeq)
        assertEquals("Type mismatch: expected Long but was Number", error.msg)
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
    assertEquals(TypeSet(NodeType()), s1.expressionTypes(identifier))
  }

  @Test
  def shouldReturnTypesOfIdentifierAtLaterExpression() {
    val identifier1 = ast.Identifier("foo", DummyToken(0, 1))
    val identifier2 = ast.Identifier("foo", DummyToken(3, 5))
    val s1 = SemanticState.clean.declareIdentifier(identifier1, NodeType()).right.get
    val s2 = s1.implicitIdentifier(identifier2, NodeType()).right.get
    assertEquals(TypeSet(NodeType()), s2.expressionTypes(identifier2))
  }

  @Test
  def shouldReturnTypesOfIdentifierAfterClear() {
    val identifier = ast.Identifier("foo", DummyToken(0, 1))
    val s1 = SemanticState.clean.declareIdentifier(identifier, NodeType()).right.get
    assertEquals(TypeSet(NodeType()), s1.clearSymbols.expressionTypes(identifier))
  }

  @Test
  def shouldReturnTypesOfLaterImplicitIdentifierAfterClear() {
    val identifier1 = ast.Identifier("foo", DummyToken(0, 1))
    val identifier2 = ast.Identifier("foo", DummyToken(3, 5))
    val s1 = SemanticState.clean.declareIdentifier(identifier1, NodeType()).right.get
    val s2 = s1.implicitIdentifier(identifier2, NodeType()).right.get
    assertEquals(TypeSet(NodeType()), s2.clearSymbols.expressionTypes(identifier2))
  }

  @Test
  def shouldReturnTypesOfLaterEnsuredIdentifierAfterClear() {
    val identifier1 = ast.Identifier("foo", DummyToken(0, 1))
    val identifier2 = ast.Identifier("foo", DummyToken(3, 5))
    val s1 = SemanticState.clean.declareIdentifier(identifier1, NodeType()).right.get
    val s2 = s1.ensureIdentifierDefined(identifier2).right.get
    assertEquals(TypeSet(NodeType()), s2.clearSymbols.expressionTypes(identifier2))
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
