/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
    val identifier1 = ast.Identifier("foo")(DummyToken(0,1))
    val identifier2 = ast.Identifier("foo")(DummyToken(3,6))
    val state = SemanticState.clean.declareIdentifier(identifier1, CTNode).right.get

    state.declareIdentifier(identifier2, CTNode) match {
      case Right(_) => fail("Expected an error from second declaration")
      case Left(error) =>
        assertEquals(identifier2.token, error.token)
        assertEquals(Set(identifier1.token), error.references)
    }
  }

  @Test
  def shouldCollectAllIdentifiersWhenImplicitlyDeclared() {
    val identifier1 = ast.Identifier("foo")(DummyToken(0,1))
    val identifier2 = ast.Identifier("foo")(DummyToken(2,3))
    val identifier3 = ast.Identifier("foo")(DummyToken(3,6))

    SemanticState.clean.implicitIdentifier(identifier1, CTNode) then
    ((_: SemanticState).implicitIdentifier(identifier2, CTNode)) then
    ((_: SemanticState).implicitIdentifier(identifier3, CTNode)) match {
      case Left(_) => fail("Expected success")
      case Right(state) =>
        val tokens = state.symbolTable.get("foo").map(_.tokens)
        assertEquals(Set(identifier1.token, identifier2.token, identifier3.token), tokens.get)
    }
  }

  @Test
  def shouldConstrainTypesForConsecutiveImplicitIdentifierDeclarations() {
    val identifier1 = ast.Identifier("foo")(DummyToken(0,1))
    val identifier2 = ast.Identifier("foo")(DummyToken(3,6))

    SemanticState.clean.implicitIdentifier(identifier1, CTNode | CTRelationship) then
    ((_: SemanticState).implicitIdentifier(identifier2, CTNode)) match {
      case Left(_) => fail("Expected success")
      case Right(state) =>
        val types = state.symbolTypes("foo")
        assertEquals(CTNode: TypeSpec, types)
    }

    SemanticState.clean.implicitIdentifier(identifier1, CTRelationship) then
    ((_: SemanticState).implicitIdentifier(identifier2, CTNode | CTRelationship)) match {
      case Left(_) => fail("Expected success")
      case Right(state) =>
        val types = state.symbolTypes("foo")
        assertEquals(CTRelationship: TypeSpec, types)
    }

    SemanticState.clean.implicitIdentifier(identifier1, CTNode | CTRelationship) then
    ((_: SemanticState).implicitIdentifier(identifier2, CTAny.covariant)) match {
      case Left(_) => fail("Expected success")
      case Right(state) =>
        val types = state.symbolTypes("foo")
        assertEquals(CTNode | CTRelationship, types)
    }

    SemanticState.clean.implicitIdentifier(identifier1, CTNode) then
    ((_: SemanticState).implicitIdentifier(identifier2, CTMap.covariant)) match {
      case Left(_) => fail("Expected success")
      case Right(state) =>
        val types = state.symbolTypes("foo")
        assertEquals(CTNode: TypeSpec, types)
    }
  }

  @Test
  def shouldFailIfNoPossibleTypesRemainAfterImplicitIdentifierDeclaration() {
    SemanticState.clean.implicitIdentifier(ast.Identifier("foo")(DummyToken(0,1)), CTMap) then
      ((_: SemanticState).implicitIdentifier(ast.Identifier("foo")(DummyToken(3,6)), CTNode)) match {
      case Right(_) => fail("Expected an error")
      case Left(error) =>
        assertEquals(DummyToken(3,6), error.token)
        assertEquals(Seq(DummyToken(0,1)), error.references.toSeq)
        assertEquals("Type mismatch: foo already defined with conflicting type Map (expected Node)", error.msg)
    }

    SemanticState.clean.implicitIdentifier(ast.Identifier("foo")(DummyToken(0,1)), CTNode | CTRelationship) then
    ((_: SemanticState).implicitIdentifier(ast.Identifier("foo")(DummyToken(3,6)), CTNode | CTInteger)) then
    ((_: SemanticState).implicitIdentifier(ast.Identifier("foo")(DummyToken(9,12)), CTInteger | CTRelationship)) match {
      case Right(_) => fail("Expected an error")
      case Left(error) =>
        assertEquals(DummyToken(9,12), error.token)
        assertEquals(Seq(DummyToken(0,1), DummyToken(3,6)), error.references.toSeq)
        assertEquals("Type mismatch: foo already defined with conflicting type Node (expected Integer or Relationship)", error.msg)
    }
  }

  @Test
  def shouldRecordTypeForExpressionWhenSpecifyingType() {
    val expression = DummyExpression(CTInteger | CTString)
    val state = SemanticState.clean.specifyType(expression, expression.possibleTypes).right.get
    assertEquals(expression.possibleTypes, state.expressionType(expression).specified)
    assertEquals(expression.possibleTypes, state.expressionType(expression).actual)
  }

  @Test
  def shouldExpectTypeForExpression() {
    val expression = DummyExpression(CTInteger | CTString | CTMap)
    val state = SemanticState.clean.specifyType(expression, expression.possibleTypes).right.get

    state.expectType(expression, CTNumber.covariant) match {
      case (s, typ) =>
        assertEquals(CTInteger: TypeSpec, typ)
        assertEquals(typ, s.expressionType(expression).actual)
    }

    state.expectType(expression, CTNode.covariant | CTNumber.covariant) match {
      case (s, typ) =>
        assertEquals(CTInteger: TypeSpec, typ)
        assertEquals(typ, s.expressionType(expression).actual)
    }
  }

  @Test
  def shouldFindSymbolInParent() {
    val s1 = SemanticState.clean.declareIdentifier(ast.Identifier("foo")(DummyToken(0, 1)), CTNode).right.get
    val s2 = s1.newScope
    assertEquals(CTNode: TypeSpec, s2.symbolTypes("foo"))
  }

  @Test
  def shouldOverrideSymbolInParent() {
    val s1 = SemanticState.clean.declareIdentifier(ast.Identifier("foo")(DummyToken(0, 1)), CTNode).right.get
    val s2 = s1.newScope.declareIdentifier(ast.Identifier("foo")(DummyToken(0, 1)), CTString).right.get
    assertEquals(CTString: TypeSpec, s2.symbolTypes("foo"))
  }

  @Test
  def shouldExtendSymbolInParent() {
    val s1 = SemanticState.clean.declareIdentifier(ast.Identifier("foo")(DummyToken(0, 1)), CTNode).right.get
    val s2 = s1.newScope.implicitIdentifier(ast.Identifier("foo")(DummyToken(0, 1)), CTAny.covariant).right.get
    assertEquals(CTNode: TypeSpec, s2.symbolTypes("foo"))
  }

  @Test
  def shouldReturnTypesOfIdentifier() {
    val identifier = ast.Identifier("foo")(DummyToken(0, 1))
    val s1 = SemanticState.clean.declareIdentifier(identifier, CTNode).right.get
    assertEquals(CTNode: TypeSpec, s1.expressionType(identifier).actual)
  }

  @Test
  def shouldReturnTypesOfIdentifierAtLaterExpression() {
    val identifier1 = ast.Identifier("foo")(DummyToken(0, 1))
    val identifier2 = ast.Identifier("foo")(DummyToken(3, 5))
    val s1 = SemanticState.clean.declareIdentifier(identifier1, CTNode).right.get
    val s2 = s1.implicitIdentifier(identifier2, CTNode).right.get
    assertEquals(CTNode: TypeSpec, s2.expressionType(identifier2).actual)
  }

  @Test
  def shouldReturnTypesOfIdentifierAfterClear() {
    val identifier = ast.Identifier("foo")(DummyToken(0, 1))
    val s1 = SemanticState.clean.declareIdentifier(identifier, CTNode).right.get
    assertEquals(CTNode: TypeSpec, s1.clearSymbols.expressionType(identifier).actual)
  }

  @Test
  def shouldReturnTypesOfLaterImplicitIdentifierAfterClear() {
    val identifier1 = ast.Identifier("foo")(DummyToken(0, 1))
    val identifier2 = ast.Identifier("foo")(DummyToken(3, 5))
    val s1 = SemanticState.clean.declareIdentifier(identifier1, CTNode).right.get
    val s2 = s1.implicitIdentifier(identifier2, CTNode).right.get
    assertEquals(CTNode: TypeSpec, s2.clearSymbols.expressionType(identifier2).actual)
  }

  @Test
  def shouldReturnTypesOfLaterEnsuredIdentifierAfterClear() {
    val identifier1 = ast.Identifier("foo")(DummyToken(0, 1))
    val identifier2 = ast.Identifier("foo")(DummyToken(3, 5))
    val s1 = SemanticState.clean.declareIdentifier(identifier1, CTNode).right.get
    val s2 = s1.ensureIdentifierDefined(identifier2).right.get
    assertEquals(CTNode: TypeSpec, s2.clearSymbols.expressionType(identifier2).actual)
  }

  @Test
  def shouldNotReturnSymbolOfIdentifierAfterClear() {
    val s1 = SemanticState.clean.declareIdentifier(ast.Identifier("foo")(DummyToken(0, 1)), CTNode).right.get
    assertEquals(None, s1.clearSymbols.symbol("foo"))
    assertEquals(TypeSpec.all, s1.clearSymbols.symbolTypes("foo"))
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
