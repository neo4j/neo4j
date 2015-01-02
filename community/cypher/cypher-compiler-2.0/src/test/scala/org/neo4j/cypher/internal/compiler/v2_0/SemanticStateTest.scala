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
package org.neo4j.cypher.internal.compiler.v2_0

import symbols._
import org.neo4j.cypher.internal.commons.CypherFunSuite

class SemanticStateTest extends CypherFunSuite {

  test("should declare identifier once") {
    val identifier1 = ast.Identifier("foo")(DummyPosition(0))
    val identifier2 = ast.Identifier("foo")(DummyPosition(3))
    val state = SemanticState.clean.declareIdentifier(identifier1, CTNode).right.get

    state.declareIdentifier(identifier2, CTNode) match {
      case Right(_) => fail("Expected an error from second declaration")
      case Left(error) =>
        error.position should equal(identifier2.position)
        error.references should equal(Seq(identifier1.position))
    }
  }

  test("should collect all identifiers when implicitly declared") {
    val identifier1 = ast.Identifier("foo")(DummyPosition(0))
    val identifier2 = ast.Identifier("foo")(DummyPosition(2))
    val identifier3 = ast.Identifier("foo")(DummyPosition(3))

    SemanticState.clean.implicitIdentifier(identifier1, CTNode) then
    ((_: SemanticState).implicitIdentifier(identifier2, CTNode)) then
    ((_: SemanticState).implicitIdentifier(identifier3, CTNode)) match {
      case Left(_) => fail("Expected success")
      case Right(state) =>
        val positions = state.symbolTable.get("foo").map(_.positions).get
        positions should equal(Seq(identifier1.position, identifier2.position, identifier3.position))
    }
  }

  test("should constrain types for consecutive implicit identifier declarations") {
    val identifier1 = ast.Identifier("foo")(DummyPosition(0))
    val identifier2 = ast.Identifier("foo")(DummyPosition(3))

    SemanticState.clean.implicitIdentifier(identifier1, CTNode | CTRelationship) then
    ((_: SemanticState).implicitIdentifier(identifier2, CTNode)) match {
      case Left(_) => fail("Expected success")
      case Right(state) =>
        val types = state.symbolTypes("foo")
        types should equal(CTNode: TypeSpec)
    }

    SemanticState.clean.implicitIdentifier(identifier1, CTRelationship) then
    ((_: SemanticState).implicitIdentifier(identifier2, CTNode | CTRelationship)) match {
      case Left(_) => fail("Expected success")
      case Right(state) =>
        val types = state.symbolTypes("foo")
        types should equal(CTRelationship: TypeSpec)
    }

    SemanticState.clean.implicitIdentifier(identifier1, CTNode | CTRelationship) then
    ((_: SemanticState).implicitIdentifier(identifier2, CTAny.covariant)) match {
      case Left(_) => fail("Expected success")
      case Right(state) =>
        val types = state.symbolTypes("foo")
        types should equal(CTNode | CTRelationship)
    }

    SemanticState.clean.implicitIdentifier(identifier1, CTNode) then
    ((_: SemanticState).implicitIdentifier(identifier2, CTMap.covariant)) match {
      case Left(_) => fail("Expected success")
      case Right(state) =>
        val types = state.symbolTypes("foo")
        types should equal(CTNode: TypeSpec)
    }
  }

  test("should fail if no possible types remain after implicit identifier declaration") {
    SemanticState.clean.implicitIdentifier(ast.Identifier("foo")(DummyPosition(0)), CTMap) then
      ((_: SemanticState).implicitIdentifier(ast.Identifier("foo")(DummyPosition(3)), CTNode)) match {
      case Right(_) => fail("Expected an error")
      case Left(error) =>
        error.position should equal(DummyPosition(3))
        error.references should be (Seq(DummyPosition(0)))
        error.msg should equal("Type mismatch: foo already defined with conflicting type Map (expected Node)")
    }

    SemanticState.clean.implicitIdentifier(ast.Identifier("foo")(DummyPosition(0)), CTNode | CTRelationship) then
    ((_: SemanticState).implicitIdentifier(ast.Identifier("foo")(DummyPosition(3)), CTNode | CTInteger)) then
    ((_: SemanticState).implicitIdentifier(ast.Identifier("foo")(DummyPosition(9)), CTInteger | CTRelationship)) match {
      case Right(_) => fail("Expected an error")
      case Left(error) =>
        error.position should equal(DummyPosition(9))
        error.references should equal(Seq(DummyPosition(0), DummyPosition(3)))
        error.msg should equal("Type mismatch: foo already defined with conflicting type Node (expected Integer or Relationship)")
    }
  }

  test("should record type for expression when specifying type") {
    val expression = DummyExpression(CTInteger | CTString)
    val state = SemanticState.clean.specifyType(expression, expression.possibleTypes).right.get
    state.expressionType(expression).specified should equal(expression.possibleTypes)
    state.expressionType(expression).actual should equal(expression.possibleTypes)
  }

  test("should expect type for expression") {
    val expression = DummyExpression(CTInteger | CTString | CTMap)
    val state = SemanticState.clean.specifyType(expression, expression.possibleTypes).right.get

    state.expectType(expression, CTNumber.covariant) match {
      case (s, typ) =>
        typ should equal(CTInteger: TypeSpec)
        s.expressionType(expression).actual should equal(typ)
    }

    state.expectType(expression, CTNode.covariant | CTNumber.covariant) match {
      case (s, typ) =>
        typ should equal(CTInteger: TypeSpec)
        s.expressionType(expression).actual should equal(typ)
    }
  }

  test("should find symbol in parent") {
    val s1 = SemanticState.clean.declareIdentifier(ast.Identifier("foo")(DummyPosition(0)), CTNode).right.get
    val s2 = s1.newScope
    s2.symbolTypes("foo") should equal(CTNode: TypeSpec)
  }

  test("should override symbol in parent") {
    val s1 = SemanticState.clean.declareIdentifier(ast.Identifier("foo")(DummyPosition(0)), CTNode).right.get
    val s2 = s1.newScope.declareIdentifier(ast.Identifier("foo")(DummyPosition(0)), CTString).right.get

    s2.symbolTypes("foo") should equal(CTString: TypeSpec)
  }

  test("should extend symbol in parent") {
    val s1 = SemanticState.clean.declareIdentifier(ast.Identifier("foo")(DummyPosition(0)), CTNode).right.get
    val s2 = s1.newScope.implicitIdentifier(ast.Identifier("foo")(DummyPosition(0)), CTAny.covariant).right.get
    s2.symbolTypes("foo") should equal(CTNode: TypeSpec)
  }

  test("should return types of identifier") {
    val identifier = ast.Identifier("foo")(DummyPosition(0))
    val s1 = SemanticState.clean.declareIdentifier(identifier, CTNode).right.get
    s1.expressionType(identifier).actual should equal(CTNode: TypeSpec)
  }

  test("should return types of identifier at later expression") {
    val identifier1 = ast.Identifier("foo")(DummyPosition(0))
    val identifier2 = ast.Identifier("foo")(DummyPosition(3))
    val s1 = SemanticState.clean.declareIdentifier(identifier1, CTNode).right.get
    val s2 = s1.implicitIdentifier(identifier2, CTNode).right.get
    s2.expressionType(identifier2).actual should equal(CTNode: TypeSpec)
  }

  test("should return types of identifier after clear") {
    val identifier = ast.Identifier("foo")(DummyPosition(0))
    val s1 = SemanticState.clean.declareIdentifier(identifier, CTNode).right.get
    s1.clearSymbols.expressionType(identifier).actual should equal(CTNode: TypeSpec)
  }

  test("should return types of later implicit identifier after clear") {
    val identifier1 = ast.Identifier("foo")(DummyPosition(0))
    val identifier2 = ast.Identifier("foo")(DummyPosition(3))
    val s1 = SemanticState.clean.declareIdentifier(identifier1, CTNode).right.get
    val s2 = s1.implicitIdentifier(identifier2, CTNode).right.get
    s2.clearSymbols.expressionType(identifier2).actual should equal(CTNode: TypeSpec)
  }

  test("should return types of later ensured identifier after clear") {
    val identifier1 = ast.Identifier("foo")(DummyPosition(0))
    val identifier2 = ast.Identifier("foo")(DummyPosition(3))
    val s1 = SemanticState.clean.declareIdentifier(identifier1, CTNode).right.get
    val s2 = s1.ensureIdentifierDefined(identifier2).right.get
    s2.clearSymbols.expressionType(identifier2).actual should equal(CTNode: TypeSpec)
  }

  test("should not return symbol of identifier after clear") {
    val s1 = SemanticState.clean.declareIdentifier(ast.Identifier("foo")(DummyPosition(0)), CTNode).right.get
    s1.clearSymbols.symbol("foo") should equal(None)
    s1.clearSymbols.symbolTypes("foo") should equal(TypeSpec.all)
  }

  test("should maintain separate TypeInfo for equivalent expressions") {
    val exp1 = ast.Property(ast.Identifier("n")(DummyPosition(0)), ast.Identifier("prop")(DummyPosition(3)))(DummyPosition(0))
    val exp2 = ast.Property(ast.Identifier("n")(DummyPosition(6)), ast.Identifier("prop")(DummyPosition(9)))(DummyPosition(6))
    val s1 = SemanticState.clean.specifyType(exp1, CTNode).right.get
    val s2 = s1.specifyType(exp2, CTRelationship).right.get

    s2.expressionType(exp1).specified should equal(CTNode: TypeSpec)
    s2.expressionType(exp2).specified should equal(CTRelationship: TypeSpec)

    val s3 = s2.expectType(exp1, CTMap)._1.expectType(exp2, CTAny)._1
    s3.expressionType(exp1).expected should equal(Some(CTMap: TypeSpec))
    s3.expressionType(exp2).expected should equal(Some(CTAny: TypeSpec))
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
