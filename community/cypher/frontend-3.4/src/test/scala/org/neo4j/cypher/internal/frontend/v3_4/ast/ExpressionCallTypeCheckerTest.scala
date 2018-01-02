/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_4.ast

import org.neo4j.cypher.internal.util.v3_4.InputPosition
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_4.semantics.{SemanticCheckResult, SemanticExpressionCheck, SemanticState}
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.v3_4.expressions.{DummyExpression, Expression, TypeSignature}

class ExpressionCallTypeCheckerTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should accept a specified type") {
    typeCheckSuccess(Seq(TypeSignature(Vector(CTInteger), CTInteger)), Seq(CTInteger), CTInteger)
    typeCheckSuccess(Seq(TypeSignature(Vector(CTInteger), CTString)), Seq(CTInteger), CTString)
    typeCheckSuccess(Seq(TypeSignature(Vector(CTInteger), CTString),
                         TypeSignature(Vector(CTFloat), CTBoolean)), Seq(CTNumber.covariant), CTBoolean | CTString)
  }

  test("any type") {
    typeCheckSuccess(Seq(TypeSignature(Vector(CTInteger), CTBoolean),
                         TypeSignature(Vector(CTFloat), CTFloat)), Seq(CTAny.covariant), CTBoolean | CTFloat)
  }

  test("two ExpressionSignatures") {
    val sig = Seq(TypeSignature(Vector(CTString), CTInteger),
                  TypeSignature(Vector(CTNumber), CTInteger))
    typeCheckSuccess(sig, Seq(CTAny.covariant), CTInteger)
    typeCheckSuccess(sig, Seq(CTNumber.covariant), CTInteger)
    typeCheckSuccess(sig, Seq(CTFloat), CTInteger)
    typeCheckSuccess(sig, Seq(CTInteger), CTInteger)
    typeCheckSuccess(sig, Seq(CTString), CTInteger)
  }

  test("fail on mismatch with ExpressionSignature") {
    typeCheckFail(Seq(TypeSignature(Vector(CTBoolean), CTRelationship)), Seq(CTNode)) { errs =>
      errs should contain("Type mismatch: expected Boolean but was Node")
    }
    typeCheckFail(Seq(TypeSignature(Vector(CTBoolean), CTRelationship),
                      TypeSignature(Vector(CTString), CTRelationship),
                      TypeSignature(Vector(CTMap), CTRelationship)), Seq(CTNumber)) { errs =>
      errs should contain("Type mismatch: expected Boolean, Map, Node, Relationship or String but was Number")
    }

    typeCheckFail(Seq(TypeSignature(Vector(CTBoolean, CTNode, CTInteger), CTRelationship)), Seq(CTBoolean, CTNode, CTFloat)) { errs =>
      errs should contain("Type mismatch: expected Integer but was Float")
    }
  }

  test("should pick the most specific ExpressionSignature of many applicable maps") {
    val identityExpressionSignature = Seq(TypeSignature(Vector(CTMap), CTMap),
                                          TypeSignature(Vector(CTRelationship), CTRelationship),
                                          TypeSignature(Vector(CTNode), CTNode))
    typeCheckSuccess(identityExpressionSignature, Seq(CTAny.covariant), CTMap | CTNode | CTRelationship)
    typeCheckSuccess(identityExpressionSignature, Seq(CTMap.invariant), CTMap)
    typeCheckSuccess(identityExpressionSignature, Seq(CTMap.covariant), CTMap | CTNode | CTRelationship)
    typeCheckSuccess(identityExpressionSignature, Seq(CTNode), CTMap | CTNode)
    typeCheckSuccess(identityExpressionSignature, Seq(CTRelationship), CTMap | CTRelationship)
  }

  test("should pick the most specific ExpressionSignature of many applicable numbers") {
    val identityExpressionSignature = Seq(TypeSignature(Vector(CTInteger), CTInteger),
                                          TypeSignature(Vector(CTFloat), CTFloat))
    typeCheckSuccess(identityExpressionSignature, Seq(CTAny.covariant), CTInteger | CTFloat)
    typeCheckSuccess(identityExpressionSignature, Seq(CTNumber.covariant), CTInteger | CTFloat)
    typeCheckSuccess(identityExpressionSignature, Seq(CTInteger), CTInteger)
    typeCheckSuccess(identityExpressionSignature, Seq(CTFloat), CTFloat)
  }

  test("should handle combined typespecs") {
    val ExpressionSignatures = Seq(TypeSignature(Vector(CTInteger, CTInteger), CTInteger),
                                   TypeSignature(Vector(CTNumber, CTNumber), CTFloat))
    typeCheckSuccess(ExpressionSignatures, Seq(CTInteger, CTInteger), CTFloat | CTInteger)
  }

  test("pretty print") {
    TypeSpec.formatArguments(Seq(CTNumber, CTBoolean, CTString)) should equal("(Number, Boolean, String)")
  }

  private def typeCheck(ExpressionSignatures: Seq[TypeSignature], arguments: Seq[TypeSpec]): (TypeExpr, SemanticCheckResult) = {
    val argExpressions = arguments.map(DummyExpression(_))
    val semanticState = argExpressions.foldLeft(SemanticState.clean) {
      case (state, inner) => state.specifyType(inner, inner.possibleTypes).right.get
    }
    val expr = TypeExpr(argExpressions)
    val check = SemanticExpressionCheck.checkTypes(expr, ExpressionSignatures)(semanticState)
    (expr, check)
  }

  private def typeCheckSuccess(ExpressionSignatures: Seq[TypeSignature], arguments: Seq[TypeSpec], spec: TypeSpec) = {
    val (expr, check) = typeCheck(ExpressionSignatures, arguments)
    check.errors shouldBe empty
    check.state.typeTable.get(expr).map(_.specified) should equal(Some(spec))
  }

  private def typeCheckFail(ExpressionSignatures: Seq[TypeSignature], arguments: Seq[TypeSpec])(checkError: Seq[String] => Unit) = {
    val (_, check) = typeCheck(ExpressionSignatures, arguments)
    checkError(check.errors.map(_.msg.replaceAll("\\s+", " ")))
  }

  case class TypeExpr(override val arguments: Seq[Expression]) extends Expression {
    override def position: InputPosition = pos
  }
}
