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
package org.neo4j.cypher.internal.compiler.v2_0.ast

import org.neo4j.cypher.internal.compiler.v2_0._
import symbols._
import org.neo4j.helpers.ThisShouldNotHappenError

object Expression {
  sealed trait SemanticContext
  object SemanticContext {
    case object Simple extends SemanticContext
    case object Results extends SemanticContext
  }

  implicit class SemanticCheckableOption[A <: Expression](option: Option[A]) {
    def semanticCheck(ctx: SemanticContext): SemanticCheck =
      option.fold(SemanticCheckResult.success) { _.semanticCheck(ctx) }

    def expectType(possibleTypes: => TypeSpec): SemanticCheck =
      option.fold(SemanticCheckResult.success) { _.expectType(possibleTypes) }
  }

  implicit class SemanticCheckableExpressionTraversable[A <: Expression](traversable: TraversableOnce[A]) extends SemanticChecking {
    def semanticCheck(ctx: SemanticContext): SemanticCheck =
      traversable.foldSemanticCheck { _.semanticCheck(ctx) }
  }

  implicit class InferrableTypeTraversableOnce[A <: Expression](traversable: TraversableOnce[A]) {
    def mergeUpTypes: TypeGenerator =
      if (traversable.isEmpty)
        _ => CTAny.invariant
      else
        (state: SemanticState) => traversable.map { _.types(state) } reduce { _ mergeUp _ }

    def expectType(possibleTypes: => TypeSpec): SemanticCheck =
      traversable.foldSemanticCheck { _.expectType(possibleTypes) }
  }
}

import Expression._

abstract class Expression extends AstNode with SemanticChecking {
  def semanticCheck(ctx: SemanticContext): SemanticCheck

  def types: TypeGenerator = s => s.expressionType(this).actual

  def specifyType(typeGen: TypeGenerator): SemanticState => Either[SemanticError, SemanticState] =
    s => specifyType(typeGen(s))(s)
  def specifyType(possibleTypes: => TypeSpec): SemanticState => Either[SemanticError, SemanticState] =
    _.specifyType(this, possibleTypes)

  def expectType(typeGen: TypeGenerator): SemanticState => SemanticCheckResult =
    s => expectType(typeGen(s))(s)
  def expectType(possibleTypes: => TypeSpec): SemanticState => SemanticCheckResult = s => {
    s.expectType(this, possibleTypes) match {
      case (ss, TypeSpec.none) =>
        val existingTypesString = ss.expressionType(this).specified.mkString(", ", " or ")
        val expectedTypesString = possibleTypes.mkString(", ", " or ")
        SemanticCheckResult.error(ss, SemanticError(s"Type mismatch: expected $expectedTypesString but was $existingTypesString", this.token))
      case (ss, _)             =>
        SemanticCheckResult.success(ss)
    }
  }
}

trait SimpleTypedExpression { self: Expression =>
  protected def possibleTypes: TypeSpec
  def semanticCheck(ctx: SemanticContext): SemanticCheck = specifyType(possibleTypes)
}


case class Identifier(name: String)(val token: InputToken) extends Expression {
  // check the identifier is defined and, if not, define it so that later errors are suppressed
  def semanticCheck(ctx: SemanticContext) = s => this.ensureDefined()(s) match {
    case Right(ss) => SemanticCheckResult.success(ss)
    case Left(error) => SemanticCheckResult.error(declare(CTAny.covariant)(s).right.get, error)
  }

  // double-dispatch helpers
  def declare(possibleTypes: TypeSpec) =
    (_: SemanticState).declareIdentifier(this, possibleTypes)
  def declare(typeGen: SemanticState => TypeSpec) =
    (s: SemanticState) => s.declareIdentifier(this, typeGen(s))
  def implicitDeclaration(possibleType: CypherType) =
    (_: SemanticState).implicitIdentifier(this, possibleType)
  def ensureDefined() =
    (_: SemanticState).ensureIdentifierDefined(this)
}

case class Parameter(name: String)(val token: InputToken) extends Expression with SimpleTypedExpression {
  protected def possibleTypes = CTAny.covariant
}

case class Null()(val token: InputToken) extends Expression with SimpleTypedExpression {
  protected def possibleTypes = CTAny.covariant
}

case class True()(val token: InputToken) extends Expression with SimpleTypedExpression {
  protected def possibleTypes = CTBoolean
}

case class False()(val token: InputToken) extends Expression with SimpleTypedExpression {
  protected def possibleTypes = CTBoolean
}

case class CountStar()(val token: InputToken) extends Expression with SimpleTypedExpression {
  protected def possibleTypes = CTInteger
}

case class Property(map: Expression, identifier: Identifier)(val token: InputToken)
  extends Expression with SimpleTypedExpression {

  protected def possibleTypes = CTAny.covariant

  override def semanticCheck(ctx: SemanticContext) =
    map.semanticCheck(ctx) then
    map.expectType(CTMap.covariant) then
    super.semanticCheck(ctx)
}

object LegacyProperty {
  def apply(map: Expression, identifier: Identifier, legacyOperator: String)(token: InputToken) =
    new Property(map, identifier)(token) {
      override def semanticCheck(ctx: SemanticContext): SemanticCheck = legacyOperator match {
        case "?" => SemanticError(s"This syntax is no longer supported (missing properties are now returned as null). Please use (not(has(<ident>.${identifier.name})) OR <ident>.${identifier.name}=<value>) if you really need the old behavior.", token)
        case "!" => SemanticError(s"This syntax is no longer supported (missing properties are now returned as null).", token)
        case _   => throw new ThisShouldNotHappenError("Stefan", s"Invalid legacy operator $legacyOperator following access to property.")
      }
    }
}

case class PatternExpression(pattern: RelationshipsPattern) extends Expression with SimpleTypedExpression {
  def token = pattern.token
  protected def possibleTypes = CTCollection(CTPath)

  override def semanticCheck(ctx: SemanticContext) =
    pattern.semanticCheck(Pattern.SemanticContext.Expression) then
    super.semanticCheck(ctx)
}

case class HasLabels(expression: Expression, labels: Seq[Identifier])(val token: InputToken) extends Expression with SimpleTypedExpression {
  protected def possibleTypes = CTBoolean

  override def semanticCheck(ctx: SemanticContext) =
    expression.semanticCheck(ctx) then
    expression.expectType(CTNode.covariant) then
    super.semanticCheck(ctx)
}

case class Collection(expressions: Seq[Expression])(val token: InputToken) extends Expression {
  def semanticCheck(ctx: SemanticContext) = expressions.semanticCheck(ctx) then specifyType(possibleTypes)

  private def possibleTypes: TypeGenerator = state => expressions match {
    case Seq() => CTCollection(CTAny).invariant
    case _     => expressions.mergeUpTypes(state).wrapInCollection
  }
}

case class MapExpression(items: Seq[(Identifier, Expression)])(val token: InputToken) extends Expression with SimpleTypedExpression {
  protected def possibleTypes = CTMap

  override def semanticCheck(ctx: SemanticContext) =
    items.map(_._2).semanticCheck(ctx) then
    super.semanticCheck(ctx)
}

case class CollectionSlice(collection: Expression, from: Option[Expression], to: Option[Expression])(val token: InputToken)
  extends Expression {

  override def semanticCheck(ctx: SemanticContext) =
    collection.semanticCheck(ctx) then
    collection.expectType(CTCollection(CTAny).covariant) then
    when(from.isEmpty && to.isEmpty) {
      SemanticError("The start or end (or both) is required for a collection slice", token)
    } then
    from.semanticCheck(ctx) then
    from.expectType(CTInteger.covariant) then
    to.semanticCheck(ctx) then
    to.expectType(CTInteger.covariant) then
    specifyType(collection.types)
}

case class CollectionIndex(collection: Expression, idx: Expression)(val token: InputToken)
  extends Expression {

  override def semanticCheck(ctx: SemanticContext) =
    collection.semanticCheck(ctx) then
    collection.expectType(CTCollection(CTAny).covariant) then
    idx.semanticCheck(ctx) then
    idx.expectType(CTInteger.covariant) then
    specifyType(collection.types(_).unwrapCollections)
}
