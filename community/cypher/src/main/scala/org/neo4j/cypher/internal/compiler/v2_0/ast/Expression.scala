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
package org.neo4j.cypher.internal.compiler.v2_0.ast

import org.neo4j.cypher.internal.compiler.v2_0._
import org.neo4j.cypher.internal.compiler.v2_0.symbols._
import org.neo4j.cypher.internal.compiler.v2_0.commands
import org.neo4j.cypher.internal.compiler.v2_0.commands.{expressions => commandexpressions, values => commandvalues}
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.{Expression => CommandExpression}
import org.neo4j.cypher.internal.compiler.v2_0.commands.values.TokenType.PropertyKey
import org.neo4j.helpers.ThisShouldNotHappenError

object Expression {
  sealed trait SemanticContext
  object SemanticContext {
    case object Simple extends SemanticContext
    case object Results extends SemanticContext
  }

  implicit class SemanticCheckableOption[A <: Expression](option: Option[A]) {
    def semanticCheck(ctx: SemanticContext) : SemanticCheck =
      option.fold(SemanticCheckResult.success) { _.semanticCheck(ctx) }

    def constrainType(possibleType: CypherType, possibleTypes: CypherType*) : SemanticCheck =
      constrainType((possibleType +: possibleTypes).toSet)
    def constrainType(possibleTypes: => TypeSet) : SemanticCheck =
      option.fold(SemanticCheckResult.success) { _.constrainType(possibleTypes) }
  }

  implicit class SemanticCheckableExpressionTraversable[A <: Expression](traversable: TraversableOnce[A]) extends SemanticChecking {
    def semanticCheck(ctx: SemanticContext) : SemanticCheck =
      traversable.foldLeft(SemanticCheckResult.success) { (f, o) => f then o.semanticCheck(ctx) }
  }

  implicit class InferrableTypeTraversableOnce[A <: Expression](traversable: TraversableOnce[A]) {
    def mergeDownTypes : TypeGenerator =
      (state: SemanticState) => traversable.map { _.types(state) } reduce { _ mergeDown _ }

    def constrainType(possibleType: CypherType, possibleTypes: CypherType*) : SemanticCheck =
      traversable.foldLeft(SemanticCheckResult.success) {
        (f, e) => f then e.constrainType(possibleType, possibleTypes:_*)
      }
  }
}

import Expression._

abstract class Expression extends AstNode with SemanticChecking {
  def semanticCheck(ctx: SemanticContext): SemanticCheck

  // double-dispatch helpers
  final def types: TypeGenerator = s => s.expressionTypes(this)
  final def specifyType(possibleType: CypherType, possibleTypes: CypherType*): SemanticState => Either[SemanticError, SemanticState] =
    specifyType((possibleType +: possibleTypes).toSet)
  final def specifyType(typeGen: TypeGenerator): SemanticState => Either[SemanticError, SemanticState] =
    s => s.specifyType(this, typeGen(s))
  final def specifyType(possibleTypes: => TypeSet): SemanticState => Either[SemanticError, SemanticState] =
    _.specifyType(this, possibleTypes)
  final def constrainType(possibleType: CypherType, possibleTypes: CypherType*): SemanticState => Either[SemanticError, SemanticState] =
    constrainType((possibleType +: possibleTypes).toSet)
  final def constrainType(typeGen: TypeGenerator): SemanticState => Either[SemanticError, SemanticState] =
    s => s.constrainType(this, token, typeGen(s))
  final def constrainType(possibleTypes: => TypeSet): SemanticState => Either[SemanticError, SemanticState] =
    _.constrainType(this, token, possibleTypes)

  def toCommand: CommandExpression
}

trait SimpleTypedExpression { self: Expression =>
  protected def possibleTypes : TypeSet
  def semanticCheck(ctx: SemanticContext) : SemanticCheck = specifyType(possibleTypes)
}

case class Identifier(name: String, token: InputToken) extends Expression {
  // check the identifier is defined and, if not, define it so that later errors are suppressed
  def semanticCheck(ctx: SemanticContext) = s => this.ensureDefined()(s) match {
    case Right(ss) => SemanticCheckResult.success(ss)
    case Left(error) => SemanticCheckResult.error(declare(AnyType())(s).right.get, error)
  }

  // double-dispatch helpers
  final def declare(possibleTypes: TypeSet) =
      (_: SemanticState).declareIdentifier(this, possibleTypes)
  final def declare(possibleType: CypherType, possibleTypes: CypherType*) =
      (_: SemanticState).declareIdentifier(this, possibleType, possibleTypes:_*)
  final def declare(typeGen: SemanticState => TypeSet) =
      (s: SemanticState) => s.declareIdentifier(this, typeGen(s))
  final def implicitDeclaration(possibleType: CypherType, possibleTypes: CypherType*) =
      (_: SemanticState).implicitIdentifier(this, possibleType, possibleTypes:_*)
  final def ensureDefined() =
      (_: SemanticState).ensureIdentifierDefined(this)

  def toCommand = commands.expressions.Identifier(name)
}

case class Parameter(name: String, token: InputToken) extends Expression with SimpleTypedExpression {
  protected def possibleTypes = Set(BooleanType(), MapType(), NumberType(), StringType(), CollectionType(AnyType()))

  def toCommand = commandexpressions.ParameterExpression(name)
}

case class Null(token: InputToken) extends Expression with SimpleTypedExpression {
  protected def possibleTypes = Set(AnyType())

  def toCommand = commandexpressions.Literal(null)
}

case class True(token: InputToken) extends Expression with SimpleTypedExpression {
  protected def possibleTypes = Set(BooleanType())

  def toCommand = commands.True()
}

case class False(token: InputToken) extends Expression with SimpleTypedExpression {
  protected def possibleTypes = Set(BooleanType())

  def toCommand = commands.Not(commands.True())
}

case class CountStar(token: InputToken) extends Expression with SimpleTypedExpression {
  protected def possibleTypes = Set(LongType())

  def toCommand = commandexpressions.CountStar()
}

case class Property(map: Expression, identifier: Identifier, token: InputToken)
  extends Expression with SimpleTypedExpression {

  protected def possibleTypes = Set(BooleanType(), NumberType(), StringType(), CollectionType(AnyType()))

  override def semanticCheck(ctx: SemanticContext) =
    map.semanticCheck(ctx) then
    map.constrainType(MapType()) then
    super.semanticCheck(ctx)

  def toCommand = commands.expressions.Property(map.toCommand, PropertyKey(identifier.name))
}

object LegacyProperty {
  // use of val instead of def due to inability to refer to methods on objects as partially applied functions
  val make = (map: Expression, identifier: Identifier, legacyOperator: String, token: InputToken) =>
    new Property(map, identifier, token) {
      override def semanticCheck(ctx: SemanticContext) : SemanticCheck = legacyOperator match {
        case "?" => SemanticError(s"This syntax is no longer supported (missing properties are now returned as null). Please use (not(has(<ident>.${identifier.name})) OR <ident>.${identifier.name}=<value>) if you really need the old behavior.", token)
        case "!" => SemanticError(s"This syntax is no longer supported (missing properties are now returned as null).", token)
        case _   => throw new ThisShouldNotHappenError("Stefan", s"Invalid legacy operator $legacyOperator following access to property.")
      }

      override def toCommand = throw new UnsupportedOperationException
    }
}

case class PatternExpression(pattern: RelationshipsPattern) extends Expression with SimpleTypedExpression {
  def token = pattern.token
  protected def possibleTypes = Set(CollectionType(PathType()), BooleanType())

  override def semanticCheck(ctx: SemanticContext) =
    pattern.semanticCheck(Pattern.SemanticContext.Expression) then super.semanticCheck(ctx)

  def toCommand = commands.PatternPredicate(pattern.toLegacyPatterns)
}

case class HasLabels(expression: Expression, labels: Seq[Identifier], token: InputToken) extends Expression with SimpleTypedExpression {
  protected def possibleTypes = Set(BooleanType())

  override def semanticCheck(ctx: SemanticContext) =
    expression.semanticCheck(ctx) then
      expression.constrainType(NodeType()) then
      super.semanticCheck(ctx)

  private def toPredicate(l: Identifier): commands.Predicate =
    commands.HasLabel(expression.toCommand, commandvalues.KeyToken.Unresolved(l.name, commandvalues.TokenType.Label))

  def toCommand = labels.map(toPredicate).reduceLeft(commands.And(_, _))
}

case class Collection(expressions: Seq[Expression], token: InputToken) extends Expression {
  def semanticCheck(ctx: SemanticContext) = expressions.semanticCheck(ctx) then specifyType(possibleTypes)

  private def possibleTypes: SemanticState => TypeSet = state => expressions match {
    case Seq() => Set(CollectionType(AnyType()))
    case _     => expressions.mergeDownTypes(state).map(CollectionType.apply)
  }

  def toCommand = commandexpressions.Collection(expressions.map(_.toCommand):_*)
}

case class MapExpression(items: Seq[(Identifier, Expression)], token: InputToken) extends Expression with SimpleTypedExpression {
  protected def possibleTypes = Set(MapType())

  override def semanticCheck(ctx: SemanticContext) = items.map(_._2).semanticCheck(ctx) then super.semanticCheck(ctx)

  def toCommand = {
    val literalMap: Map[String, CommandExpression] = items.map {
      case (id, ex) => id.name -> ex.toCommand
    }.toMap

    commandexpressions.LiteralMap(literalMap)
  }
}

case class CollectionSlice(collection: Expression, from: Option[Expression], to: Option[Expression], token: InputToken)
  extends Expression {

  override def semanticCheck(ctx: SemanticContext) =
    collection.semanticCheck(ctx) then
      collection.constrainType(CollectionType(AnyType())) then
      when(from.isEmpty && to.isEmpty) {
        SemanticError("The start or end (or both) is required for a collection slice", token)
      } then
      from.semanticCheck(ctx) then
      from.constrainType(IntegerType(), LongType()) then
      to.semanticCheck(ctx) then
      to.constrainType(IntegerType(), LongType()) then
      specifyType(collection.types)

  def toCommand = commandexpressions.CollectionSliceExpression(collection.toCommand, from.map(_.toCommand), to.map(_.toCommand))
}

case class CollectionIndex(collection: Expression, idx: Expression, token: InputToken)
  extends Expression {

  override def semanticCheck(ctx: SemanticContext) =
    collection.semanticCheck(ctx) then
      collection.constrainType(CollectionType(AnyType())) then
      idx.semanticCheck(ctx) then
      idx.constrainType(IntegerType(), LongType()) then
      specifyType(collection.types(_).map(_.iteratedType))

  def toCommand = commandexpressions.CollectionIndex(collection.toCommand, idx.toCommand)
}
