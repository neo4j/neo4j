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
package org.neo4j.cypher.internal.parser.experimental.ast

import org.neo4j.cypher.internal.parser.experimental._
import org.neo4j.cypher.internal.symbols._
import org.neo4j.cypher.internal.commands
import org.neo4j.cypher.internal.commands.{expressions => commandexpressions, values => commandvalues}
import org.neo4j.cypher.internal.commands.expressions.{Expression => CommandExpression}
import org.neo4j.cypher.internal.commands.values.TokenType.PropertyKey

object Expression {
  sealed trait SemanticContext
  object SemanticContext {
    case object Simple extends SemanticContext
    case object Results extends SemanticContext
  }

  implicit class SemanticCheckableOption[A <: Expression](option: Option[A]) {
    def semanticCheck(ctx: SemanticContext) = option.fold(SemanticCheckResult.success) { _.semanticCheck(ctx) }
  }
  implicit class SemanticCheckableExpressionTraversable[A <: Expression](traversable: TraversableOnce[A]) extends SemanticChecking {
    def semanticCheck(ctx: SemanticContext) = {
      traversable.foldLeft(SemanticCheckResult.success) { (f, o) => f then o.semanticCheck(ctx) }
    }
  }
  implicit class InferrableTypeTraversableOnce[A <: Expression](traversable: TraversableOnce[A]) {
    def mergeDownTypes : TypeGenerator = (state: SemanticState) => traversable.map { _.types(state) } reduce { _ mergeDown _ }
    def limitType(possibleType: CypherType, possibleTypes: CypherType*) : SemanticCheck = {
      traversable.foldLeft(SemanticCheckResult.success) {
        (f, e) => f then e.limitType(possibleType, possibleTypes:_*)
      }
    }
  }
}

import Expression._

abstract class Expression extends AstNode with SemanticChecking {
  def semanticCheck(ctx: SemanticContext): SemanticCheck

  // double-dispatch helpers
  final def types : TypeGenerator = s => s.expressionTypes(this) match {
    case None => throw new IllegalStateException(s"Types of $this have not been evaluated (${token.startPosition})")
    case Some(types) => types
  }
  final def limitType(possibleType: CypherType, possibleTypes: CypherType*): SemanticState => Either[SemanticError, SemanticState] =
      limitType((possibleType +: possibleTypes).toSet)
  final def limitType(typeGen: TypeGenerator) : SemanticState => Either[SemanticError, SemanticState] =
      s => s.limitExpressionType(this, token, typeGen(s))
  final def limitType(possibleTypes: => TypeSet) : SemanticState => Either[SemanticError, SemanticState] =
      _.limitExpressionType(this, token, possibleTypes)

  def toCommand: CommandExpression
}

trait SimpleTypedExpression { self: Expression =>
  protected def possibleTypes : TypeSet
  def semanticCheck(ctx: SemanticContext) : SemanticCheck = limitType(possibleTypes)
}

case class Identifier(name: String, token: InputToken) extends Expression {
  // check the identifier is defined and, if not, define it so that later errors are suppressed
  def semanticCheck(ctx: SemanticContext) = s => ensureDefined(AnyType())(s) match {
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
  final def ensureDefined(possibleType: CypherType, possibleTypes: CypherType*) =
      (_: SemanticState).ensureIdentifierDefined(this, possibleType, possibleTypes:_*)

  def toCommand = commands.expressions.Identifier(name)
}

case class Parameter(name: String, token: InputToken) extends Expression with SimpleTypedExpression {
  protected def possibleTypes = Set(BooleanType(), MapType(), NumberType(), StringType(), CollectionType(ScalarType()))

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

case class Property(map: Expression, identifier: Identifier, token: InputToken) extends Expression with SimpleTypedExpression {
  protected def possibleTypes = Set(BooleanType(), NumberType(), StringType(), CollectionType(AnyType()))

  override def semanticCheck(ctx: SemanticContext) = {
    map.semanticCheck(ctx) then
      map.limitType(MapType()) then
      super.semanticCheck(ctx)
  }

  def toCommand = commands.expressions.Property(map.toCommand, PropertyKey(identifier.name))
}

case class PatternExpression(pattern: Pattern) extends Expression with SimpleTypedExpression {
  def token = pattern.token
  protected def possibleTypes = Set(CollectionType(PathType()))

  override def semanticCheck(ctx: SemanticContext) =
    pattern.semanticCheck(Pattern.SemanticContext.Expression) then super.semanticCheck(ctx)

  def toCommand = commands.PatternPredicate(pattern.toLegacyPatterns)
}

case class HasLabels(expression: Expression, labels: Seq[Identifier], token: InputToken) extends Expression with SimpleTypedExpression {
  protected def possibleTypes = Set(BooleanType())

  override def semanticCheck(ctx: SemanticContext) = expression.limitType(NodeType()) then super.semanticCheck(ctx)

  private def toPredicate(l: Identifier): commands.Predicate =
    commands.HasLabel(expression.toCommand, commandvalues.KeyToken.Unresolved(l.name, commandvalues.TokenType.Label))

  def toCommand = labels.map(toPredicate).reduceLeft(commands.And(_, _))
}

case class Collection(expressions: Seq[Expression], token: InputToken) extends Expression {
  def semanticCheck(ctx: SemanticContext) = expressions.semanticCheck(ctx) then limitType(possibleTypes)

  private def possibleTypes: SemanticState => TypeSet = state => expressions match {
    case Seq() => Set(CollectionType(AnyType()))
    case _     => expressions.mergeDownTypes(state).map(CollectionType.apply)
  }

  def toCommand = commandexpressions.Collection(expressions.map(_.toCommand):_*)
}

case class MapExpression(items: Seq[(Identifier, Expression)], token: InputToken) extends Expression with SimpleTypedExpression {
  protected def possibleTypes = Set(MapType())

  override def semanticCheck(ctx: SemanticContext) = items.map(_._2).semanticCheck(ctx) then super.semanticCheck(ctx)

  def toCommand = ???
}
