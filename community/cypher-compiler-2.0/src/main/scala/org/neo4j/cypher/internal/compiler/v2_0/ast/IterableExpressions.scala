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

import Expression.SemanticContext
import org.neo4j.cypher.internal.compiler.v2_0._
import commands.{expressions => commandexpressions, Predicate => CommandPredicate, NonEmpty}
import commands.expressions.{Expression => CommandExpression}
import symbols._

trait FilteringExpression extends Expression {
  def name: String
  def identifier: Identifier
  def expression: Expression
  def innerPredicate: Option[Expression]

  def semanticCheck(ctx: SemanticContext) =
    expression.semanticCheck(ctx) then
      expression.constrainType(CollectionType(AnyType())) then
      checkInnerPredicate

  protected def checkPredicateDefined =
    when (innerPredicate.isEmpty) {
      SemanticError(s"${name}(...) requires a WHERE predicate", token)
    }

  protected def checkPredicateNotDefined =
    when (innerPredicate.isDefined) {
      SemanticError(s"${name}(...) should not contain a WHERE predicate", token)
    }

  private def checkInnerPredicate : SemanticCheck = {
    innerPredicate match {
      case Some(e) => withScopedState {
        val innerTypes : TypeGenerator = expression.types(_).map(_.iteratedType)
        identifier.declare(innerTypes) then e.semanticCheck(SemanticContext.Simple)
      }
      case None    => SemanticCheckResult.success
    }
  }

  def toCommand(command: CommandExpression, name: String, inner: commands.Predicate) : CommandExpression
  def toPredicate(command: CommandExpression, name: String, inner: commands.Predicate) : CommandPredicate

  override def toCommand = {
    val command = expression.toCommand
    val inner: CommandPredicate = innerPredicate.map(_.toPredicate).getOrElse(commands.True())
    toCommand(command, identifier.name, inner)
  }

  override def toPredicate = {
    val command = expression.toCommand
    val inner = innerPredicate.map(_.toPredicate).getOrElse(commands.True())
    toPredicate(command, identifier.name, inner)
  }
}


case class FilterExpression(identifier: Identifier, expression: Expression, innerPredicate: Option[Expression], token: InputToken) extends FilteringExpression {
  val name = "filter"

  override def semanticCheck(ctx: SemanticContext) =
    checkPredicateDefined then
      super.semanticCheck(ctx) then
      this.specifyType(expression.types)

  def toCommand(command: CommandExpression, name: String, inner: commands.Predicate) = commandexpressions.FilterFunction(command, name, inner)

  def toPredicate(command: CommandExpression, name: String, inner: CommandPredicate) = NonEmpty(toCommand(command, name, inner))
}


case class ExtractExpression(
    identifier: Identifier,
    expression: Expression,
    innerPredicate: Option[Expression],
    extractExpression: Option[Expression],
    token: InputToken) extends FilteringExpression
{
  val name = "extract"

  override def semanticCheck(ctx: SemanticContext) =
    checkPredicateNotDefined then
      checkExtractExpressionDefined then
      super.semanticCheck(ctx) then
      checkInnerExpression

  private def checkExtractExpressionDefined =
    when (extractExpression.isEmpty) {
      SemanticError(s"$name(...) requires '| expression' (an extract expression)", token)
    }

  private def checkInnerExpression : SemanticCheck =
    extractExpression.fold(SemanticCheckResult.success) {
      e => withScopedState {
        val innerTypes : TypeGenerator = expression.types(_).map(_.iteratedType)
        identifier.declare(innerTypes) then e.semanticCheck(SemanticContext.Simple)
      } then {
        val outerTypes : TypeGenerator = e.types(_).map(CollectionType(_))
        this.specifyType(outerTypes)
      }
    }

  def toCommand(command: CommandExpression, name: String, inner: commands.Predicate) =
    commandexpressions.ExtractFunction(command, name, extractExpression.get.toCommand)

  def toPredicate(command: CommandExpression, name: String, inner: CommandPredicate) =
    NonEmpty(toCommand(command, name, inner))
}


case class ListComprehension(
    identifier: Identifier,
    expression: Expression,
    innerPredicate: Option[Expression],
    extractExpression: Option[Expression],
    token: InputToken) extends FilteringExpression
{
  val name = "[...]"

  override def semanticCheck(ctx: SemanticContext) = super.semanticCheck(ctx) then checkInnerExpression

  private def checkInnerExpression : SemanticCheck = {
    extractExpression match {
      case Some(e) => withScopedState {
          val innerTypes : TypeGenerator = expression.types(_).map(_.iteratedType)
          identifier.declare(innerTypes) then e.semanticCheck(SemanticContext.Simple)
        } then {
          val outerTypes : TypeGenerator = e.types(_).map(CollectionType(_))
          this.specifyType(outerTypes)
        }
      case None    => this.specifyType(expression.types)
    }
  }

  def toCommand(command: CommandExpression, name: String, inner: commands.Predicate) = {
    val filter = inner match {
      case commands.True() => command
      case _               => commandexpressions.FilterFunction(command, name, inner)
    }
    val extract = extractExpression match {
      case Some(e) => commandexpressions.ExtractFunction(filter, name, e.toCommand)
      case None    => filter
    }
    extract
  }

  def toPredicate(command: CommandExpression, name: String, inner: CommandPredicate) =
     NonEmpty(toCommand(command, name, inner))
}


sealed trait IterablePredicateExpression extends FilteringExpression {
  override def semanticCheck(ctx: SemanticContext) =
    checkPredicateDefined then
      super.semanticCheck(ctx) then
      this.specifyType(BooleanType())

  def toPredicate(command: CommandExpression, name: String, inner: commands.Predicate) : commands.Predicate

  def toCommand(command: CommandExpression, name: String, inner: commands.Predicate) =
    toPredicate(command, identifier.name, inner)
}

case class AllIterablePredicate(identifier: Identifier, expression: Expression, innerPredicate: Option[Expression], token: InputToken) extends IterablePredicateExpression {
  val name = "all"
  def toPredicate(command: CommandExpression, name: String, inner: commands.Predicate) = {
    commands.AllInCollection(command, identifier.name, inner)
  }
}

case class AnyIterablePredicate(identifier: Identifier, expression: Expression, innerPredicate: Option[Expression], token: InputToken) extends IterablePredicateExpression {
  val name = "any"
  def toPredicate(command: CommandExpression, name: String, inner: commands.Predicate) = {
    commands.AnyInCollection(command, identifier.name, inner)
  }
}

case class NoneIterablePredicate(identifier: Identifier, expression: Expression, innerPredicate: Option[Expression], token: InputToken) extends IterablePredicateExpression {
  val name = "none"
  def toPredicate(command: CommandExpression, name: String, inner: commands.Predicate) = {
    commands.NoneInCollection(command, identifier.name, inner)
  }
}

case class SingleIterablePredicate(identifier: Identifier, expression: Expression, innerPredicate: Option[Expression], token: InputToken) extends IterablePredicateExpression {
  val name = "single"
  def toPredicate(command: CommandExpression, name: String, inner: commands.Predicate) = {
    commands.SingleInCollection(command, identifier.name, inner)
  }
}

case class ReduceExpression(accumulator: Identifier, init: Expression, id: Identifier, collection: Expression, expression: Expression, token: InputToken) extends Expression {
  def semanticCheck(ctx: SemanticContext): SemanticCheck =
    init.semanticCheck(ctx) then
      collection.semanticCheck(ctx) then
      collection.constrainType(CollectionType(AnyType())) then
      withScopedState {
        val indexType: TypeGenerator = collection.types(_).map(_.iteratedType)
        val accType: TypeGenerator = init.types
        id.declare(indexType) then
        accumulator.declare(accType) then
        expression.semanticCheck(SemanticContext.Simple)
      } then expression.constrainType(init.types) then
      this.specifyType(s => init.types(s) mergeDown expression.types(s))

  def toCommand: CommandExpression = commandexpressions.ReduceFunction(collection.toCommand, id.name, expression.toCommand, accumulator.name, init.toCommand)
}
