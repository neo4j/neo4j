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

import org.neo4j.cypher.SyntaxException
import org.neo4j.cypher.internal.parser.experimental._
import org.neo4j.cypher.internal.symbols._
import org.neo4j.cypher.internal.commands
import org.neo4j.cypher.internal.commands.{expressions => commandexpressions, Predicate => CommandPredicate}
import org.neo4j.cypher.internal.commands.expressions.{Expression => CommandExpression}
import org.neo4j.cypher.internal.parser.experimental.ast.Expression.SemanticContext

trait FilteringExpression extends Expression {
  def name: String
  def identifier: Identifier
  def expression: Expression
  def innerPredicate: Option[Expression]

  def semanticCheck(ctx: SemanticContext) = {
    expression.semanticCheck(ctx) then
      expression.limitType(CollectionType(AnyType())) then
      checkInnerPredicate
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

  def toCommand = {
    val command = expression.toCommand
    val inner = innerPredicate.map(_.toCommand) match {
      case Some(e: commands.Predicate) => e
      case None                        => commands.True()
      case _                           => throw new SyntaxException(s"Argument to ${name} is not a predicate (${expression.token.startPosition})")
    }
    toCommand(command, identifier.name, inner)
  }
}


case class FilterExpression(identifier: Identifier, expression: Expression, innerPredicate: Option[Expression], token: InputToken) extends FilteringExpression {
  val name = "FILTER"

  override def semanticCheck(ctx: SemanticContext) = {
    checkPredicateDefined then super.semanticCheck(ctx) then limitType(expression.types)
  }

  def checkPredicateDefined = if (innerPredicate.isDefined) None else Some(SemanticError(s"${name} requires a WHERE predicate", token))

  def toCommand(command: CommandExpression, name: String, inner: commands.Predicate) = commandexpressions.FilterFunction(command, name, inner)
}


case class ExtractExpression(
    identifier: Identifier,
    expression: Expression,
    innerPredicate: Option[Expression],
    extractExpression: Option[Expression],
    token: InputToken) extends FilteringExpression
{
  val name = "EXTRACT"

  override def semanticCheck(ctx: SemanticContext) = checkPredicateNotDefined then super.semanticCheck(ctx) then checkInnerExpression

  def checkPredicateNotDefined = if (innerPredicate.isEmpty) None else Some(SemanticError(s"${name} should not contain a WHERE predicate", token))

  private def checkInnerExpression : SemanticCheck = {
    extractExpression match {
      case Some(e) => withScopedState {
        val innerTypes : TypeGenerator = expression.types(_).map(_.iteratedType)
        identifier.declare(innerTypes) then e.semanticCheck(SemanticContext.Simple)
      } then {
        val outerTypes : TypeGenerator = e.types(_).map(CollectionType(_))
        limitType(outerTypes)
      }
      case None    => SemanticError(s"${name} requires an extract expression", token)
    }
  }

  def toCommand(command: CommandExpression, name: String, inner: commands.Predicate) = commandexpressions.ExtractFunction(command, name, extractExpression.get.toCommand)
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
        limitType(outerTypes)
      }
      case None    => limitType(expression.types)
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
}


sealed trait IterablePredicateExpression extends FilteringExpression {
  override def semanticCheck(ctx: SemanticContext) = super.semanticCheck(ctx) then limitType(BooleanType())

  def toPredicate(command: CommandExpression, name: String, inner: commands.Predicate) : commands.Predicate

  def toCommand(command: CommandExpression, name: String, inner: commands.Predicate) =
    toPredicate(command, identifier.name, inner)
}

case class AllIterablePredicate(identifier: Identifier, expression: Expression, innerPredicate: Option[Expression], token: InputToken) extends IterablePredicateExpression {
  val name = "ALL"
  def toPredicate(command: CommandExpression, name: String, inner: commands.Predicate) = {
    commands.AllInCollection(command, identifier.name, inner)
  }
}

case class AnyIterablePredicate(identifier: Identifier, expression: Expression, innerPredicate: Option[Expression], token: InputToken) extends IterablePredicateExpression {
  val name = "ANY"
  def toPredicate(command: CommandExpression, name: String, inner: commands.Predicate) = {
    commands.AnyInCollection(command, identifier.name, inner)
  }
}

case class NoneIterablePredicate(identifier: Identifier, expression: Expression, innerPredicate: Option[Expression], token: InputToken) extends IterablePredicateExpression {
  val name = "NONE"
  def toPredicate(command: CommandExpression, name: String, inner: commands.Predicate) = {
    commands.NoneInCollection(command, identifier.name, inner)
  }
}

case class SingleIterablePredicate(identifier: Identifier, expression: Expression, innerPredicate: Option[Expression], token: InputToken) extends IterablePredicateExpression {
  val name = "SINGLE"
  def toPredicate(command: CommandExpression, name: String, inner: commands.Predicate) = {
    commands.SingleInCollection(command, identifier.name, inner)
  }
}

case class ReduceExpression(accumulator: Identifier, init: Expression, id: Identifier, collection: Expression, expression: Expression, token: InputToken) extends Expression {
  def semanticCheck(ctx: SemanticContext): SemanticCheck = {
    init.semanticCheck(ctx) then
      collection.semanticCheck(ctx) then
      collection.limitType(CollectionType(AnyType())) then
      withScopedState {
        val indexType: TypeGenerator = collection.types(_).map(_.iteratedType)
        val accType: TypeGenerator = init.types
        id.declare(indexType) then
          accumulator.declare(accType) then
          expression.semanticCheck(SemanticContext.Simple)
      } then expression.limitType(init.types) then
      limitType(s => init.types(s) mergeDown expression.types(s))
  }

  def toCommand: CommandExpression = commandexpressions.ReduceFunction(collection.toCommand, id.name, expression.toCommand, accumulator.name, init.toCommand)
}
