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
import org.neo4j.cypher.internal.commands.{expressions => commandexpressions, Predicate => CommandPredicate}
import org.neo4j.cypher.internal.commands.expressions.{Expression => CommandExpression}
import org.neo4j.cypher.internal.parser.experimental.ast.Expression.SemanticContext

case class CaseExpression(expression: Option[Expression], alternatives: Seq[(Expression, Expression)], default: Option[Expression], token: InputToken) extends Expression {
  def semanticCheck(ctx: SemanticContext): SemanticCheck = {
    val possibleTypes : TypeGenerator = (alternatives.map(_._2) ++ default) mergeDownTypes

    expression.semanticCheck(ctx) then
      alternatives.flatMap { a => Seq(a._1, a._2) }.semanticCheck(ctx) then
      default.semanticCheck(ctx) then
      when (expression.isEmpty) {
        alternatives.map(_._1).limitType(BooleanType())
      } then limitType(possibleTypes)
  }

  def toCommand: CommandExpression = expression match {
    case Some(e) => {
      val legacyAlternatives = alternatives.map {
        a => (a._1.toCommand, a._2.toCommand)
      }
      commandexpressions.SimpleCase(e.toCommand, legacyAlternatives, default.map(_.toCommand))
    }
    case None => {
      val predicateAlternatives = alternatives.map { a =>
        a._1.toCommand match {
          case predicate: CommandPredicate => (predicate, a._2.toCommand)
          case _ => throw new SyntaxException(s"Argument to WHEN is not a predicate (${a._1.token.startPosition})")
        }
      }
      commandexpressions.GenericCase(predicateAlternatives, default.map(_.toCommand))
    }
  }
}
