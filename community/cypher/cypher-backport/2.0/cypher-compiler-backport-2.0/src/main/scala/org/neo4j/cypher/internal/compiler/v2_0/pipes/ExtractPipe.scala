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
package org.neo4j.cypher.internal.compiler.v2_0.pipes

import org.neo4j.cypher.internal.compiler.v2_0._
import commands.expressions.{CachedExpression, Identifier, Expression}
import data.SimpleVal
import symbols._

object ExtractPipe {
  def apply(source: Pipe, expressions: Map[String, Expression]): ExtractPipe = source match {
      // If we can merge the two pipes together, do it
    case p: ExtractPipe if canMerge(p, expressions) =>
      new ExtractPipe(p.source, p.expressions ++ expressions)

    case _              =>
      new ExtractPipe(source, expressions)
  }

  private def canMerge(source:ExtractPipe, expressions: Map[String, Expression]) = {
    val symbols = source.source.symbols.identifiers.keySet
    val expressionsDependenciesMet = expressions.values.forall(_.symbolDependenciesMet(source.source.symbols))
    val expressionsDependOnIntroducedSymbols = expressions.values.exists {
      case e => e.exists {
        case Identifier(x) => symbols.contains(x)
        case _             => false
      }
    }

    expressionsDependenciesMet && !expressionsDependOnIntroducedSymbols
  }

}

class ExtractPipe(val source: Pipe, val expressions: Map[String, Expression]) extends PipeWithSource(source) {
  val symbols: SymbolTable = {
    val newIdentifiers = expressions.map {
      case (name, expression) => name -> expression.getType(source.symbols)
    }

    source.symbols.add(newIdentifiers)
  }

  /*
  Most of the time, we can execute expressions and put the results straight back into the original execution context.
  Some times, an expression we want to run can overwrite an identifier that already exists in the context. In these
  cases, we need to run the expressions on the original execution context. Here we decide wich one it is we're dealing
  with and hard code the version to use
   */
  val applyExpressions: (ExecutionContext, QueryState) => ExecutionContext = {
    val overwritesAlreadyExistingIdentifiers = expressions.exists {
      case (name, Identifier(originalName)) => name != originalName
      case (name, CachedExpression(originalName, _)) => name != originalName
      case (name, _) => source.symbols.hasIdentifierNamed(name)
    }

    val applyExpressionsOverwritingOriginal = (ctx: ExecutionContext, state: QueryState) => {
      expressions.foreach {
        case (name, expression) =>
          ctx += name -> expression(ctx)(state)
      }
      ctx
    }
    val applyExpressionsWhileKeepingOriginal = (ctx: ExecutionContext, state: QueryState) => {
      val original = ctx.clone()
      expressions.foreach {
        case (name, expression) =>
          ctx += name -> expression(original)(state)
      }
      ctx
    }

    if (overwritesAlreadyExistingIdentifiers)
      applyExpressionsWhileKeepingOriginal
    else
      applyExpressionsOverwritingOriginal
  }

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) =
    input.map( ctx => applyExpressions(ctx, state) )

  override def executionPlanDescription =
    source.executionPlanDescription
      .andThen(this, "Extract",
        "symKeys" -> SimpleVal.fromIterable(source.symbols.keys),
        "exprKeys" -> SimpleVal.fromIterable(expressions.keys))
}

