/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{CachedExpression, Expression, Identifier}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.Effects._
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.KeyNames
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.{PlanDescriptionImpl, SingleChild}
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

object ExtractPipe {
  def apply(source: Pipe, expressions: Map[String, Expression])(implicit pipeMonitor: PipeMonitor): ExtractPipe = source match {
    // If we can merge the two pipes together, do it
    case p: ExtractPipe if canMerge(p, expressions) =>
      new ExtractPipe(p.source, p.expressions ++ expressions, true)

    case _              =>
      new ExtractPipe(source, expressions, true)
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

case class ExtractPipe(source: Pipe, expressions: Map[String, Expression], hack_remove_this:Boolean)
                      (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) {
  val symbols: SymbolTable = {
    val newIdentifiers = expressions.map {
      case (name, expression) => name -> expression.getType(source.symbols)
    }

    source.symbols.add(newIdentifiers)
  }

  /*
  Most of the time, we can execute expressions and put the results straight back into the original execution context.
  Some times, an expression we want to run can overwrite an identifier that already exists in the context. In these
  cases, we need to run the expressions on the original execution context. Here we decide which one it is we're dealing
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

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    //register as parent so that stats are associated with this pipe
    state.decorator.registerParentPipe(this)

    input.map( ctx => applyExpressions(ctx, state) )
  }

  override def planDescription = {
    val arguments = expressions.map(_._1).toSeq

    new PlanDescriptionImpl(this.id, "Extract", SingleChild(source.planDescription), Seq(KeyNames(arguments)), identifiers)
  }

  def dup(sources: List[Pipe]): Pipe = {
    val (source :: Nil) = sources
    copy(source = source)
  }

  override def localEffects = expressions.effects(symbols)
}

