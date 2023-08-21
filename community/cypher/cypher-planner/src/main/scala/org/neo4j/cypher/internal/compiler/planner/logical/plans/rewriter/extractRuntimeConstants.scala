/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.extractRuntimeConstants.STOPPER
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.logical.plans.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.runtime.ast.RuntimeConstant
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.RewriterStopper
import org.neo4j.cypher.internal.util.bottomUp

/**
 * Extract expressions that are known to be constant for the lifetime of the execution of the query.
 * These are not the same thing as literals, since the value can change between executions.
 *
 * Example:
 * {{{
 *   MATCH (n) WHERE n.datetime < datetime({date: $date}) RETURN n
 * }}}
 *
 * will be rewritten to something like
 *
 * {{{
 *   MATCH (n) WHERE n.datetime < RuntimeConstant(datetime({date: $date})) RETURN n
 * }}}
 */
case class extractRuntimeConstants(anonymousVariableNameGenerator: AnonymousVariableNameGenerator) extends Rewriter {

  override def apply(input: AnyRef): AnyRef = {
    val rewriter = bottomUp(
      Rewriter.lift {
        case d @ Datetime(Seq(arg)) if isConstant(arg)        => constant(d)
        case d @ LocalDatetime(Seq(arg)) if isConstant(arg)   => constant(d)
        case d @ Date(Seq(arg)) if isConstant(arg)            => constant(d)
        case d @ LocalTime(Seq(arg)) if isConstant(arg)       => constant(d)
        case d @ Time(Seq(arg)) if isConstant(arg)            => constant(d)
        case d @ Duration(Seq(arg)) if arg.isConstantForQuery => constant(d)
      },
      stopper = STOPPER
    )

    rewriter.apply(input)
  }

  private def constant(e: Expression) =
    RuntimeConstant(Variable(anonymousVariableNameGenerator.nextName)(InputPosition.NONE), e)

  private def isConstant(arg: Expression): Boolean = arg match {
    // this is not supported semantically, but here just in case
    case MapExpression(Seq()) => false
    // {timezone: TZ} is like the no arg case, that is not necessarily constant
    case MapExpression(Seq((PropertyKeyName("timezone"), _))) => false
    case MapExpression(items) => items.forall { case (_, value) => value.isConstantForQuery }
    case _                    => arg.isConstantForQuery
  }
}

object extractRuntimeConstants {
  val STOPPER: RewriterStopper = (a: AnyRef) => a.isInstanceOf[RuntimeConstant]
}

trait FunctionMatcher extends Product {

  def unapply(arg: Expression): Option[IndexedSeq[Expression]] = {
    arg match {
      case ResolvedFunctionInvocation(QualifiedName(ns, n), _, args) if n.equalsIgnoreCase(name) && ns == namespace =>
        Some(args)
      case _ => None
    }
  }

  def namespace: Seq[String] = Seq.empty
  def name: String = toString
}

case object Date extends FunctionMatcher
case object Datetime extends FunctionMatcher
case object LocalDatetime extends FunctionMatcher
case object Time extends FunctionMatcher
case object LocalTime extends FunctionMatcher
case object Duration extends FunctionMatcher
