/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.rendering

import org.neo4j.cypher.CypherExecutionMode
import org.neo4j.cypher.internal.QueryOptions
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.logical.plans.ResolvedCall
import org.neo4j.cypher.internal.logical.plans.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.runtime.ast.ParameterFromSlot
import org.neo4j.cypher.internal.util.InputPosition

object QueryRenderer {

  private object clauseExtension extends Prettifier.ClausePrettifier {
    override def asString(ctx: Prettifier.QueryPrettifier): PartialFunction[Clause, String] = {
      case rc: ResolvedCall => ctx.asString(rc.asUnresolvedCall)
    }
  }

  private object exprExtension extends ExpressionStringifier.Extension {
    override def apply(ctx: ExpressionStringifier)(expression: Expression): String = expression match {
      case p: ParameterFromSlot           => ctx.apply(Parameter(p.name, p.parameterType)(p.position))
    }
  }

  private val renderStrict = Prettifier(
    expr = ExpressionStringifier(
      extension = exprExtension,
      alwaysParens = true,
      alwaysBacktick = true,
      preferSingleQuotes = false,
      sensitiveParamsAsParams = true
    ),
    extension = clauseExtension,
    useInCommands = false
  )
  private val renderPretty = renderStrict.copy(
    expr = renderStrict.expr.copy(
      alwaysParens = false,
      alwaysBacktick = false,
      sensitiveParamsAsParams = false
    ),
    useInCommands = true
  )

  private val pos = InputPosition.NONE

  private val NL = System.lineSeparator()

  def render(clauses: Seq[Clause]): String =
    render(Query(None, SingleQuery(clauses)(pos))(pos))

  def render(statement: Statement): String =
    renderStrict.asString(statement)

  def addOptions(statement: String, options: QueryOptions): String =
    renderOptions(options) + statement

  def renderOptions(options: QueryOptions): String =
    renderExecutionMode(options.executionMode) + options.render.map(_ + NL).getOrElse("")

  private def renderExecutionMode(executionMode: CypherExecutionMode): String = executionMode match {
    case CypherExecutionMode.explain => "EXPLAIN " + NL
    case CypherExecutionMode.profile => "PROFILE " + NL
    case _                           => ""
  }

  def pretty(expression: Expression): String =
    renderPretty.expr.apply(expression)

  def pretty(statement: Statement): String =
    renderPretty.asString(statement)
}
