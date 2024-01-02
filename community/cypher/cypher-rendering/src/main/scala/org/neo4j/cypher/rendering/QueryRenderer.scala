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
package org.neo4j.cypher.rendering

import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.frontend.phases.ResolvedCall
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
      case p: ParameterFromSlot => ctx.apply(ExplicitParameter(p.name, p.parameterType)(p.position))
      case _                    => throw new IllegalStateException("Expected type: ParameterFromSlot")
    }
  }

  private val renderStrict = Prettifier(
    expr = stringifier(pretty = false),
    extension = clauseExtension,
    useInCommands = false
  )

  private val renderPretty = renderStrict.copy(
    expr = stringifier(pretty = true),
    useInCommands = true
  )

  private val pos = InputPosition.NONE

  def render(clauses: Seq[Clause]): String =
    render(SingleQuery(clauses)(pos))

  def render(statement: Statement): String =
    renderStrict.asString(statement)

  def pretty(expression: Expression): String =
    renderPretty.expr.apply(expression)

  private def stringifier(pretty: Boolean): ExpressionStringifier = {
    ExpressionStringifier(
      extensionStringifier = exprExtension,
      alwaysParens = !pretty,
      alwaysBacktick = !pretty,
      preferSingleQuotes = false,
      sensitiveParamsAsParams = !pretty
    )
  }
}
