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
package org.neo4j.cypher.internal.runtime.ast

import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.runtime.ast.RuntimeConstant.STRINGIFIER

/**
 * A runtime constant is an expression that only needs to be evaluated once per query.
 * It is not necessarily the same thing as a literal which are true constants and the values can be cached.
 *
 * Examples:
 * - `datetime({year: $param})`
 * - `duration($AUTOSTRING)`
 * @param variable
 * @param inner
 */
case class RuntimeConstant(variable: LogicalVariable, inner: Expression) extends RuntimeExpression {
  self =>

  override def isConstantForQuery: Boolean = true

  override def asCanonicalStringVal: String = STRINGIFIER(inner)

}

object RuntimeConstant {

  val STRINGIFIER: ExpressionStringifier = ExpressionStringifier(
    extensionStringifier = EXTENSION,
    alwaysParens = false,
    alwaysBacktick = false,
    preferSingleQuotes = true,
    sensitiveParamsAsParams = false
  )

  private object EXTENSION extends ExpressionStringifier.Extension {

    override def apply(ctx: ExpressionStringifier)(expression: Expression): String = expression match {
      case RuntimeConstant(_, inner) => ctx(inner)
      case e if ctx ne STRINGIFIER   => ctx(e)
      case e                         => e.asCanonicalStringVal
    }
  }
}
