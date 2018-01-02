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
package org.neo4j.cypher.internal.compiler.v2_3.ast.conditions

import org.neo4j.cypher.internal.frontend.v2_3.ast.functions
import org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters.Condition
import org.neo4j.cypher.internal.frontend.v2_3.ast.{Expression, FunctionInvocation, Property, Equals}

case object normalizedEqualsArguments extends Condition {
  def apply(that: Any): Seq[String] = {
    val equals = collectNodesOfType[Equals].apply(that)
    equals.collect {
      case eq@Equals(expr, Property(_,_)) if !expr.isInstanceOf[Property] && notIdFunction(expr) =>
        s"Equals at ${eq.position} is not normalized: $eq"
      case eq@Equals(expr, func@FunctionInvocation(_, _, _)) if isIdFunction(func) && notIdFunction(expr) =>
        s"Equals at ${eq.position} is not normalized: $eq"
    }
  }

  private def isIdFunction(func: FunctionInvocation) = func.function == Some(functions.Id)

  private def notIdFunction(expr: Expression) =
    !expr.isInstanceOf[FunctionInvocation] || !isIdFunction(expr.asInstanceOf[FunctionInvocation])

  override def name: String = productPrefix
}
