/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.ast.convert

import ExpressionConverters._
import PatternConverters._
import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.compiler.v2_1.commands.{expressions => commandexpressions, values => commandvalues}
import org.neo4j.cypher.internal.compiler.v2_1.commands.StartItem
import org.neo4j.cypher.internal.compiler.v2_1.commands.PeriodicCommitQuery
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.cypher.internal.compiler.v2_1.commands.expressions.Expression

object OtherConverters {

  implicit class SortItemConverter(val e: ast.SortItem) extends AnyVal {
    def asCommandSortItem: commands.SortItem = {
      val (expression, ascending) = e match {
        case ast.AscSortItem(expr) => (expr, true)
        case ast.DescSortItem(expr) => (expr, false)
      }
      commands.SortItem(expression.asCommandExpression, ascending)
    }
  }

}
