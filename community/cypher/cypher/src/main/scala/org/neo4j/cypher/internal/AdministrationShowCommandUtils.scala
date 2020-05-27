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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.ast.OrderBy
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItem
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.runtime.ast.ParameterFromSlot
import org.neo4j.cypher.internal.util.InputPosition

object AdministrationShowCommandUtils {

  private val prettifier = Prettifier(ExpressionStringifier()).IndentingQueryPrettifier()

  private def genDefaultOrderBy(columns: List[String], defaultOrder: Seq[String]): Option[OrderBy] =
    defaultOrder.filter(columns.contains) match {
      case Nil => None
      case columns => Some(OrderBy(columns.zipWithIndex.map {
        case (col, i) =>
          val pos = new InputPosition(i, 1, 0)
          ast.AscSortItem(Variable(col)(pos))(pos)
    })(InputPosition.NONE))
  }

  private def maybeReplaceOrderBy(r: Return, symbols: List[String], defaultOrder: Seq[String]): Return = r.orderBy match {
      case None => r.copy(orderBy = genDefaultOrderBy(symbols, defaultOrder))(r.position)
      case _ => r
  }

  def generateWhereClause(where: Option[Where]) :String = {
    where.map { w =>
      val expr = ExpressionStringifier {
        case ParameterFromSlot(_, name, _) => s"$$${ExpressionStringifier.backtick(name, alwaysBacktick = false)}"
        case expression => ExpressionStringifier.failingExtender(expression)
      }
      s"WHERE ${expr(w.expression)}"
    }.getOrElse("")
  }

  def generateReturnClause(symbols: List[String], yields: Option[Return], returns: Option[Return], defaultOrder: Seq[String]): String = {
    val yieldColumns: Option[Return] = yields.map(maybeReplaceOrderBy(_, symbols, defaultOrder))
    val returnColumns: Option[Return] = returns.map(maybeReplaceOrderBy(_, symbols, defaultOrder))

    def symbolsToReturnItems(symbols: List[String]): List[ReturnItem] = symbols.map(s => UnaliasedReturnItem(Variable(s)(InputPosition.NONE), s)(InputPosition.NONE))

    val yieldsClause: Return = yieldColumns
      .getOrElse(Return(distinct = false, ReturnItems(false, symbolsToReturnItems(symbols))(InputPosition.NONE), genDefaultOrderBy(symbols, defaultOrder), None, None)(InputPosition.NONE))

    returnColumns match {
      case Some(r) =>
        prettifier.asString(With(distinct = false, yieldsClause.returnItems, yieldsClause.orderBy, yieldsClause.skip, yieldsClause.limit, None)(yieldsClause.position)) +
          " " + prettifier.asString(r)
      case None =>
        prettifier.asString(yieldsClause)
    }
  }

}
