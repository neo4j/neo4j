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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.OrderBy
import org.neo4j.cypher.internal.ast.ProjectionClause
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItem
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.runtime.ast.ParameterFromSlot
import org.neo4j.cypher.internal.util.InputPosition

object AdministrationShowCommandUtils {

  private val prettifier = Prettifier(ExpressionStringifier {
    case ParameterFromSlot(_, name, _) => s"$$${ExpressionStringifier.backtick(name)}"
    case expression                    => ExpressionStringifier.failingExtender(expression)
  }).IndentingQueryPrettifier()

  private def genDefaultOrderBy(columns: List[String], defaultOrder: Seq[String]): Option[OrderBy] =
    defaultOrder.filter(columns.contains) match {
      case Seq() => None
      case columns => Some(OrderBy(columns.zipWithIndex.map {
          case (col, i) =>
            val pos = InputPosition(i, 1, 0)
            ast.AscSortItem(Variable(col)(pos))(pos)
        })(InputPosition.NONE))
    }

  private def calcOrderBy(
    r: ProjectionClause,
    symbols: List[String],
    defaultOrder: Seq[String],
    explicitlyOrdered: Boolean = false
  ): Option[OrderBy] = {
    r.orderBy match {
      case None => if (explicitlyOrdered) None else genDefaultOrderBy(symbols, defaultOrder)
      case _    => r.orderBy
    }
  }

  private def getScope(previousScope: List[String], clause: Option[ProjectionClause]): List[String] = {
    def aliasSymbolsWhereNecessary(r: ReturnItems, symbols: List[String]): List[String] = {
      symbols.map(s =>
        r.items.find {
          case AliasedReturnItem(Variable(name), _) => name == s
          case _                                    => false
        } match {
          case Some(AliasedReturnItem(_, Variable(alias))) => alias
          case _                                           => s
        }
      )
    }
    clause match {
      case Some(projectionClause) if projectionClause.returnItems.includeExisting =>
        aliasSymbolsWhereNecessary(projectionClause.returnItems, previousScope)
      case Some(projectionClause) => projectionClause.returnItems.items.map(ri => ri.alias.get.name).toList
      case None                   => previousScope
    }
  }

  def generateReturnClause(
    defaultSymbols: List[String],
    yields: Option[Yield],
    returns: Option[Return],
    defaultOrder: Seq[String]
  ): String = {
    val yieldScope = getScope(defaultSymbols, yields)
    val returnScope = getScope(yieldScope, returns)

    val yieldColumns: Option[Yield] =
      yields.map(y => y.copy(orderBy = calcOrderBy(y, yieldScope, defaultOrder))(y.position))
    val explicitSort = yieldColumns.flatMap(y => y.orderBy.map(o => o.sortItems)).nonEmpty
    val returnColumns: Option[Return] =
      returns.map(r => r.copy(orderBy = calcOrderBy(r, returnScope, defaultOrder, explicitSort))(r.position))

    def symbolsToReturnItems(symbols: List[String]): List[ReturnItem] =
      symbols.map(s => UnaliasedReturnItem(Variable(s)(InputPosition.NONE), s)(InputPosition.NONE))

    val clauses = (yieldColumns, returnColumns) match {
      // YIELD with WHERE and no RETURN so convert YIELD / WHERE to WITH and YIELD to RETURN
      case (Some(y @ Yield(returnItems, orderBy, skip, limit, Some(where))), None) =>
        Seq(
          With(distinct = false, returnItems, orderBy, skip, limit, Some(where))(y.position),
          Return(distinct = false, generateReturnItemsFromAliases(returnItems), orderBy, skip, limit)(y.position)
        )
      // YIELD with no WHERE so convert YIELD to RETURN
      case (Some(y @ Yield(returnItems, orderBy, skip, limit, None)), None) =>
        Seq(Return(distinct = false, returnItems, orderBy, skip, limit)(y.position))
      // YIELD and RETURN so convert YIELD to WITH, and keep the RETURN
      case (Some(y @ Yield(returnItems, orderBy, skip, limit, where)), Some(returnClause)) =>
        Seq(With(distinct = false, returnItems, orderBy, skip, limit, where)(y.position), returnClause)
      // No YIELD or RETURN so just make up a RETURN with everything
      case (None, _) => Seq(Return(
          distinct = false,
          ReturnItems(includeExisting = false, symbolsToReturnItems(defaultSymbols))(InputPosition.NONE),
          genDefaultOrderBy(defaultSymbols, defaultOrder),
          None,
          None
        )(InputPosition.NONE))
    }
    clauses.map(prettifier.asString).mkString(" ")
  }

  private def generateReturnItemsFromAliases(ri: ReturnItems): ReturnItems = {
    // If variables are renamed in YIELD, we need to reflect those renamings in RETURN
    ri.mapItems(items =>
      items.map {
        case aliasedReturnItem: AliasedReturnItem     => AliasedReturnItem(aliasedReturnItem.alias.get)
        case unAliasedReturnItem: UnaliasedReturnItem => unAliasedReturnItem
      }
    )
  }

}
