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
package org.neo4j.cypher.internal.compiler.v1_9

import commands._
import expressions.{Identifier, Expression}


/*
This rewriter rewrites expressions that come after the RETURN clause,
so the user can either use the raw expression, or the alias
 */
object ReattachAliasedExpressions {
  def apply(q: Query): Query = {
    val newSort = q.sort.map(rewrite(q.returns.returnItems))

    q.copy(sort = newSort, tail = q.tail.map(apply))
  }

  private def rewrite(returnItems: Seq[ReturnColumn])(in: SortItem): SortItem = {
    SortItem(in.expression.rewrite(expressionRewriter(returnItems)), in.ascending)
  }

  private def expressionRewriter(returnColumns: Seq[ReturnColumn])(expression: Expression): Expression = expression match {
    case e@Identifier(entityName) =>
      val returnItems = keepReturnItems(returnColumns)
      val found = returnItems.find(_.name == e.entityName)

      found match {
        case None             => e
        case Some(returnItem) => returnItem.expression
      }

    case somethingElse => somethingElse
  }

  private def keepReturnItems(returnColumns: Seq[ReturnColumn]):Seq[ReturnItem] = returnColumns.
    filter(_.isInstanceOf[ReturnItem]).
    map(_.asInstanceOf[ReturnItem])
}
