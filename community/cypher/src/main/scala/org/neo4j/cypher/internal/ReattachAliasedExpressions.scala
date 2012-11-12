/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal

import commands._


/*
This rewriter rewrites expressions that come after the RETURN clause,
so the user can either use the raw expression, or the alias

These clauses are HAVING and ORDER BY right now.
 */
object ReattachAliasedExpressions {
  def apply(q: Query): Query = {
    val newSort = q.sort.map(oldSort => Sort(oldSort.sortItems.map(rewrite(q.returns.returnItems)): _*))

    q.copy(sort = newSort)
  }

  private def rewrite(returnItems: Seq[ReturnItem])(in: SortItem): SortItem = {
    SortItem(in.expression.rewrite(expressionRewriter(returnItems)), in.ascending)
  }

  private def expressionRewriter(returnItems: Seq[ReturnItem])(expression: Expression): Expression = expression match {
    case e: Entity => {
      val found = returnItems.find(_.columnName == e.entityName)

      found match {
        case None => e
        case Some(returnItem) => returnItem.expression
      }
    }
    case somethingElse => somethingElse
  }
}