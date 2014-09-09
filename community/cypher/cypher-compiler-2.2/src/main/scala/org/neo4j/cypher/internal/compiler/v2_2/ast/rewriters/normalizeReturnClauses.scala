/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.helpers.FreshIdNameGenerator
import org.neo4j.cypher.internal.compiler.v2_2.{Rewriter, bottomUp, topDown}

/**
 * This rewriter makes sure that all return items in a RETURN clauses are aliased, and moves
 * any ORDER BY to a preceding WITH clause
 *
 * Example:
 *
 * MATCH (n)
 * RETURN n.foo AS foo, n.bar ORDER BY foo
 *
 * This rewrite will change the query to:
 *
 * MATCH (n)
 * WITH n.foo AS foo, n.bar AS `  FRESHIDnn` ORDER BY foo
 * RETURN foo AS foo, `  FRESHIDnn` AS `n.bar`
 */
case object normalizeReturnClauses extends Rewriter {

  def apply(that: AnyRef): Option[AnyRef] = bottomUp(instance).apply(that)

  private val clauseRewriter: (Clause => Seq[Clause]) = {
    case clause @ Return(_, returnItemList: ListedReturnItems, None, _, _) =>
      val aliasedItems = returnItemList.items.map({
        case i: AliasedReturnItem =>
          i
        case i =>
          AliasedReturnItem(i.expression, Identifier(i.name)(i.expression.position))(i.position)
      })
      Seq(
        clause.copy(returnItems = ListedReturnItems(aliasedItems)(returnItemList.position))(clause.position)
      )

    case clause @ Return(distinct, returnItemList: ListedReturnItems, orderBy, skip, limit) =>
      val (aliasProjection, finalProjection) = returnItemList.items.map(i => {
        if (i.alias.isDefined) {
          (i, AliasedReturnItem(i.alias.get, i.alias.get)(i.position))
        } else {
          val newIdentifier = Identifier(FreshIdNameGenerator.name(i.expression.position))(i.position)
          (AliasedReturnItem(i.expression, newIdentifier)(i.position), AliasedReturnItem(newIdentifier, Identifier(i.name)(i.expression.position))(i.position))
        }
      }).unzip

      Seq(
        With(distinct = distinct, returnItems = ListedReturnItems(aliasProjection)(returnItemList.position), orderBy = orderBy, skip = skip, limit = limit, where = None)(clause.position),
        Return(distinct = false, returnItems = ListedReturnItems(finalProjection)(returnItemList.position), orderBy = None, skip = None, limit = None)(clause.position)
      )

    case clause =>
      Seq(clause)
  }

  private val instance: Rewriter = Rewriter.lift {
    case query @ SingleQuery(clauses) =>
      query.copy(clauses = clauses.flatMap(clauseRewriter))(query.position)
  }
}
