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
 * This rewriter ensures that WITH clauses containing a ORDER BY or WHERE are split, such that the ORDER BY or WHERE does not
 * refer to any newly introduced identifier.
 *
 * This is required due to constraints in the planner. Note that this structure is invalid for semantic checking, which requires
 * that ORDER BY and WHERE _only refer to identifiers introduced in the associated WITH_.
 *
 * Additionally, it splits RETURN clauses containing ORDER BY. This would typically be done earlier during normalizeReturnClauses, however
 * "RETURN * ORDER BY" is not handled at that stage, due to lacking identifier information. If expandStar has already been run, then this
 * will now work as expected.
 */
case object projectFreshSortExpressions extends Rewriter {

  def apply(that: AnyRef): Option[AnyRef] = bottomUp(instance).apply(that)

  private val clauseRewriter: (Clause => Seq[Clause]) = {
    case clause @ With(_, _, None, _, _, None) =>
      Seq(clause)

    case clause @ With(_, returnItemList: ListedReturnItems, _, _, _, _) =>
      val duplicateProjection = returnItemList.items.map(item =>
        item.alias.fold(item)(alias => AliasedReturnItem(alias, alias)(item.position))
      )

      Seq(
        clause.copy(orderBy = None, skip = None, limit = None, where = None)(clause.position),
        clause.copy(distinct = false, returnItems = ListedReturnItems(duplicateProjection)(returnItemList.position))(clause.position)
      )

    case r@Return(distinct, lri@ListedReturnItems(_), Some(orderBy), skip, limit) =>
      val (firstProjection, secondProjection, identifierItems, newOrderBy) = splitupClosingClause(lri, orderBy)
      Seq(
        With(distinct = false, returnItems = ListedReturnItems(firstProjection)(lri.position), orderBy = None, skip = None, limit = None, where = None)(r.position),
        With(distinct = false, returnItems = ListedReturnItems(secondProjection)(lri.position), orderBy = None, skip = None, limit = None, where = None)(r.position),
        Return(distinct, returnItems = ListedReturnItems(identifierItems)(lri.position), orderBy = Some(newOrderBy), skip = skip, limit = limit)(r.position)
      )

    case clause =>
      Seq(clause)
  }

  private def splitupClosingClause(returnItems: ListedReturnItems, orderBy: OrderBy): (Seq[ReturnItem], Seq[ReturnItem], Seq[ReturnItem], OrderBy) = {
    val aliases = returnItems.items

    val (aliasDependencies, nonAliasDependencies) = orderBy.treeFold(Seq.empty[Identifier]) {
      case id: Identifier => (acc, children) => children(acc :+ id)
    }.partition(id => aliases.exists(_.name == id.name))

    val sortExpressionMap = orderBy.sortItems.map(_.expression).map {
      case id: Identifier => id -> id
      case expr: Expression => expr -> Identifier(FreshIdNameGenerator.name(expr.position))(expr.position)
    }.toMap

    val sortExpressions = sortExpressionMap.map {
      case (expr, id) => AliasedReturnItem(expr, id)(expr.position)
    }.toSeq

    val first = nonAliasDependencies.map {
      id => AliasedReturnItem(id.copy()(id.position), id)(id.position)
    }.toSeq ++ returnItems.items
    val second = aliases.map {
      item => AliasedReturnItem(
        Identifier(item.name)(item.position),
        Identifier(item.name)(item.position)
      )(item.position)
    }

    val newOrderBy = orderBy.endoRewrite(topDown(Rewriter.lift {
      case expr: Expression => sortExpressionMap(expr)
    }))
    (first.distinct, (second ++ sortExpressions).distinct, second.distinct, newOrderBy)
  }

  private val instance: Rewriter = Rewriter.lift {
    case query @ SingleQuery(clauses) =>
      query.copy(clauses = clauses.flatMap(clauseRewriter))(query.position)
  }
}
