/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.v4_0.rewriting.rewriters

import org.neo4j.cypher.internal.v4_0.ast.{Where, _}
import org.neo4j.cypher.internal.v4_0.expressions.{Expression, LogicalVariable, Variable}
import org.neo4j.cypher.internal.v4_0.util._

/**
  * This rewriter normalizes the scoping structure of a query, ensuring it is able to
  * be correctly processed for semantic checking. It makes sure that all return items
  * in WITH clauses are aliased.
  *
  * It also replaces expressions and subexpressions in ORDER BY and WHERE
  * to use aliases introduced by the WITH, where possible.
  *
  * Example:
  *
  * MATCH n
  * WITH n.prop AS prop ORDER BY n.prop DESC
  * RETURN prop
  *
  * This rewrite will change the query to:
  *
  * MATCH n
  * WITH n.prop AS prop ORDER BY prop DESC
  * RETURN prop AS prop
  */
case class normalizeWithAndReturnClauses(cypherExceptionFactory: CypherExceptionFactory) extends Rewriter {

  def apply(that: AnyRef): AnyRef = instance.apply(that)

  private val clauseRewriter: Clause => Clause = {
    // Only alias return items
    case clause@ProjectionClause(_, ri: ReturnItems, None, _, _, None) =>
      val (unaliasedReturnItems, aliasedReturnItems) = partitionReturnItems(ri.items, implicitlyAliasVariables)
      val initialReturnItems = unaliasedReturnItems ++ aliasedReturnItems
      clause.copyProjection(returnItems = ri.copy(items = initialReturnItems)(ri.position))

    // Alias return items and rewrite ORDER BY and WHERE
    case clause@ProjectionClause(distinct, ri: ReturnItems, orderBy, skip, limit, where) =>
      clause.verifyOrderByAggregationUse((s, i) => throw cypherExceptionFactory.syntaxException(s, i))
      val (unaliasedReturnItems, aliasedReturnItems) = partitionReturnItems(ri.items, implicitlyAliasVariables)
      val initialReturnItems = unaliasedReturnItems ++ aliasedReturnItems

      val existingAliases = aliasedReturnItems.map(i => i.expression -> i.alias.get.copyId).toMap
      val updatedOrderBy = orderBy.map(aliasOrderBy(existingAliases, _))
      val updatedWhere = where.map(aliasWhere(existingAliases, _))

      clause.copyProjection(returnItems = ri.copy(items = initialReturnItems)(ri.position), orderBy = updatedOrderBy, where = updatedWhere)

    // Not our business
    case clause =>
      clause
  }

  private def implicitlyAliasVariables(i: UnaliasedReturnItem): Option[AliasedReturnItem] =
    if (i.alias.isDefined) {
      Some(AliasedReturnItem(i.expression, i.alias.get.copyId)(i.position))
    } else {
      None
    }

  /**
    * Aliases return items if possible. Return a tuple of unaliased (because impossible) and
    * aliased (because they already were aliases or we just introduced an alias for them)
    * return items.
    *
    * @param aliasImplicitly UnaliasedReturnItems are rewritten to AliasedReturnItems if this function is defined.
    */
  private def partitionReturnItems(returnItems: Seq[ReturnItem],
                                   aliasImplicitly: UnaliasedReturnItem => Option[AliasedReturnItem]): (Seq[ReturnItem], Seq[AliasedReturnItem]) =
    returnItems.foldLeft((Vector.empty[ReturnItem], Vector.empty[AliasedReturnItem])) {
      case ((unaliasedItems, aliasedItems), item) => item match {
        case i: AliasedReturnItem =>
          (unaliasedItems, aliasedItems :+ i)

        case i: UnaliasedReturnItem =>
          aliasImplicitly(i) match {
            case Some(aliasedReturnItem) =>
              (unaliasedItems, aliasedItems :+ aliasedReturnItem)
            case None =>
              // Unaliased return items are preserved so that semantic check can report them as errors
              (unaliasedItems :+ item, aliasedItems)
          }
      }
    }

  /**
    * Given a list of existing aliases, this rewrites an OrderBy to use these where possible.
    */
  private def aliasOrderBy(existingAliases: Map[Expression, LogicalVariable], originalOrderBy: OrderBy): OrderBy = {
    val updatedSortItems = originalOrderBy.sortItems.map { aliasSortItem(existingAliases, _)}
    OrderBy(updatedSortItems)(originalOrderBy.position)
  }

  /**
    * Given a list of existing aliases, this rewrites a SortItem to use these where possible.
    */
  private def aliasSortItem(existingAliases: Map[Expression, LogicalVariable], sortItem: SortItem): SortItem = {
    sortItem match {
      case AscSortItem(expression) => AscSortItem(aliasExpression(existingAliases, expression))(sortItem.position)
      case DescSortItem(expression) => DescSortItem(aliasExpression(existingAliases, expression))(sortItem.position)
    }
  }

  /**
    * Given a list of existing aliases, this rewrites a where to use these where possible.
    */
  private def aliasWhere(existingAliases: Map[Expression, LogicalVariable], originalWhere: Where): Where = {
    Where(aliasExpression(existingAliases, originalWhere.expression))(originalWhere.position)
  }

  /**
    * Given a list of existing aliases, this rewrites expressions to use these where possible.
    */
  private def aliasExpression(existingAliases: Map[Expression, LogicalVariable], expression: Expression): Expression = {
    existingAliases.get(expression) match {
      case Some(alias) if !existingAliases.valuesIterator.contains(expression) =>
        alias.copyId
      case _ =>
        val newExpression = expression.endoRewrite(topDown(Rewriter.lift {
          case subExpression: Expression =>
            existingAliases.get(subExpression) match {
              case Some(subAlias) if !existingAliases.valuesIterator.contains(subExpression) => subAlias.copyId
              case _ => subExpression
            }
        }))
        newExpression
    }
  }

  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case query@SingleQuery(clauses) =>
      val finalClause =
        clauses.last match {
          case r @ Return(_, ri: ReturnItems, _, _, _, _) =>

            // All un-aliased return items in the final RETURN are OK
            val aliasedReturnItems =
              ri.items.map {
                case i: AliasedReturnItem => i
                case i: UnaliasedReturnItem =>
                  val newPosition = i.expression.position.bumped()
                  AliasedReturnItem(i.expression, Variable(i.name)(newPosition))(i.position)
              }
            r.copyProjection(returnItems = ri.copy(items = aliasedReturnItems)(ri.position))

          case clause => clause
      }
      query.copy(clauses = (clauses.init :+ finalClause).map(clauseRewriter))(query.position)
  })
}
