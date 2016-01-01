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
package org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.compiler.v2_1.planner.CantHandleQueryException

object inlineProjections extends Rewriter {

  def apply(in: AnyRef): Option[AnyRef] = instance.apply(in)

  val instance = Rewriter.lift { case input: Statement =>
    val context = inliningContextCreator(input)

    val removePatternPartNames = TypedRewriter[Pattern](bottomUp(namedPatternPartRemover))
    val inlineIdentifiers = TypedRewriter[ASTNode](context.identifierRewriter)
    val inlinePatterns = TypedRewriter[Pattern](context.patternRewriter)
    val inlineReturnItems = inlineReturnItemsFactory(inlineIdentifiers.narrowed)

    val inliningRewriter = Rewriter.lift {
      case withClause @ With(false, returnItems @ ListedReturnItems(items), orderBy, _, _, _) =>
        val pos = returnItems.position

        def identifierOnly(item: AliasedReturnItem) = item.expression == item.identifier
        def identifierAlreadyInScope(ident: Identifier) = !context.projections.contains(ident)
        def shadows(item: AliasedReturnItem) = identifierAlreadyInScope(item.identifier) && !identifierOnly(item)

        // Do not remove grouping keys from a WITH with aggregation.
        val containsAggregation = items.map(_.expression).exists(containsAggregate)
        val resultProjections = if (containsAggregation) {
          items
        } else {
          items.collect {
            case item@AliasedReturnItem(expr, ident) if shadows(item) => item
          }
        }

        val newReturnItems =
          if (resultProjections.isEmpty) ReturnAll()(pos)
          else inlineReturnItems(returnItems.copy(items = resultProjections)(pos))

        withClause.copy(
          returnItems = newReturnItems,
          orderBy = orderBy.map(inlineIdentifiers.narrowed)
        )(withClause.position)

      case returnClause @ Return(_, returnItems: ListedReturnItems, orderBy, skip, limit) =>
        returnClause.copy(
          returnItems = inlineReturnItems(returnItems),
          orderBy = orderBy.map(inlineIdentifiers.narrowed),
          skip = skip.map(inlineIdentifiers.narrowed),
          limit = limit.map(inlineIdentifiers.narrowed)
        )(returnClause.position)

      case m @ Match(_, mPattern, mHints, mOptWhere) =>
        val newOptWhere = mOptWhere.map(inlineIdentifiers.narrowed)
        val newHints = mHints.map(inlineIdentifiers.narrowed)
        // no need to inline expressions in patterns since all expressions have been moved to WHERE prior to
        // calling inlineProjections
        val newPattern = inlinePatterns(removePatternPartNames(mPattern))
        m.copy(pattern = newPattern, hints = newHints, where = newOptWhere)(m.position)

      case _: UpdateClause  =>
        throw new CantHandleQueryException

      case clause: Clause =>
        inlineIdentifiers.narrowed(clause)
    }

    input.endoRewrite(topDown(inliningRewriter))
  }

  private def inlineReturnItemsFactory(inlineExpressions: Expression => Expression) =
    (returnItems: ListedReturnItems) =>
      returnItems.copy(
        items = returnItems.items.collect {
          case item: AliasedReturnItem =>
            item.copy( expression = inlineExpressions(item.expression))(item.position)
        }
      )(returnItems.position)
}
