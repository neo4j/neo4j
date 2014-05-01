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
package org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1._

object inlineProjections extends (Statement => Statement) {

  def apply(input: Statement): Statement = {
    val context = inliningContextCreator(input)

    val removePatternPartNames = TypedRewriter[Match](bottomUp(namedPatternPartRemover))
    val inlineExpressionsRewriter = context.identifierRewriter
    val inlineExpressions = TypedRewriter[Expression](inlineExpressionsRewriter)
    val inlineClause = TypedRewriter[Clause](inlineExpressionsRewriter)
    val inlineOrderBy = TypedRewriter[OrderBy](inlineExpressionsRewriter)
    val inlineReturnItems = inlineReturnItemsFactory(inlineExpressions)

    val inliningRewriter = Rewriter.lift {
      case clause @ With(false, returnItems @ ListedReturnItems(items), orderBy, None, None, None) =>
        val pos = returnItems.position
        val filteredItems = items.collect {
          case item@AliasedReturnItem(expr, ident) if !context.projections.contains(ident) || containsAggregate(expr) =>
            item
        }

        val newReturnItems =
          if (filteredItems.isEmpty) ReturnAll()(pos)
          else inlineReturnItems(returnItems.copy(items = filteredItems)(pos))

        clause.copy(
          returnItems = newReturnItems,
          orderBy = orderBy.map(inlineOrderBy)
        )(clause.position)

      case clause @ Return(_, returnItems: ListedReturnItems, orderBy, _, _) =>
        clause.copy(
          returnItems = inlineReturnItems(returnItems),
          orderBy = orderBy.map(inlineOrderBy)
        )(clause.position)

      case clause: Match =>
        val withoutPatternNames = removePatternPartNames(clause)
        inlineClause(withoutPatternNames)

      case clause: Clause =>
        inlineClause(clause)
    }

    input.rewrite(topDown(inliningRewriter)).asInstanceOf[Statement]
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



