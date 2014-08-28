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
import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.planner.CantHandleQueryException
import Foldable._

object inlineProjections extends Rewriter {

  def apply(in: AnyRef): Option[AnyRef] = instance.apply(in)

  val instance = Rewriter.lift { case input: Statement =>
    val context = inliningContextCreator(input)

    val inlineIdentifiers = TypedRewriter[ASTNode](context.identifierRewriter)
    val inlinePatterns = TypedRewriter[Pattern](context.patternRewriter)
    val inlineReturnItemsInWith = Rewriter.lift(aliasedReturnItemRewriter(inlineIdentifiers.narrowed, context, inlineAliases = true))
    val inlineReturnItemsInReturn = Rewriter.lift(aliasedReturnItemRewriter(inlineIdentifiers.narrowed, context, inlineAliases = false))

    val inliningRewriter = Rewriter.lift {
      case withClause @ With(false, returnItems @ ListedReturnItems(items), orderBy, _, _, _) =>
        withClause.copy(
          returnItems = returnItems.rewrite(inlineReturnItemsInWith).asInstanceOf[ReturnItems]
        )(withClause.position)

      case returnClause @ Return(_, returnItems: ListedReturnItems, orderBy, skip, limit) =>
        returnClause.copy(
          returnItems = returnItems.rewrite(inlineReturnItemsInReturn).asInstanceOf[ReturnItems]
        )(returnClause.position)

      case m @ Match(_, mPattern, mHints, mOptWhere) =>
        val newOptWhere = mOptWhere.map(inlineIdentifiers.narrowed)
        val newHints = mHints.map(inlineIdentifiers.narrowed)
        // no need to inline expressions in patterns since all expressions have been moved to WHERE prior to
        // calling inlineProjections
        val newPattern = inlinePatterns(mPattern)
        m.copy(pattern = newPattern, hints = newHints, where = newOptWhere)(m.position)

      case _: UpdateClause  =>
        throw new CantHandleQueryException

      case clause: Clause =>
        inlineIdentifiers.narrowed(clause)
    }

    input.endoRewrite(topDown(inliningRewriter))
  }

  private def aliasedReturnItemRewriter(inlineExpressions: Expression => Expression, context: InliningContext,
                                        inlineAliases: Boolean): PartialFunction[AnyRef, AnyRef] = {
    case lri @ ListedReturnItems(items) =>
      val newItems = items.flatMap {
        case item: AliasedReturnItem
          if context.projections.contains(item.identifier) && inlineAliases =>
          val expr = context.projections(item.identifier)
          val dependencies = expr.treeFold(Set.empty[Identifier]) {
            case id: Identifier if !context.projections.contains(id) || id == expr =>
              (acc, children) => children(acc + id)
          }
          dependencies.map { id =>
            AliasedReturnItem(id.copy()(id.position), id.copy()(id.position))(item.position)
          }.toSeq
        case item: AliasedReturnItem => Seq(
          item.copy(expression = inlineExpressions(item.expression))(item.position)
        )
      }
      if (newItems.isEmpty) {
        ReturnAll()(lri.position)
      } else {
        lri.copy(items = newItems)(lri.position)
      }
  }
}
