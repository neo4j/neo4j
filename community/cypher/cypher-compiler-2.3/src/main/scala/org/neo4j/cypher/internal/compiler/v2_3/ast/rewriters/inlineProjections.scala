/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters

import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.compiler.v2_3.helpers.Converge.iterateUntilConverged
import org.neo4j.cypher.internal.compiler.v2_3.planner.CantHandleQueryException
import org.neo4j.cypher.internal.frontend.v2_3.{replace, Rewriter, TypedRewriter}

case object inlineProjections extends Rewriter {

  def apply(in: AnyRef): AnyRef = instance.apply(in)

  val instance = Rewriter.lift { case input: Statement =>
    val context = inliningContextCreator(input)

    val inlineIdentifiers = TypedRewriter[ASTNode](context.identifierRewriter)
    val inlinePatterns = TypedRewriter[Pattern](context.patternRewriter)
    val inlineReturnItemsInWith = Rewriter.lift(aliasedReturnItemRewriter(inlineIdentifiers.narrowed, context, inlineAliases = true))
    val inlineReturnItemsInReturn = Rewriter.lift(aliasedReturnItemRewriter(inlineIdentifiers.narrowed, context, inlineAliases = false))

    val inliningRewriter: Rewriter = replace(replacer => {
      case expr: Expression =>
        replacer.stop(expr)

      case withClause: With if !withClause.distinct =>
        withClause.copy(
          returnItems = withClause.returnItems.rewrite(inlineReturnItemsInWith).asInstanceOf[ReturnItems],
          where = withClause.where.map(inlineIdentifiers.narrowed)
        )(withClause.position)

      case returnClause: Return =>
        returnClause.copy(
          returnItems = returnClause.returnItems.rewrite(inlineReturnItemsInReturn).asInstanceOf[ReturnItems]
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

      case astNode =>
        replacer.expand(astNode)
    })

    input.endoRewrite(inliningRewriter)
  }

  private def findAllDependencies(identifier: Identifier, context: InliningContext): Set[Identifier] = {
    val (dependencies, _) = iterateUntilConverged[(Set[Identifier], List[Identifier])]({
      case (deps, Nil) =>
        (deps, Nil)
      case (deps, queue) =>
        val id :: tail = queue
        context.projections.get(id) match {
          case Some(expr) =>
            val exprDependencies = expr.dependencies
            (deps - id ++ exprDependencies, (exprDependencies -- deps).toList ++ tail)
          case None =>
            (deps + id, queue)
        }
    })((Set(identifier), List(identifier)))
    dependencies
  }

  private def aliasedReturnItemRewriter(inlineExpressions: Expression => Expression, context: InliningContext,
                                        inlineAliases: Boolean): PartialFunction[AnyRef, AnyRef] = {
    case ri: ReturnItems =>
      val newItems = ri.items.flatMap {
        case item: AliasedReturnItem
          if context.okToRewrite(item.identifier) && inlineAliases =>
          val dependencies = findAllDependencies(item.identifier, context)
          if (dependencies == Set(item.identifier)) {
            Seq(item)
          } else {
            dependencies.map { id =>
              AliasedReturnItem(id.copyId, id.copyId)(item.position)
            }.toSeq
          }
        case item: AliasedReturnItem => Seq(
          item.copy(expression = inlineExpressions(item.expression))(item.position)
        )
      }
      ri.copy(items = newItems)(ri.position)
  }
}
