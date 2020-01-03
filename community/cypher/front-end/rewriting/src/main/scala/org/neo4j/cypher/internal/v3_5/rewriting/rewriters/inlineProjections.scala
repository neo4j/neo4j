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
package org.neo4j.cypher.internal.v3_5.rewriting.rewriters

import org.neo4j.cypher.internal.v3_5.ast._
import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.util._
import org.neo4j.cypher.internal.v3_5.util.helpers.fixedPoint
import org.neo4j.cypher.internal.v3_5.ast.Statement
import org.neo4j.cypher.internal.v3_5.expressions.Pattern


case object inlineProjections extends Rewriter {

  def apply(in: AnyRef): AnyRef = instance(in)

  private val instance = Rewriter.lift { case input: Statement =>
    val context = inliningContextCreator(input)

    val inlineVariables = TypedRewriter[ASTNode](context.variableRewriter)
    val inlinePatterns = TypedRewriter[Pattern](context.patternRewriter)
    val inlineReturnItemsInWith = Rewriter.lift(aliasedReturnItemRewriter(inlineVariables.narrowed, context, inlineAliases = true))
    val inlineReturnItemsInReturn = Rewriter.lift(aliasedReturnItemRewriter(inlineVariables.narrowed, context, inlineAliases = false))

    val inliningRewriter: Rewriter = Rewriter.lift {
      case withClause: With if !withClause.distinct =>
        withClause.copy(
          returnItems = withClause.returnItems.rewrite(inlineReturnItemsInWith).asInstanceOf[ReturnItems],
          where = withClause.where.map(inlineVariables.narrowed)
        )(withClause.position)

      case returnClause: Return =>
        returnClause.copy(
          returnItems = returnClause.returnItems.rewrite(inlineReturnItemsInReturn).asInstanceOf[ReturnItems]
        )(returnClause.position)

      case m @ Match(_, mPattern, mHints, mOptWhere) =>
        val newOptWhere = mOptWhere.map(inlineVariables.narrowed)
        val newHints = mHints.map(inlineVariables.narrowed)
        // no need to inline expressions in patterns since all expressions have been moved to WHERE prior to
        // calling inlineProjections
        val newPattern = inlinePatterns(mPattern)
        m.copy(pattern = newPattern, hints = newHints, where = newOptWhere)(m.position)

      case _: UpdateClause  =>
        throw new InternalException("Update clauses not excepted here")

      case clause: Clause =>
        inlineVariables.narrowed(clause)
    }

    input.endoRewrite(topDown(inliningRewriter, _.isInstanceOf[Expression]))
  }


  private def findAllDependencies(variable: LogicalVariable, context: InliningContext): Set[LogicalVariable] = {
    val (dependencies, _) = fixedPoint[(Set[LogicalVariable], List[LogicalVariable])]({
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
    })((Set(variable), List(variable)))
    dependencies
  }

  private def aliasedReturnItemRewriter(inlineExpressions: Expression => Expression, context: InliningContext,
                                        inlineAliases: Boolean): PartialFunction[AnyRef, AnyRef] = {
    case ri: ReturnItems =>
      val newItems = ri.items.flatMap {
        case item: AliasedReturnItem
          if context.okToRewrite(item.variable) && inlineAliases =>
          val dependencies = findAllDependencies(item.variable, context)
          if (dependencies == Set(item.variable)) {
            IndexedSeq(item)
          } else {
            dependencies.map { id =>
              AliasedReturnItem(id.copyId, id.copyId)(item.position)
            }.toIndexedSeq
          }
        case item: AliasedReturnItem => IndexedSeq(
          item.copy(expression = inlineExpressions(item.expression))(item.position)
        )
      }
      ri.copy(items = newItems)(ri.position)
  }
}