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
import org.neo4j.cypher.internal.compiler.v2_1.planner.CantHandleQueryException

case class InliningContext(projections: Map[Identifier, Expression] = Map.empty,
                           stagedRewrites: Map[Identifier, Expression] = Map.empty) {

  def enterQueryPart(projections: Map[Identifier, Expression]): InliningContext =
    copy(projections = projections ++ projections)

  def spoilIdentifier(identifier: Identifier): InliningContext =
    copy(projections = projections - identifier)

  def inlineIfPossible(from: Identifier) = projections.get(from) match {
    case Some(rewriteTo) => copy(stagedRewrites = stagedRewrites + (from -> rewriteTo))
    case None => this
  }
}

object inlineProjections extends (Statement => Statement) {
  def apply(input: Statement): Statement = {

    val context = input.treeFold(InliningContext()) {
      case (With(false, ListedReturnItems(items), None, None, None, None)) =>
        (context, children) => {
          val map = items.map(item => (item.alias, item.expression)).flatMap {
            case (Some(alias), expr) => Some(alias, expr)
            case _ => None
          }.toMap
          children(context.enterQueryPart(map))
        }
      case Match(_, Pattern(parts), _, _) =>
        (context, children) =>
          children(context.enterQueryPart(parts.collect {
            case part @ NamedPatternPart(identifier, patternPart) =>
              identifier -> PathExpression(patternPartPathExpression(patternPart))(part.position)
          }.toMap))
      case NodePattern(Some(identifier), _, _, _) =>
        (context, children) => children(context.spoilIdentifier(identifier))
      case RelationshipPattern(Some(identifier), _, _, _, _, _) =>
        (context, children) => children(context.spoilIdentifier(identifier))
      case id: Identifier =>
        (context, children) => children(context.inlineIfPossible(id))
    }

    val namedPatternRewriter: Rewriter = bottomUp(Rewriter.lift {
      case NamedPatternPart(_, part) => part
    })

    val identifierRewriter: Rewriter = bottomUp(Rewriter.lift {
      case identifier: Identifier =>
        context.projections.get(identifier) match {
          case Some(expr) => expr
          case _ => identifier
      }
    })

    def rewriteClauses(expr0: Clause): Clause = {
      val expr1 = expr0.typedRewrite[Clause](namedPatternRewriter)
      val expr2 = expr1.typedRewrite[Clause](identifierRewriter)
      expr2
    }

    // WITH a AS b WITH b + 1 AS c
    val rewriter: Rewriter = Rewriter.lift {
      case clause @ With(false, items@ListedReturnItems(listedItems), orderBy, None, None, None) =>

          val newItems = listedItems.collect {
            case item@AliasedReturnItem(expr, identifier)
              if !containsAggregation(item.expression) && !context.projections.contains(identifier) => item
          }
          val newListedItems: ReturnItems = newItems match {
            case Seq() => ReturnAll()(items.position)
            case remaining => items.copy(
              items = remaining.map {
                case item: AliasedReturnItem =>
                  item.copy( expression = item.expression.typedRewrite[Expression](identifierRewriter) )(item.position)
              }
            )(items.position)
          }

          clause.copy(
            returnItems = newListedItems,
            orderBy = orderBy.map(_.typedRewrite[OrderBy](identifierRewriter))
          )(clause.position)

      case clause @ Return(_, returnItems @ ListedReturnItems(items), orderBy, _, _) =>
        clause.copy(
          returnItems = returnItems.copy(
            items = items.map {
              case item: AliasedReturnItem =>
                item.copy( expression = item.expression.typedRewrite[Expression](identifierRewriter) )(item.position)

              case item: UnaliasedReturnItem =>
                item.copy( expression = item.expression.typedRewrite[Expression](identifierRewriter) )(item.position)
            }
          )(returnItems.position),
          orderBy = orderBy.map(_.typedRewrite[OrderBy](identifierRewriter))
        )(clause.position)

      case clause: Clause =>
        rewriteClauses(clause)
    }

    input.rewrite(topDown(rewriter)).asInstanceOf[Statement]
  }

  private def containsAggregation(expr: ast.Expression) = {
    expr.treeFold[Boolean](false) {
      case fi: CountStar =>
        (acc, children) =>
          true
      case fi: FunctionInvocation =>
        (acc, children) =>
          fi.distinct || fi.function.exists(_.isInstanceOf[AggregatingFunction]) || children(acc)
    }
  }

  private def patternPartPathExpression(patternPart: AnonymousPatternPart): PathStep = patternPart match {
    case EveryPath(element) => patternElementPathExpression(element)
    case _                  => throw new CantHandleQueryException
  }

  // MATCH (a)-[r]->(b)-[r2]->(c)
  private def patternElementPathExpression(patternElement: PatternElement): PathStep =
    flip(patternElement, NilPathStep)

  private def flip(element: PatternElement, step: PathStep): PathStep  = {
    element match {
      case NodePattern(node, _, _, _) =>
        NodePathStep(node.get, step)

      case RelationshipChain(relChain, RelationshipPattern(rel, _, _, length, _, direction), _) => length match {
        case None =>
          flip(relChain, SingleRelationshipPathStep(rel.get, direction, step))

        case Some(_) =>
          flip(relChain, MultiRelationshipPathStep(rel.get, direction, step))
      }
    }
  }
}
