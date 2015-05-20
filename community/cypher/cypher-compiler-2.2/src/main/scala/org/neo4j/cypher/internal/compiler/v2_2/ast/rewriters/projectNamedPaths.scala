/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v2_2.Foldable._
import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.planner.CantHandleQueryException

import scala.annotation.tailrec

case object projectNamedPaths extends Rewriter {

  case class Projectibles(paths: Map[Identifier, PathExpression] = Map.empty,
                          protectedIdentifiers: Set[Ref[Identifier]] = Set.empty,
                          identifierRewrites: Map[Ref[Identifier], PathExpression] = Map.empty) {

    self =>

    def withoutNamedPaths = copy(paths = Map.empty)

    def withProtectedIdentifier(ident: Ref[Identifier]) = copy(protectedIdentifiers = protectedIdentifiers + ident)

    def withNamedPath(entry: (Identifier, PathExpression)) = copy(paths = paths + entry)

    def withRewrittenIdentifier(entry: (Ref[Identifier], PathExpression)) = {
      val (ref, pathExpr) = entry
      copy(identifierRewrites = identifierRewrites + (ref -> pathExpr.endoRewrite(copyIdentifiers)))
    }

    def returnItems = paths.map {
      case (ident, pathExpr) => AliasedReturnItem(pathExpr, ident)(ident.position)
    }.toSeq

    def withIdentifierRewritesForExpression(expr: Expression) =
      expr.treeFold(self) {
        case ident: Identifier =>
          (acc, children) =>
            acc.paths.get(ident) match {
              case Some(pathExpr) => children(acc.withRewrittenIdentifier(Ref(ident) -> pathExpr))
              case None => children(acc)
            }
      }
  }

  object Projectibles {
    val empty = Projectibles()
  }

  private def collectProjectibles(input: AnyRef): Projectibles = input.treeFold(Projectibles.empty) {
    case aliased: AliasedReturnItem =>
      (acc, children) =>
        children(acc.withProtectedIdentifier(Ref(aliased.identifier)))

    case ident: Identifier =>
      (acc, children) =>
        acc.paths.get(ident) match {
          case Some(pathExpr) => children(acc.withRewrittenIdentifier(Ref(ident) -> pathExpr))
          case None => children(acc)
        }

    // TODO: Project for use in WHERE
    // TODO: Pull out common subexpressions for path expr using WITH *, ... and run expand star again
    // TODO: Plan level rewriting to delay computation of unused projections

    case projection: With =>
      (acc, children) =>
        val projectedIdentifiers = projection.returnItems.items.flatMap(_.alias).toSet
        val projectedAcc = projection.returnItems.items.map(_.expression).foldLeft(acc) {
          (acc, expr) => acc.withIdentifierRewritesForExpression(expr)
        }
        children(projectedAcc.withoutNamedPaths)

    case NamedPatternPart(_, part: ShortestPaths) =>
      (acc, children) => children(acc)

    case part @ NamedPatternPart(identifier, patternPart) =>
      (acc, children) =>
        val pathExpr = PathExpression(patternPartPathExpression(patternPart))(part.position)
        children(acc.withNamedPath(identifier -> pathExpr).withProtectedIdentifier(Ref(identifier)))
  }

  def patternPartPathExpression(patternPart: AnonymousPatternPart): PathStep = patternPart match {
    case EveryPath(element) => flip(element, NilPathStep)
    case _                  => throw new CantHandleQueryException
  }

  @tailrec
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

  def apply(input: AnyRef): AnyRef = {
    val Projectibles(paths, protectedIdentifiers, identifierRewrites) = collectProjectibles(input)
    val applicator = Rewriter.lift {
      case namedPart@NamedPatternPart(_, _: ShortestPaths) =>
        namedPart

//      case query @ SingleQuery(clauses) =>
//        val newClauses = clauses.flatMap {
//          case matchClause@Match(_, pattern, _, Some(where)) =>
//            val namedPaths = pattern.patternParts.flatMap {
//              case NamedPatternPart(_, _: ShortestPaths) => None
//              case part: NamedPatternPart                => Some(part.identifier)
//            }
//            val newMatchClause = matchClause.copy(where = None)(matchClause.position)
//            val extraReturnItems = namedPaths.map { ident =>
//              AliasedReturnItem(paths(ident), ident.copyId)(ident.position)
//            }
//            val pathNames = namedPaths.toSet
//            val newWhere = bottomUp(Rewriter.lift {
//              case ident: Identifier if pathNames(ident) => ident.copyId
//            })(where).asInstanceOf[Where]
//            val newWithClause = With(distinct = false, ReturnItems(includeExisting = true, extraReturnItems)(where.position), None, None, None, Some(newWhere))(where.position)
//            Seq(newMatchClause, newWithClause)
//
//          case clause =>
//            Seq(clause)
//        }
//        query.copy(clauses = newClauses)(query.position)

      case NamedPatternPart(_, part) =>
        part

      case expr: PathExpression =>
        expr

      case (ident: Identifier) if !protectedIdentifiers(Ref(ident))=>
        identifierRewrites.getOrElse(Ref(ident), ident)
    }
    val result = topDown(applicator)(input)
    result
  }
}
