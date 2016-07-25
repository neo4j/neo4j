/*
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
package org.neo4j.cypher.internal.compiler.v3_1.ast.rewriters

import org.neo4j.cypher.internal.compiler.v3_1.planner.CantHandleQueryException
import org.neo4j.cypher.internal.frontend.v3_1.Foldable._
import org.neo4j.cypher.internal.frontend.v3_1.ast._
import org.neo4j.cypher.internal.frontend.v3_1.{Ref, Rewriter, topDown}

import scala.annotation.tailrec

case object projectNamedPaths extends Rewriter {

  case class Projectibles(paths: Map[Variable, PathExpression] = Map.empty,
                          protectedVariables: Set[Ref[Variable]] = Set.empty,
                          variableRewrites: Map[Ref[Variable], PathExpression] = Map.empty) {

    self =>

    def withoutNamedPaths = copy(paths = Map.empty)
    def withProtectedVariable(ident: Ref[Variable]) = copy(protectedVariables = protectedVariables + ident)
    def withNamedPath(entry: (Variable, PathExpression)) = copy(paths = paths + entry)
    def withRewrittenVariable(entry: (Ref[Variable], PathExpression)) = {
      val (ref, pathExpr) = entry
      copy(variableRewrites = variableRewrites + (ref -> pathExpr.endoRewrite(copyVariables)))
    }

    def returnItems = paths.map {
      case (ident, pathExpr) => AliasedReturnItem(pathExpr, ident)(ident.position)
    }.toSeq

    def withVariableRewritesForExpression(expr: Expression) =
      expr.treeFold(self) {
        case ident: Variable =>
          acc =>
            acc.paths.get(ident) match {
              case Some(pathExpr) => (acc.withRewrittenVariable(Ref(ident) -> pathExpr), Some(identity))
              case None => (acc, Some(identity))
            }
      }
  }

  object Projectibles {
    val empty = Projectibles()
  }

  def apply(input: AnyRef): AnyRef = {
    val Projectibles(paths, protectedVariables, variableRewrites) = collectProjectibles(input)
    val applicator = Rewriter.lift {

      case (ident: Variable) if !protectedVariables(Ref(ident)) =>
        variableRewrites.getOrElse(Ref(ident), ident)

      case namedPart@NamedPatternPart(_, _: ShortestPaths) =>
        namedPart

      case NamedPatternPart(_, part) =>
        part

      case expr: PathExpression =>
        expr
    }
    topDown(applicator)(input)
  }

  private def collectProjectibles(input: AnyRef): Projectibles = input.treeFold(Projectibles.empty) {
    case aliased: AliasedReturnItem =>
      acc =>
        (acc.withProtectedVariable(Ref(aliased.variable)), Some(identity))

    case ident: Variable =>
      acc =>
        acc.paths.get(ident) match {
          case Some(pathExpr) => (acc.withRewrittenVariable(Ref(ident) -> pathExpr), Some(identity))
          case None => (acc, Some(identity))
        }

    // Optimization 1
    //
    // M p = ... WHERE ..., p, p, ...
    // M p = ... WITH a AS a, b AS b, PathExpr AS p WHERE ...

    // Optimization 2
    //
    // plan rewriter for pushing projections up the tree

    // TODO: Project for use in WHERE
    // TODO: Pull out common subexpressions for path expr using WITH *, ... and run expand star again
    // TODO: Plan level rewriting to delay computation of unused projections

    case projection: With =>
      acc =>
        val projectedAcc = projection.returnItems.items.map(_.expression).foldLeft(acc) {
          (acc, expr) => acc.withVariableRewritesForExpression(expr)
        }
        (projectedAcc.withoutNamedPaths, Some(identity))

    case NamedPatternPart(_, part: ShortestPaths) =>
      acc => (acc, Some(identity))

    case part @ NamedPatternPart(variable, patternPart) =>
      acc =>
        val pathExpr = PathExpression(patternPartPathExpression(patternPart))(part.position)
        (acc.withNamedPath(variable -> pathExpr).withProtectedVariable(Ref(variable)), Some(identity))
  }

  def patternPartPathExpression(patternPart: AnonymousPatternPart): PathStep = patternPart match {
    case EveryPath(element) => patternPartPathExpression(element)
    case _                  => throw new CantHandleQueryException
  }

  def patternPartPathExpression(element: PatternElement): PathStep = flip(element, NilPathStep)

  @tailrec
  private def flip(element: PatternElement, step: PathStep): PathStep  = {
    element match {
      case NodePattern(node, _, _) =>
        NodePathStep(node.get.copyId, step)

      case RelationshipChain(relChain, RelationshipPattern(rel, _, _, length, _, direction), _) => length match {
        case None =>
          flip(relChain, SingleRelationshipPathStep(rel.get.copyId, direction, step))

        case Some(_) =>
          flip(relChain, MultiRelationshipPathStep(rel.get.copyId, direction, step))
      }
    }
  }
}
