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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.ProjectionClause
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.AnonymousPatternPart
import org.neo4j.cypher.internal.expressions.EveryPath
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MultiRelationshipPathStep
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.PathStep
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.ShortestPaths
import org.neo4j.cypher.internal.expressions.SingleRelationshipPathStep
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildrenNewAccForSiblings
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.topDown

import scala.annotation.tailrec

case object projectNamedPaths extends Rewriter {

  case class Projectibles(paths: Map[Variable, PathExpression] = Map.empty,
                          protectedVariables: Set[Ref[LogicalVariable]] = Set.empty,
                          variableRewrites: Map[Ref[LogicalVariable], PathExpression] = Map.empty) {

    self =>

    def withoutNamedPaths: Projectibles = copy(paths = Map.empty)
    def withProtectedVariable(ident: Ref[LogicalVariable]): Projectibles = copy(protectedVariables = protectedVariables + ident)
    def withNamedPath(entry: (Variable, PathExpression)): Projectibles = copy(paths = paths + entry)
    def withRewrittenVariable(entry: (Ref[LogicalVariable], PathExpression)): Projectibles = {
      val (ref, pathExpr) = entry
      copy(variableRewrites = variableRewrites + (ref -> pathExpr.endoRewrite(copyVariables)))
    }

    def returnItems: IndexedSeq[AliasedReturnItem] = paths.map {
      case (ident, pathExpr) => AliasedReturnItem(pathExpr, ident)(ident.position)
    }.toIndexedSeq

    def withVariableRewritesForExpression(expr: Expression): Projectibles =
      expr.treeFold(self) {
        case ident: Variable =>
          acc =>
            acc.paths.get(ident) match {
              case Some(pathExpr) => TraverseChildren(acc.withRewrittenVariable(Ref(ident) -> pathExpr))
              case None => TraverseChildren(acc)
            }
      }
  }

  object Projectibles {
    val empty = Projectibles()
  }

  def apply(input: AnyRef): AnyRef = {
    val Projectibles(paths, protectedVariables, variableRewrites) = collectProjectibles(input)
    val applicator = Rewriter.lift {

      case ident: Variable if !protectedVariables(Ref(ident)) =>
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
        TraverseChildren(acc.withProtectedVariable(Ref(aliased.variable)))

    case ident: Variable =>
      acc =>
        acc.paths.get(ident) match {
          case Some(pathExpr) => TraverseChildren(acc.withRewrittenVariable(Ref(ident) -> pathExpr))
          case None => TraverseChildren(acc)
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

    case projection: ProjectionClause =>
      acc =>
        val projectedAcc = projection.returnItems.items.map(_.expression).foldLeft(acc) {
          (acc, expr) => acc.withVariableRewritesForExpression(expr)
        }
        TraverseChildrenNewAccForSiblings(projectedAcc, _.withoutNamedPaths)

    case NamedPatternPart(_, _: ShortestPaths) =>
      acc => TraverseChildren(acc)

    case part @ NamedPatternPart(variable, patternPart) =>
      acc =>
        val pathExpr = expressions.PathExpression(patternPartPathExpression(patternPart))(part.position)
        TraverseChildren(acc.withNamedPath(variable -> pathExpr).withProtectedVariable(Ref(variable)))
  }

  def patternPartPathExpression(patternPart: AnonymousPatternPart): PathStep = patternPart match {
    case EveryPath(element) => patternPartPathExpression(element)
    case x                  => throw new IllegalStateException(s"Unknown pattern part: $x")
  }

  def patternPartPathExpression(element: PatternElement): PathStep = flip(element, NilPathStep)

  @tailrec
  private def flip(element: PatternElement, step: PathStep): PathStep  = {
    element match {
      case NodePattern(node, _, _, _) =>
        NodePathStep(node.get.copyId, step)

      case RelationshipChain(relChain, RelationshipPattern(rel, _, length, _, direction, _, _), to) => length match {
        case None =>
          flip(relChain, SingleRelationshipPathStep(rel.get.copyId, direction, to.variable.map(_.copyId), step))

        case Some(_) =>
          flip(relChain, MultiRelationshipPathStep(rel.get.copyId, direction, to.variable.map(_.copyId), step))
      }
    }
  }
}
