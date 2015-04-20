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
package org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_3.Foldable._
import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.ast._
import org.neo4j.cypher.internal.compiler.v2_3.planner.CantHandleQueryException

import scala.annotation.tailrec

case object projectNamedPaths extends Rewriter {

  private def projectNamedPathsRewriter(paths: Map[Identifier, PathExpression], blacklist: IdentityMap[Identifier, Boolean]) =
    Rewriter.lift {
      case NamedPatternPart(identifier, part) if paths.contains(identifier) =>
        part
      case identifier: Identifier if !blacklist.contains(identifier) && paths.contains(identifier) =>
        paths(identifier).endoRewrite(copyIdentifiers)
    }

  /**
   * In order to safely project named paths we must also cary along the dependencies of the paths
   */
  private def projectDependenciesRewriter(paths: Map[Identifier, PathExpression]) = replace(replacer => {
    case expr: Expression =>
      replacer.stop(expr)

    case clause@With(_, ri: ReturnItems, _, _, _, _) if !ri.containsAggregate =>
      val pathDependencies = clause.treeFold(Set.empty[Identifier]) {
        case step:PathStep => (acc, children) => children(acc ++ step.dependencies)
        case _ => (acc, children) => children(acc)
      }

      //add dependencies coming from paths, without duplication
      val newDeps = (ri.items.toSet ++ pathDependencies.map(a => AliasedReturnItem(a, a)(a.position))).toSeq
      val newRi = ri.copy(items = newDeps)(ri.position)

      clause.copy(returnItems = newRi)(clause.position).endoRewrite(copyIdentifiers)

    case astNode =>
      replacer.expand(astNode)
  })

  private def collectNamedPaths(input: AnyRef): Map[Identifier, PathExpression] = {
    input.treeFold(Map.empty[Identifier, PathExpression]) {
      case NamedPatternPart(_, part: ShortestPaths) =>
        (acc, children) => children(acc)
      case part @ NamedPatternPart(identifier, patternPart) =>
        (acc, children) => children(acc + (identifier ->
          PathExpression(patternPartPathExpression(patternPart))(part.position)))
    }
  }

  private def collectUninlinableIdentifiers(input: AnyRef): IdentityMap[Identifier, Boolean] = {
    IdentityMap(input.treeFold(Seq.empty[Identifier]) {
      case AliasedReturnItem(_, alias) =>
        (acc, children) => children(acc :+ alias)
      case NamedPatternPart(identifier, _) =>
        (acc, children) => children(acc :+ identifier)
    }.map(_ -> true): _*)
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
    val namedPaths = collectNamedPaths(input)
    val blacklist = collectUninlinableIdentifiers(input)

    bottomUp(inSequence(
      projectNamedPathsRewriter(namedPaths, blacklist),
      projectDependenciesRewriter(namedPaths))
    )(input)
  }
}
