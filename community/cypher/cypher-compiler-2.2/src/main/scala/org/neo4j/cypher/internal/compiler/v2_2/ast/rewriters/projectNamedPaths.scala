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

import org.neo4j.cypher.internal.compiler.v2_2.planner.CantHandleQueryException
import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import Foldable._

object projectNamedPaths extends Rewriter {

  private def getRewriter(paths: Map[Identifier, PathExpression],
                          blacklist: IdentityMap[Identifier, Boolean]) = Rewriter.lift {
    case namedPart @ NamedPatternPart(identifier, part) if paths.contains(identifier) =>
      part
    case identifier: Identifier if !blacklist.contains(identifier) =>
      paths.getOrElse(identifier, identifier)
  }

  private def collectNamedPaths(input: AnyRef): Map[Identifier, PathExpression] = {
    input.treeFold(Map.empty[Identifier, PathExpression]) {
      case namedPart @ NamedPatternPart(_, part: ShortestPaths) =>
        (acc, children) => children(acc)
      case part @ NamedPatternPart(identifier, patternPart) =>
        (acc, children) => children(acc + (identifier ->
          PathExpression(patternPartPathExpression(patternPart))(part.position)))
    }
  }

  private def collectUninlinableIdentifiers(input: AnyRef): IdentityMap[Identifier, Boolean] = {
    IdentityMap(input.treeFold(Seq.empty[Identifier]) {
      case item @ AliasedReturnItem(_, alias) =>
        (acc, children) => children(acc :+ alias)
      case namedPart @ NamedPatternPart(identifier, part) =>
        (acc, children) => children(acc :+ identifier)
    }.map(_ -> true): _*)
  }

  def patternPartPathExpression(patternPart: AnonymousPatternPart): PathStep = patternPart match {
    case EveryPath(element) => flip(element, NilPathStep)
    case _                  => throw new CantHandleQueryException
  }

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

  def apply(input: AnyRef): Option[AnyRef] =
    bottomUp(getRewriter(
      collectNamedPaths(input),
      collectUninlinableIdentifiers(input)
    )).apply(input)
}
