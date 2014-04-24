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

import org.neo4j.cypher.internal.compiler.v2_1.{topDown, bottomUp, Rewriter}
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.ast.Identifier
import org.neo4j.cypher.internal.compiler.v2_1.planner.CantHandleQueryException

object inlineNamedPaths {

  type InlinedPaths = Map[Identifier, PathExpression]

  object InlinedPaths {
    def empty = Map.empty[Identifier, PathExpression]
  }

  def apply(statement: Statement): Statement = {
    val inlinedPaths = statement.treeFold(InlinedPaths.empty) {
      case Match(_, Pattern(parts), _, _) => (m, children) =>
        parts.foldLeft(m) {
          case (acc, part @ NamedPatternPart(identifier, patternPart)) =>
            acc + (identifier -> PathExpression(patternPartPathExpression(patternPart))(part.position))
          case (acc, _) =>
            acc
        }
    }

    val rewriter = Rewriter.lift {
      case NamedPatternPart(identifier, part) if inlinedPaths.contains(identifier) => part
      case identifier: Identifier if inlinedPaths.contains(identifier) => inlinedPaths(identifier)
    }

    statement.rewrite(topDown(rewriter)).asInstanceOf[Statement]
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
