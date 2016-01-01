/**
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
package org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters.inliningContextCreator.InlineablePatternPart
import org.neo4j.cypher.internal.compiler.v2_1.planner.CantHandleQueryException

object inliningContextCreator extends (ast.Statement => InliningContext) {

  object InlineablePatternPart {
    def unapply(v: Any): Option[AnonymousPatternPart] = v match {
      case p: ShortestPaths        => None
      case p: AnonymousPatternPart => Some(p)
      case _                       => None
    }
  }

  def apply(input: ast.Statement): InliningContext = {
    input.treeFold(InliningContext()) {
      case (With(false, ListedReturnItems(items), _, _, _, _)) =>
        (context, children) => children(context.enterQueryPart(aliasedReturnItems(items)))

      case Match(_, Pattern(parts), _, _) =>
        (context, children) => children(context.enterQueryPart(
          PatternPartToPathExpression.namedPatternPartPathExpressions(parts))
        )

      case NodePattern(Some(identifier), _, _, _) =>
        (context, children) =>
          if (context.alias(identifier).isEmpty) children(context.spoilIdentifier(identifier)) else children(context)

      case RelationshipPattern(Some(identifier), _, _, _, _, _) =>
        (context, children) =>
          if (context.alias(identifier).isEmpty) children(context.spoilIdentifier(identifier)) else children(context)
    }
  }

  object inliningContextCreator extends (ast.Statement => InliningContext) {
    def apply(input: ast.Statement): InliningContext = {
      input.treeFold(InliningContext()) {
        case (With(false, ListedReturnItems(items), _, _, _, _)) =>
          (context, children) => children(context.enterQueryPart(aliasedReturnItems(items)))

        case Match(_, Pattern(parts), _, _) =>
          (context, children) => children(context.enterQueryPart(PatternPartToPathExpression.namedPatternPartPathExpressions(parts)))

        case NodePattern(Some(identifier), _, _, _) =>
          (context, children) =>
            if (context.alias(identifier).isEmpty) children(context.spoilIdentifier(identifier)) else children(context)

        case RelationshipPattern(Some(identifier), _, _, _, _, _) =>
          (context, children) =>
            if (context.alias(identifier).isEmpty) children(context.spoilIdentifier(identifier)) else children(context)
      }
    }
  }

  private def aliasedReturnItems(items: Seq[ReturnItem]): Map[Identifier, Expression] =
    items.collect { case AliasedReturnItem(expr, ident) => ident -> expr }.toMap
}

object PatternPartToPathExpression {
  def namedPatternPartPathExpressions(parts: Seq[PatternPart]): Map[Identifier, PathExpression] =
    parts.collect {
      case part @ NamedPatternPart(identifier, InlineablePatternPart(patternPart)) =>
        identifier -> PathExpression(patternPartPathExpression(patternPart))(part.position)
    }.toMap

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
}
