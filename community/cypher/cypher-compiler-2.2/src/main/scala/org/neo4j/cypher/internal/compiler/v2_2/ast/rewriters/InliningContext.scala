/**
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

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.bottomUp.BottomUpRewriter

case class InliningContext(projections: Map[Identifier, Expression] = Map.empty, seenIdentifiers: Set[Identifier] = Set.empty) {

  def enterQueryPart(newProjections: Map[Identifier, Expression]): InliningContext = {
    val inlineExpressions = TypedRewriter[Expression](identifierRewriter)
    val containsAggregation = newProjections.values.exists(containsAggregate)
    val shadowing = newProjections.filterKeys(seenIdentifiers.contains).filter {
      case (_, _: PathExpression) => false
      case (key, value) => key != value
    }

   assert(shadowing.isEmpty, "Should have deduped by this point: " + shadowing)

    val resultProjections = if (containsAggregation) {
      projections
    } else {
      projections ++ newProjections.mapValues(inlineExpressions)
    }
    copy(projections = resultProjections, seenIdentifiers = seenIdentifiers ++ newProjections.keys)
  }

  def spoilIdentifier(identifier: Identifier): InliningContext =
    copy(projections = projections - identifier)

  def identifierRewriter: BottomUpRewriter = bottomUp(Rewriter.lift {
    case identifier: Identifier =>
      projections.getOrElse(identifier, identifier)
  })

  def patternRewriter: BottomUpRewriter = bottomUp(Rewriter.lift {
    case node @ NodePattern(Some(ident), _, _, _) =>
      alias(ident) match {
        case alias @ Some(_) => node.copy(identifier = alias)(node.position)
        case _               => node
      }
    case rel @ RelationshipPattern(Some(ident), _, _, _, _, _) =>
      alias(ident) match {
        case alias @ Some(_) => rel.copy(identifier = alias)(rel.position)
        case _               => rel
      }
  })

  def alias(identifier: Identifier): Option[Identifier] = projections.get(identifier) match {
    case Some(other: Identifier) => Some(other)
    case _                       => None
  }
}

