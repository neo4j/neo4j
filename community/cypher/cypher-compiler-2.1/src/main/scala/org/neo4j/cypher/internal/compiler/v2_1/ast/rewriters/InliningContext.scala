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

import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.planner.CantHandleQueryException

case class InliningContext(projections: Map[Identifier, Expression] = Map.empty, seenIdentifiers: Set[Identifier] = Set.empty) {

  def enterQueryPart(newProjections: Map[Identifier, Expression]): InliningContext = {
    val inlineExpressions = TypedRewriter[Expression](identifierRewriter)
    val resultProjections = newProjections.foldLeft(projections) {
      case (m, (k, v)) if seen(k) => m - k
      case (m,( k, v))            => m + (k -> inlineExpressions(v))
    }
    copy(projections = resultProjections, seenIdentifiers = seenIdentifiers ++ newProjections.keySet)
  }

  def spoilIdentifier(identifier: Identifier): InliningContext =
    copy(projections = projections - identifier, seenIdentifiers = seenIdentifiers + identifier)

  def identifierRewriter = bottomUp(Rewriter.lift {
    case expr: ScopeIntroducingExpression if seen(expr.identifier) =>
      throw new CantHandleQueryException

    case identifier: Identifier =>
      projections.getOrElse(identifier, identifier)
  })

  private def seen(identifier: Identifier) = seenIdentifiers.contains(identifier)
}

