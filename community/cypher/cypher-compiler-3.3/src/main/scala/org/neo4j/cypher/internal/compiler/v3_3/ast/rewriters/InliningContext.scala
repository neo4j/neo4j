/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_3.ast.rewriters

import org.neo4j.cypher.internal.compiler.v3_3.ast.rewriters.InliningContext._
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters.copyVariables
import org.neo4j.cypher.internal.frontend.v3_4.{Rewriter, TypedRewriter, bottomUp}

case class InliningContext(projections: Map[Variable, Expression] = Map.empty,
                           seenVariables: Set[Variable] = Set.empty,
                           usageCount: Map[Variable, Int] = Map.empty) {

  def trackUsageOfVariable(id: Variable) =
    copy(usageCount = usageCount + (id -> (usageCount.withDefaultValue(0)(id) + 1)))

  def enterQueryPart(newProjections: Map[Variable, Expression]): InliningContext = {
    val inlineExpressions = TypedRewriter[Expression](variableRewriter)
    val containsAggregation = newProjections.values.exists(containsAggregate)
    val shadowing = newProjections.filterKeys(seenVariables.contains).filter {
      case (_, _: PathExpression) => false
      case (key, value) => key != value
    }

   assert(shadowing.isEmpty, "Should have deduped by this point: " + shadowing)

    val resultProjections = if (containsAggregation) {
      projections
    } else {
      projections ++ newProjections.mapValues(inlineExpressions)
    }
    copy(projections = resultProjections, seenVariables = seenVariables ++ newProjections.keys)
  }

  def spoilVariable(variable: Variable): InliningContext =
    copy(projections = projections - variable)

  def variableRewriter: Rewriter = bottomUp(Rewriter.lift {
    case variable: Variable if okToRewrite(variable) =>
      projections.get(variable).map(_.endoRewrite(copyVariables)).getOrElse(variable.copyId)
  })

  def okToRewrite(i: Variable) =
    projections.contains(i) &&
    usageCount.withDefaultValue(0)(i) < INLINING_THRESHOLD

  def patternRewriter: Rewriter = bottomUp(Rewriter.lift {
    case node @ NodePattern(Some(ident), _, _) if okToRewrite(ident) =>
      alias(ident) match {
        case alias @ Some(_) => node.copy(variable = alias)(node.position)
        case _               => node
      }
    case rel @ RelationshipPattern(Some(ident), _, _, _, _, _) if okToRewrite(ident) =>
      alias(ident) match {
        case alias @ Some(_) => rel.copy(variable = alias)(rel.position)
        case _               => rel
      }
  })

  def isAliasedVarible(variable: Variable) = alias(variable).nonEmpty

  def alias(variable: Variable): Option[Variable] = projections.get(variable) match {
    case Some(other: Variable) => Some(other.copyId)
    case _                       => None
  }
}

object InliningContext {
  private val INLINING_THRESHOLD = 3
}
