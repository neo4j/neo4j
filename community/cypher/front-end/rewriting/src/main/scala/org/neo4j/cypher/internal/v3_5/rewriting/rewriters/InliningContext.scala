/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.v3_5.rewriting.rewriters

import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.rewriting.rewriters.InliningContext.INLINING_THRESHOLD
import org.neo4j.cypher.internal.v3_5.util._
import org.neo4j.cypher.internal.v3_5.expressions.{PathExpression, Variable}

case class InliningContext(projections: Map[LogicalVariable, Expression] = Map.empty,
                           seenVariables: Set[LogicalVariable] = Set.empty,
                           usageCount: Map[LogicalVariable, Int] = Map.empty) {

  def trackUsageOfVariable(id: Variable) =
    copy(usageCount = usageCount + (id -> (usageCount.withDefaultValue(0)(id) + 1)))

  def enterQueryPart(newProjections: Map[LogicalVariable, Expression]): InliningContext = {
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

  def spoilVariable(variable: LogicalVariable): InliningContext =
    copy(projections = projections - variable)

  def variableRewriter: Rewriter = bottomUp(Rewriter.lift {
    case variable: Variable if okToRewrite(variable) =>
      projections.get(variable).map(_.endoRewrite(copyVariables)).getOrElse(variable.copyId)
  })

  def okToRewrite(i: LogicalVariable) =
    projections.contains(i) &&
      usageCount.withDefaultValue(0)(i) < INLINING_THRESHOLD

  def patternRewriter: Rewriter = bottomUp(Rewriter.lift {
    case node @ NodePattern(Some(ident), _, _, _) if okToRewrite(ident) =>
      alias(ident) match {
        case alias @ Some(_) => node.copy(variable = alias)(node.position)
        case _               => node
      }
    case rel @ RelationshipPattern(Some(ident), _, _, _, _, _, _) if okToRewrite(ident) =>
      alias(ident) match {
        case alias @ Some(_) => rel.copy(variable = alias)(rel.position)
        case _               => rel
      }
  })

  def isAliasedVarible(variable: LogicalVariable) = alias(variable).nonEmpty

  def alias(variable: LogicalVariable): Option[LogicalVariable] = projections.get(variable) match {
    case Some(other: Variable) => Some(other.copyId)
    case _                       => None
  }
}

object InliningContext {
  private val INLINING_THRESHOLD = 3
}
