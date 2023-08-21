/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.containsAggregate
import org.neo4j.cypher.internal.rewriting.rewriters.InliningContext.INLINING_THRESHOLD
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.TypedRewriter
import org.neo4j.cypher.internal.util.bottomUp

case class InliningContext(
  projections: Map[LogicalVariable, Expression] = Map.empty,
  seenVariables: Set[LogicalVariable] = Set.empty,
  usageCount: Map[LogicalVariable, Int] = Map.empty
) {

  def trackUsageOfVariable(id: Variable): InliningContext =
    copy(usageCount = usageCount + (id -> (usageCount.withDefaultValue(0)(id) + 1)))

  def enterQueryPart(newProjections: Map[LogicalVariable, Expression]): InliningContext = {
    val inlineExpressions = TypedRewriter[Expression](variableRewriter)
    val containsAggregation = newProjections.values.exists(containsAggregate)
    val shadowing = newProjections.view.filterKeys(seenVariables.contains).filter {
      case (_, _: PathExpression) => false
      case (key, value)           => key != value
    }

    assert(shadowing.isEmpty, "Should have deduped by this point: " + shadowing)

    val resultProjections =
      if (containsAggregation) {
        projections
      } else {
        projections ++ newProjections.view.mapValues(inlineExpressions)
      }
    copy(projections = resultProjections, seenVariables = seenVariables ++ newProjections.keys)
  }

  def spoilVariable(variable: LogicalVariable): InliningContext =
    copy(projections = projections - variable)

  def variableRewriter: Rewriter = bottomUp(Rewriter.lift {
    case variable: Variable if okToRewrite(variable) =>
      projections.get(variable).map(_.endoRewrite(copyVariables)).getOrElse(variable.copyId)
  })

  // noinspection DfaConstantConditions
  // Intellij seems to think this method always returns false, added the noinspection to not warn for it
  def okToRewrite(i: LogicalVariable): Boolean =
    projections.contains(i) &&
      usageCount.withDefaultValue(0)(i) < INLINING_THRESHOLD

  def patternRewriter: Rewriter = bottomUp(Rewriter.lift {
    case node @ NodePattern(Some(ident), _, _, _) if okToRewrite(ident) =>
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

  def isAliasedVariable(variable: LogicalVariable): Boolean = alias(variable).nonEmpty

  def alias(variable: LogicalVariable): Option[LogicalVariable] = projections.get(variable) match {
    case Some(other: Variable) => Some(other.copyId)
    case _                     => None
  }
}

object InliningContext {
  private val INLINING_THRESHOLD = 3
}
