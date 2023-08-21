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
package org.neo4j.cypher.internal.ast.prettifier

import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MultiRelationshipPathStep
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.NodeRelPair
import org.neo4j.cypher.internal.expressions.PathStep
import org.neo4j.cypher.internal.expressions.RepeatPathStep
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SingleRelationshipPathStep

trait PathStepStringifier {
  def apply(pathStep: PathStep): String
}

object PathStepStringifier {
  def apply(expr: ExpressionStringifier): PathStepStringifier = new DefaultPathStepStringifier(expr)
}

private class DefaultPathStepStringifier(expr: ExpressionStringifier) extends PathStepStringifier {

  def apply(pathStep: PathStep): String = pathStep match {
    case SingleRelationshipPathStep(rel, direction, toNode, next) =>
      relationshipPathStep(rel, direction, toNode, next, isMultiRel = false)

    case NodePathStep(node, next) => s"(${expr(node)})${apply(next)}"

    case MultiRelationshipPathStep(rel, direction, toNode, next) =>
      relationshipPathStep(rel, direction, toNode, next, isMultiRel = true)

    case RepeatPathStep(variables, toNode, next) =>
      repeatPathStep(variables, toNode, next)

    case NilPathStep() => ""
  }

  private def relationshipPathStep(
    rel: LogicalVariable,
    direction: SemanticDirection,
    toNode: Option[LogicalVariable],
    next: PathStep,
    isMultiRel: Boolean
  ) = {
    val lArrow = if (direction == SemanticDirection.INCOMING) "<" else ""
    val rArrow = if (direction == SemanticDirection.OUTGOING) ">" else ""
    val stringifiedToNode = toNode.map(expr(_)).getOrElse("")
    val stringifiedRel = expr(rel)
    val multiRel = if (isMultiRel) "*" else ""

    s"$lArrow-[$stringifiedRel$multiRel]-$rArrow($stringifiedToNode)" + this(next)
  }

  private def repeatPathStep(variables: Seq[NodeRelPair], toNode: LogicalVariable, next: PathStep): String = {
    val variableString = variables.flatMap(_.variables).map(_.name).zipWithIndex.map {
      case (name, index) if (index % 2 == 0) =>
        s"($name)"
      case (name, _) =>
        s"-[$name]-"
    }.mkString("")

    s" ($variableString())* (${toNode.name})${apply(next)}"
  }
}
