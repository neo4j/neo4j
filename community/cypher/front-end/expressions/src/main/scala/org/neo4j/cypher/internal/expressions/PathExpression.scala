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
package org.neo4j.cypher.internal.expressions

import org.neo4j.cypher.internal.util.InputPosition

sealed trait PathStep extends Expression {
  def dependencies: Set[LogicalVariable]

  override def isConstantForQuery: Boolean = false
}

final case class NodePathStep(node: LogicalVariable, next: PathStep)(val position: InputPosition) extends PathStep {
  override val dependencies: Set[LogicalVariable] = next.dependencies + node
}

final case class SingleRelationshipPathStep(
  rel: LogicalVariable,
  direction: SemanticDirection,
  toNode: Option[LogicalVariable],
  next: PathStep
)(val position: InputPosition) extends PathStep {
  override val dependencies: Set[LogicalVariable] = next.dependencies ++ toNode + rel
}

final case class MultiRelationshipPathStep(
  rel: LogicalVariable,
  direction: SemanticDirection,
  toNode: Option[LogicalVariable],
  next: PathStep
)(val position: InputPosition) extends PathStep {
  override val dependencies: Set[LogicalVariable] = next.dependencies ++ toNode + rel
}

final case class RepeatPathStep(variables: Seq[NodeRelPair], toNode: LogicalVariable, next: PathStep)(
  val position: InputPosition
) extends PathStep {
  override def dependencies: Set[LogicalVariable] = next.dependencies ++ variables.flatMap(_.variables) ++ Set(toNode)
}

object RepeatPathStep {

  def asRepeatPathStep(
    variables: Seq[LogicalVariable],
    toNode: LogicalVariable,
    next: PathStep
  )(position: InputPosition): RepeatPathStep = {
    val nodeRelPairs = variables.grouped(2).toSeq.flatMap {
      case Seq(node: LogicalVariable, rel: LogicalVariable) => Some(NodeRelPair(node, rel))
      case _                                                => None
    }

    RepeatPathStep(nodeRelPairs, toNode, next)(position)
  }
}

case class NilPathStep()(val position: InputPosition) extends PathStep {
  override def dependencies = Set.empty[LogicalVariable]
}

case class PathExpression(step: PathStep)(val position: InputPosition) extends Expression {
  override def isConstantForQuery: Boolean = false
}

case class NodeRelPair(node: LogicalVariable, relationship: LogicalVariable) {
  def variables: Seq[LogicalVariable] = Seq(node, relationship)
}
