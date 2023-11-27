/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.ir

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasMappableExpressions
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection

sealed trait CreateCommand extends HasMappableExpressions[CreateCommand] {
  def variable: LogicalVariable
  def properties: Option[Expression]
  def dependencies: Set[LogicalVariable]
}

/**
 * Create a new node with the provided labels and properties and assign it to the variable 'idName'.
 */
case class CreateNode(variable: LogicalVariable, labels: Set[LabelName], properties: Option[Expression])
    extends CreateCommand {
  def dependencies: Set[LogicalVariable] = properties.map(_.dependencies).getOrElse(Set.empty)

  override def mapExpressions(f: Expression => Expression): CreateNode = copy(properties = properties.map(f))
}

/**
* Create a new relationship with the provided type and properties and assign it to the variable 'idName'.
*/
case class CreateRelationship(
  variable: LogicalVariable,
  leftNode: LogicalVariable,
  relType: RelTypeName,
  rightNode: LogicalVariable,
  direction: SemanticDirection,
  properties: Option[Expression]
) extends CreateCommand {

  val (startNode, endNode) =
    if (direction == SemanticDirection.OUTGOING || direction == SemanticDirection.BOTH) (leftNode, rightNode)
    else (rightNode, leftNode)

  def dependencies: Set[LogicalVariable] =
    properties.map(_.dependencies).getOrElse(Set.empty) + leftNode + rightNode

  override def mapExpressions(f: Expression => Expression): CreateRelationship = copy(properties = properties.map(f))
}
