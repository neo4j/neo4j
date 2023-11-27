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
package org.neo4j.cypher.internal.logical.plans.create

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir
import org.neo4j.cypher.internal.ir.CreateCommand

// Note, this is a copy of org.neo4j.cypher.internal.ir.CreateCommand
// We can probably unify them.
sealed trait CreateEntity {
  def variable: LogicalVariable
  def dependencies: Set[LogicalVariable]
}

object CreateEntity {

  def from(create: CreateCommand): CreateEntity = create match {
    case n: ir.CreateNode         => CreateNode.from(n)
    case r: ir.CreateRelationship => CreateRelationship.from(r)
  }
}

// Note, this is a copy of org.neo4j.cypher.internal.ir.CreateNode
// We can probably unify them.
case class CreateNode(
  variable: LogicalVariable,
  labels: Set[LabelName],
  properties: Option[Expression]
) extends CreateEntity {
  override def dependencies: Set[LogicalVariable] = properties.map(_.dependencies).getOrElse(Set.empty)
}

object CreateNode {
  def from(n: ir.CreateNode): CreateNode = CreateNode(n.variable, n.labels, n.properties)
}

// Note, this is a copy of org.neo4j.cypher.internal.ir.CreateRelationship
// We can probably unify them.
case class CreateRelationship(
  variable: LogicalVariable,
  leftNode: LogicalVariable,
  relType: RelTypeName,
  rightNode: LogicalVariable,
  direction: SemanticDirection,
  properties: Option[Expression]
) extends CreateEntity {

  def startNode: LogicalVariable =
    if (direction == SemanticDirection.OUTGOING || direction == SemanticDirection.BOTH) leftNode else rightNode

  def endNode: LogicalVariable =
    if (direction == SemanticDirection.OUTGOING || direction == SemanticDirection.BOTH) rightNode else leftNode
  override def dependencies: Set[LogicalVariable] = properties.map(_.dependencies).getOrElse(Set.empty)
}

object CreateRelationship {

  def from(r: ir.CreateRelationship): CreateRelationship =
    CreateRelationship(
      r.variable,
      r.leftNode,
      r.relType,
      r.rightNode,
      r.direction,
      r.properties
    )
}
