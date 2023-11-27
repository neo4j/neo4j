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
package org.neo4j.cypher.internal.logical.plans.set

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.ir
import org.neo4j.cypher.internal.ir.CreateCommand
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.CreateRelationship

// Note, this is a copy of org.neo4j.cypher.internal.ir.MutatingPattern
// We can probably unify them.
sealed trait MutatingPattern extends Product {
  def dependencies: Set[LogicalVariable]
}

// Note, this is a copy of org.neo4j.cypher.internal.ir.MutatingPattern
// We can probably unify them.
sealed trait SimpleMutatingPattern extends MutatingPattern

object SimpleMutatingPattern {

  def from(pattern: ir.SimpleMutatingPattern): SimpleMutatingPattern = pattern match {
    case s: ir.SetMutatingPattern    => SetMutatingPattern.from(s)
    case d: ir.DeleteMutatingPattern => DeleteMutatingPattern.from(d)
    case c: ir.CreatePattern         => CreatePattern.from(c)
  }
}

// Note, this is a copy of org.neo4j.cypher.internal.ir.SetMutatingPattern
// We can probably unify them.
sealed trait SetMutatingPattern extends SimpleMutatingPattern

object SetMutatingPattern {

  def from(pattern: org.neo4j.cypher.internal.ir.SetMutatingPattern): SetMutatingPattern = pattern match {
    case ir.SetPropertyPattern(entityExpression, propertyKeyName, expression) =>
      SetPropertyPattern(entityExpression, propertyKeyName, expression)
    case ir.SetPropertiesPattern(entityExpression, items) =>
      SetPropertiesPattern(entityExpression, items)
    case ir.SetRelationshipPropertyPattern(idName, propertyKey, expression) =>
      SetRelationshipPropertyPattern(idName, propertyKey, expression)
    case ir.SetRelationshipPropertiesPattern(idName, items) =>
      SetRelationshipPropertiesPattern(idName, items)
    case ir.SetNodePropertiesFromMapPattern(idName, expression, removeOtherProps) =>
      SetNodePropertiesFromMapPattern(idName, expression, removeOtherProps)
    case ir.SetRelationshipPropertiesFromMapPattern(idName, expression, removeOtherProps) =>
      SetRelationshipPropertiesFromMapPattern(idName, expression, removeOtherProps)
    case ir.SetPropertiesFromMapPattern(entityExpression, expression, removeOtherProps) =>
      SetPropertiesFromMapPattern(entityExpression, expression, removeOtherProps)
    case ir.SetNodePropertyPattern(idName, propertyKey, expression) =>
      SetNodePropertyPattern(idName, propertyKey, expression)
    case ir.SetNodePropertiesPattern(idName, items) =>
      SetNodePropertiesPattern(idName, items)
    case ir.SetLabelPattern(idName, labels) =>
      SetLabelPattern(idName, labels)
    case ir.RemoveLabelPattern(idName, labels) =>
      RemoveLabelPattern(idName, labels)
  }
}

// Note, this is a copy of org.neo4j.cypher.internal.ir.DeleteMutatingPattern
// We can probably unify them.
sealed trait DeleteMutatingPattern extends SimpleMutatingPattern

object DeleteMutatingPattern {

  def from(pattern: ir.DeleteMutatingPattern): DeleteMutatingPattern = pattern match {
    case ir.DeleteExpression(expression, detachDelete) => DeleteExpression(expression, detachDelete)
  }
}

case class SetPropertyPattern(
  entityExpression: Expression,
  propertyKeyName: PropertyKeyName,
  expression: Expression
) extends SetMutatingPattern {
  override def dependencies: Set[LogicalVariable] = (entityExpression.dependencies ++ expression.dependencies)
}

case class SetPropertiesPattern(
  entityExpression: Expression,
  items: Seq[(PropertyKeyName, Expression)]
) extends SetMutatingPattern {

  override def dependencies: Set[LogicalVariable] = {
    entityExpression.dependencies ++ items.view.flatMap { case (_, e) => e.dependencies }
  }
}

case class SetRelationshipPropertyPattern(
  idName: LogicalVariable,
  propertyKey: PropertyKeyName,
  expression: Expression
) extends SetMutatingPattern {
  override def dependencies: Set[LogicalVariable] = expression.dependencies + idName
}

case class SetRelationshipPropertiesPattern(
  idName: LogicalVariable,
  items: Seq[(PropertyKeyName, Expression)]
) extends SetMutatingPattern {

  override def dependencies: Set[LogicalVariable] = {
    items.view.flatMap { case (_, e) => e.dependencies }.toSet + idName
  }
}

case class SetNodePropertiesFromMapPattern(
  idName: LogicalVariable,
  expression: Expression,
  removeOtherProps: Boolean
) extends SetMutatingPattern {
  override def dependencies: Set[LogicalVariable] = expression.dependencies + idName
}

case class SetRelationshipPropertiesFromMapPattern(
  idName: LogicalVariable,
  expression: Expression,
  removeOtherProps: Boolean
) extends SetMutatingPattern {
  override def dependencies: Set[LogicalVariable] = expression.dependencies + idName
}

case class SetPropertiesFromMapPattern(
  entityExpression: Expression,
  expression: Expression,
  removeOtherProps: Boolean
) extends SetMutatingPattern {
  override def dependencies: Set[LogicalVariable] = entityExpression.dependencies ++ expression.dependencies
}

case class SetNodePropertyPattern(
  idName: LogicalVariable,
  propertyKey: PropertyKeyName,
  expression: Expression
) extends SetMutatingPattern {
  override def dependencies: Set[LogicalVariable] = expression.dependencies + idName
}

case class SetNodePropertiesPattern(
  idName: LogicalVariable,
  items: Seq[(PropertyKeyName, Expression)]
) extends SetMutatingPattern {

  override def dependencies: Set[LogicalVariable] = {
    items.view.flatMap { case (_, e) => e.dependencies }.toSet + idName
  }
}

case class SetLabelPattern(
  idName: LogicalVariable,
  labels: Seq[LabelName]
) extends SetMutatingPattern {
  override def dependencies: Set[LogicalVariable] = Set(idName)
}

case class RemoveLabelPattern(
  idName: LogicalVariable,
  labels: Seq[LabelName]
) extends SetMutatingPattern {
  override def dependencies: Set[LogicalVariable] = Set(idName)
}

case class CreatePattern(commands: Seq[CreateCommand]) extends SimpleMutatingPattern {

  def nodes: Iterable[CreateNode] = commands.view.collect { case c: CreateNode => c }
  def relationships: Iterable[CreateRelationship] = commands.view.collect { case c: CreateRelationship => c }

  override def dependencies: Set[LogicalVariable] = {
    commands.view.flatMap(_.dependencies).toSet
  }
}

object CreatePattern {
  def from(c: ir.CreatePattern): CreatePattern = CreatePattern(c.commands)
}

case class DeleteExpression(
  expression: Expression,
  detachDelete: Boolean
) extends DeleteMutatingPattern {
  override def dependencies: Set[LogicalVariable] = expression.dependencies
}

sealed trait MergePattern

case class MergeNodePattern(
  createNode: CreateNode,
  onCreate: Seq[SetMutatingPattern],
  onMatch: Seq[SetMutatingPattern]
) extends MutatingPattern with MergePattern {

  override def dependencies: Set[LogicalVariable] = {
    createNode.dependencies ++ onCreate.flatMap(_.dependencies) ++ onMatch.flatMap(_.dependencies)
  }
}

case class MergeRelationshipPattern(
  createNodes: Seq[CreateNode],
  createRelationships: Seq[CreateRelationship],
  onCreate: Seq[SetMutatingPattern],
  onMatch: Seq[SetMutatingPattern]
) extends MutatingPattern with MergePattern {

  override def dependencies: Set[LogicalVariable] = {
    createNodes.flatMap(_.dependencies).toSet ++
      createRelationships.flatMap(_.dependencies) ++
      onCreate.flatMap(_.dependencies) ++
      onMatch.flatMap(_.dependencies)
  }
}

case class ForeachPattern(
  variable: LogicalVariable,
  expression: Expression,
  innerDependencies: Set[LogicalVariable]
) extends MutatingPattern {

  override def dependencies: Set[LogicalVariable] = {
    expression.dependencies ++ innerDependencies
  }
}
