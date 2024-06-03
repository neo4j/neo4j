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
import org.neo4j.cypher.internal.expressions.PropertyKeyName

import scala.util.hashing.MurmurHash3

sealed trait MutatingPattern extends Product {
  def coveredIds: Set[LogicalVariable]
  def dependencies: Set[LogicalVariable]

  // We spend a lot of time hashing these objects,
  // since they are all immutable we can memoize the hashcode
  override val hashCode: Int = MurmurHash3.productHash(this)

}

sealed trait NoSymbols {
  self: MutatingPattern =>
  override def coveredIds = Set.empty[LogicalVariable]
}

sealed trait SimpleMutatingPattern extends MutatingPattern

sealed trait SetMutatingPattern extends SimpleMutatingPattern with NoSymbols

sealed trait DeleteMutatingPattern extends SimpleMutatingPattern with NoSymbols

case class SetPropertyPattern(entityExpression: Expression, propertyKeyName: PropertyKeyName, expression: Expression)
    extends SetMutatingPattern
    with HasMappableExpressions[SetPropertyPattern] {
  override def dependencies: Set[LogicalVariable] = entityExpression.dependencies ++ expression.dependencies

  override def mapExpressions(f: Expression => Expression): SetPropertyPattern =
    copy(
      entityExpression = f(entityExpression),
      expression = f(expression)
    )
}

case class SetPropertiesPattern(entityExpression: Expression, items: Seq[(PropertyKeyName, Expression)])
    extends SetMutatingPattern with HasMappableExpressions[SetPropertiesPattern] {

  override def dependencies: Set[LogicalVariable] =
    items.map(_._2).flatMap(_.dependencies).toSet ++ entityExpression.dependencies

  override def mapExpressions(f: Expression => Expression): SetPropertiesPattern =
    copy(
      entityExpression = f(entityExpression),
      items = items.map {
        case (k, e) => (k, f(e))
      }
    )
}

case class SetRelationshipPropertyPattern(
  variable: LogicalVariable,
  propertyKey: PropertyKeyName,
  expression: Expression
) extends SetMutatingPattern
    with HasMappableExpressions[SetRelationshipPropertyPattern] {
  override def dependencies: Set[LogicalVariable] = expression.dependencies + variable

  override def mapExpressions(f: Expression => Expression): SetRelationshipPropertyPattern =
    copy(expression = f(expression))
}

case class SetRelationshipPropertiesPattern(variable: LogicalVariable, items: Seq[(PropertyKeyName, Expression)])
    extends SetMutatingPattern with HasMappableExpressions[SetRelationshipPropertiesPattern] {

  override def dependencies: Set[LogicalVariable] = items.map(_._2).flatMap(_.dependencies).toSet + variable

  override def mapExpressions(f: Expression => Expression): SetRelationshipPropertiesPattern = {
    copy(items = items.map {
      case (k, e) => (k, f(e))
    })
  }
}

case class SetNodePropertiesFromMapPattern(variable: LogicalVariable, expression: Expression, removeOtherProps: Boolean)
    extends SetMutatingPattern
    with HasMappableExpressions[SetNodePropertiesFromMapPattern] {
  override def dependencies: Set[LogicalVariable] = expression.dependencies + variable

  override def mapExpressions(f: Expression => Expression): SetNodePropertiesFromMapPattern =
    copy(expression = f(expression))
}

case class SetRelationshipPropertiesFromMapPattern(
  variable: LogicalVariable,
  expression: Expression,
  removeOtherProps: Boolean
) extends SetMutatingPattern
    with HasMappableExpressions[SetRelationshipPropertiesFromMapPattern] {
  override def dependencies: Set[LogicalVariable] = expression.dependencies + variable

  override def mapExpressions(f: Expression => Expression): SetRelationshipPropertiesFromMapPattern =
    copy(expression = f(expression))
}

case class SetPropertiesFromMapPattern(entityExpression: Expression, expression: Expression, removeOtherProps: Boolean)
    extends SetMutatingPattern
    with HasMappableExpressions[SetPropertiesFromMapPattern] {

  override def dependencies: Set[LogicalVariable] =
    entityExpression.dependencies ++ expression.dependencies

  override def mapExpressions(f: Expression => Expression): SetPropertiesFromMapPattern =
    copy(f(entityExpression), f(expression))
}

case class SetDynamicPropertyPattern(entity: Expression, property: Expression, expression: Expression)
    extends SetMutatingPattern with HasMappableExpressions[SetDynamicPropertyPattern] {

  override def mapExpressions(f: Expression => Expression): SetDynamicPropertyPattern =
    copy(f(entity), f(property), f(expression))

  override def dependencies: Set[LogicalVariable] =
    entity.dependencies ++ property.dependencies ++ expression.dependencies
}

case class SetNodePropertyPattern(variable: LogicalVariable, propertyKey: PropertyKeyName, expression: Expression)
    extends SetMutatingPattern
    with HasMappableExpressions[SetNodePropertyPattern] {
  override def dependencies: Set[LogicalVariable] = expression.dependencies + variable
  override def mapExpressions(f: Expression => Expression): SetNodePropertyPattern = copy(expression = f(expression))
}

case class SetNodePropertiesPattern(variable: LogicalVariable, items: Seq[(PropertyKeyName, Expression)])
    extends SetMutatingPattern with HasMappableExpressions[SetNodePropertiesPattern] {

  override def dependencies: Set[LogicalVariable] = items.map(_._2).flatMap(_.dependencies).toSet + variable

  override def mapExpressions(f: Expression => Expression): SetNodePropertiesPattern = {
    copy(items = items.map {
      case (k, e) => (k, f(e))
    })
  }
}

case class SetLabelPattern(variable: LogicalVariable, labels: Seq[LabelName]) extends SetMutatingPattern {
  override def dependencies: Set[LogicalVariable] = Set(variable)
}

case class RemoveLabelPattern(variable: LogicalVariable, labels: Seq[LabelName]) extends SetMutatingPattern
    with NoSymbols {
  override def dependencies: Set[LogicalVariable] = Set(variable)
}

case class CreatePattern(commands: Seq[CreateCommand]) extends SimpleMutatingPattern
    with HasMappableExpressions[CreatePattern] {

  def nodes: Seq[CreateNode] = commands.collect {
    case c: CreateNode => c
  }

  def relationships: Seq[CreateRelationship] = commands.collect {
    case c: CreateRelationship => c
  }

  override def coveredIds: Set[LogicalVariable] = {
    val builder = Set.newBuilder[LogicalVariable]
    for (command <- commands)
      builder += command.variable
    builder.result()
  }

  override def dependencies: Set[LogicalVariable] = {
    val builder = Set.newBuilder[LogicalVariable]
    for (command <- commands)
      builder ++= command.dependencies
    builder.result()
  }

  override def mapExpressions(f: Expression => Expression): CreatePattern = {
    copy(commands = commands.map(_.mapExpressions(f)))
  }
}

case class DeleteExpression(expression: Expression, detachDelete: Boolean) extends DeleteMutatingPattern with NoSymbols
    with HasMappableExpressions[DeleteExpression] {
  override def dependencies: Set[LogicalVariable] = expression.dependencies

  override def mapExpressions(f: Expression => Expression): DeleteExpression = copy(expression = f(expression))
}

sealed trait MergePattern {
  self: MutatingPattern =>
  def matchGraph: QueryGraph
}

case class MergeNodePattern(
  createNode: CreateNode,
  matchGraph: QueryGraph,
  onCreate: Seq[SetMutatingPattern],
  onMatch: Seq[SetMutatingPattern]
) extends MutatingPattern with MergePattern {
  override def coveredIds: Set[LogicalVariable] = matchGraph.allCoveredIds

  override def dependencies: Set[LogicalVariable] =
    createNode.dependencies ++
      matchGraph.dependencies ++
      onCreate.flatMap(_.dependencies) ++
      onMatch.flatMap(_.dependencies)
}

case class MergeRelationshipPattern(
  createNodes: Seq[CreateNode],
  createRelationships: Seq[CreateRelationship],
  matchGraph: QueryGraph,
  onCreate: Seq[SetMutatingPattern],
  onMatch: Seq[SetMutatingPattern]
) extends MutatingPattern with MergePattern {
  override def coveredIds: Set[LogicalVariable] = matchGraph.allCoveredIds

  override def dependencies: Set[LogicalVariable] =
    createNodes.flatMap(_.dependencies).toSet ++
      createRelationships.flatMap(_.dependencies).toSet ++
      matchGraph.dependencies ++
      onCreate.flatMap(_.dependencies) ++
      onMatch.flatMap(_.dependencies)
}

case class ForeachPattern(variable: LogicalVariable, expression: Expression, innerUpdates: SinglePlannerQuery)
    extends MutatingPattern with NoSymbols {
  override def dependencies: Set[LogicalVariable] = expression.dependencies ++ innerUpdates.dependencies
}
