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
package org.neo4j.cypher.internal.compiler.v3_1.planner

import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.frontend.v3_1.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_1.ast.{Expression, LabelName, PropertyKeyName, RelTypeName}

sealed trait MutatingPattern {
  def coveredIds: Set[IdName]
}

sealed trait NoSymbols {
  self : MutatingPattern =>
  override def coveredIds = Set.empty[IdName]
}

case class CreateNodePattern(nodeName: IdName, labels: Seq[LabelName], properties: Option[Expression]) extends MutatingPattern {
  override def coveredIds = Set(nodeName)
}

case class CreateRelationshipPattern(relName: IdName, leftNode: IdName, relType: RelTypeName, rightNode: IdName,
                                     properties: Option[Expression], direction: SemanticDirection) extends  MutatingPattern {
  def startNode = inOrder._1

  def endNode = inOrder._2

  //WHEN merging we can have an undirected CREATE, it is interpreted left-to-right
  def inOrder =  if (direction == SemanticDirection.OUTGOING || direction == SemanticDirection.BOTH) (leftNode, rightNode) else (rightNode, leftNode)

  override def coveredIds = Set(relName)
}

sealed trait SetMutatingPattern extends MutatingPattern with NoSymbols

case class SetLabelPattern(idName: IdName, labels: Seq[LabelName]) extends SetMutatingPattern

case class SetNodePropertyPattern(idName: IdName, propertyKey: PropertyKeyName, expression: Expression) extends SetMutatingPattern

case class SetNodePropertiesFromMapPattern(idName: IdName, expression: Expression, removeOtherProps: Boolean) extends SetMutatingPattern

case class SetRelationshipPropertyPattern(idName: IdName, propertyKey: PropertyKeyName, expression: Expression) extends SetMutatingPattern

case class SetRelationshipPropertiesFromMapPattern(idName: IdName, expression: Expression, removeOtherProps: Boolean) extends SetMutatingPattern

case class SetPropertyPattern(entityExpression: Expression, propertyKeyName: PropertyKeyName, expression: Expression) extends SetMutatingPattern

case class RemoveLabelPattern(idName: IdName, labels: Seq[LabelName]) extends MutatingPattern with NoSymbols

case class DeleteExpression(expression: Expression, forced: Boolean) extends MutatingPattern with NoSymbols

trait MergePattern {
  self : MutatingPattern =>
  def matchGraph: QueryGraph
}

case class MergeNodePattern(createNodePattern: CreateNodePattern, matchGraph: QueryGraph, onCreate: Seq[SetMutatingPattern],
                            onMatch: Seq[SetMutatingPattern]) extends MutatingPattern with MergePattern {
  override def coveredIds = matchGraph.allCoveredIds
}

case class MergeRelationshipPattern(createNodePatterns: Seq[CreateNodePattern], createRelPatterns: Seq[CreateRelationshipPattern],
                                    matchGraph: QueryGraph, onCreate: Seq[SetMutatingPattern], onMatch: Seq[SetMutatingPattern]) extends MutatingPattern with MergePattern {
  override def coveredIds = matchGraph.allCoveredIds
}

case class ForeachPattern(variable: IdName, expression: Expression, innerUpdates: PlannerQuery) extends MutatingPattern with NoSymbols

