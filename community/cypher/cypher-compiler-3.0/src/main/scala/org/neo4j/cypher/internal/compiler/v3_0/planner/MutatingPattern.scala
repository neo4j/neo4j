package org.neo4j.cypher.internal.compiler.v3_0.planner

import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.IdName
import org.neo4j.cypher.internal.frontend.v3_0.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_0.ast.{Expression, LabelName, PropertyKeyName, RelTypeName}

sealed trait MutatingPattern


case class CreateNodePattern(nodeName: IdName, labels: Seq[LabelName], properties: Option[Expression]) extends MutatingPattern

case class CreateRelationshipPattern(relName: IdName, leftNode: IdName, relType: RelTypeName, rightNode: IdName,
                                     properties: Option[Expression], direction: SemanticDirection) extends  MutatingPattern {
  assert(direction != SemanticDirection.BOTH)

  def startNode = inOrder._1

  def endNode = inOrder._2

  def inOrder =  if (direction == SemanticDirection.OUTGOING) (leftNode, rightNode) else (rightNode, leftNode)
}

case class SetLabelPattern(idName: IdName, labels: Seq[LabelName]) extends MutatingPattern

case class SetNodePropertyPattern(idName: IdName, propertyKey: PropertyKeyName, expression: Expression) extends MutatingPattern

case class SetIncludingNodePropertiesFromMapPattern(idName: IdName, expression: Expression) extends MutatingPattern

case class SetRelationshipPropertyPattern(idName: IdName, propertyKey: PropertyKeyName, expression: Expression) extends MutatingPattern

case class SetIncludingRelationshipPropertiesFromMapPattern(idName: IdName, expression: Expression) extends MutatingPattern

case class RemoveLabelPattern(idName: IdName, labels: Seq[LabelName]) extends MutatingPattern

case class DeleteExpression(expression: Expression, forced: Boolean) extends MutatingPattern
