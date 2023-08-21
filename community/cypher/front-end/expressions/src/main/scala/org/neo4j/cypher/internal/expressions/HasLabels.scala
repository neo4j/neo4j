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

/**
 * Boolean expression evaluating the labels or types of a node or relationship.
 */
trait LabelOrTypeCheckExpression extends BooleanExpression {
  def entityExpression: Expression
  override def isConstantForQuery: Boolean = false
}

/**
 * Boolean expression evaluating the labels of a node.
 */
trait LabelCheckExpression extends BooleanExpression {
  def expression: Expression
  override def isConstantForQuery: Boolean = false
}

/*
 * Checks if expression has all labels
 */
case class HasLabels(expression: Expression, labels: Seq[LabelName])(val position: InputPosition)
    extends LabelCheckExpression {

  override def asCanonicalStringVal =
    s"${expression.asCanonicalStringVal}${labels.map(_.asCanonicalStringVal).mkString(":", ":", "")}"
}

/*
 * Checks if expression has any of the specified labels
 */
case class HasAnyLabel(expression: Expression, labels: Seq[LabelName])(val position: InputPosition)
    extends LabelCheckExpression {

  override def asCanonicalStringVal =
    s"${expression.asCanonicalStringVal}${labels.map(_.asCanonicalStringVal).mkString(":", "|", "")}"
}

/*
 * Checks if expression has all labels OR all types
 */
case class HasLabelsOrTypes(entityExpression: Expression, labelsOrTypes: Seq[LabelOrRelTypeName])(
  val position: InputPosition
) extends LabelOrTypeCheckExpression {

  override def asCanonicalStringVal =
    s"${entityExpression.asCanonicalStringVal}${labelsOrTypes.map(_.asCanonicalStringVal).mkString(":", ":", "")}"
}

/**
 * Checks if expression has at least one label or type. That is always true for relationships but not necessarily for nodes.
 */
case class HasALabelOrType(entityExpression: Expression)(val position: InputPosition)
    extends LabelOrTypeCheckExpression {

  override def asCanonicalStringVal =
    s"${entityExpression.asCanonicalStringVal}:%"
}

/**
 * Checks if expression has at least one label.
 */
case class HasALabel(expression: Expression)(val position: InputPosition) extends LabelCheckExpression {

  override def asCanonicalStringVal =
    s"${expression.asCanonicalStringVal}:%"
}

/*
 * Checks if expression has all types
 */
case class HasTypes(expression: Expression, types: Seq[RelTypeName])(val position: InputPosition)
    extends BooleanExpression {

  override def asCanonicalStringVal =
    s"${expression.asCanonicalStringVal}${types.map(_.asCanonicalStringVal).mkString(":", ":", "")}"

  override def isConstantForQuery: Boolean = false
}
