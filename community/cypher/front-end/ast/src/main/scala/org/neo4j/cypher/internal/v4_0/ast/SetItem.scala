/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.v4_0.ast

import org.neo4j.cypher.internal.v4_0.ast.semantics.{SemanticAnalysisTooling, SemanticCheckable, SemanticExpressionCheck, _}
import org.neo4j.cypher.internal.v4_0.expressions.{Expression, LabelName, LogicalProperty, Variable}
import org.neo4j.cypher.internal.v4_0.util.symbols._
import org.neo4j.cypher.internal.v4_0.util.{ASTNode, InputPosition}

sealed trait SetItem extends ASTNode with SemanticCheckable

case class SetLabelItem(variable: Variable, labels: Seq[LabelName])(val position: InputPosition) extends SetItem {
  def semanticCheck =
    SemanticExpressionCheck.simple(variable) chain
      SemanticPatternCheck.checkValidLabels(labels, position) chain
      SemanticExpressionCheck.expectType(CTNode.covariant, variable)
}

sealed trait SetProperty extends SetItem with SemanticAnalysisTooling

case class SetPropertyItem(property: LogicalProperty, expression: Expression)(val position: InputPosition) extends SetProperty {
  def semanticCheck =
    SemanticExpressionCheck.simple(property) chain
      SemanticPatternCheck.checkValidPropertyKeyNames(Seq(property.propertyKey), property.position) chain
      SemanticExpressionCheck.simple(expression) chain
      expectType(CTNode.covariant | CTRelationship.covariant, property.map)
}

case class SetExactPropertiesFromMapItem(variable: Variable, expression: Expression)
                                        (val position: InputPosition) extends SetProperty {
  def semanticCheck =
    SemanticExpressionCheck.simple(variable) chain
      expectType(CTNode.covariant | CTRelationship.covariant, variable) chain
      SemanticExpressionCheck.simple(expression) chain
      expectType(CTMap.covariant, expression)
}

case class SetIncludingPropertiesFromMapItem(variable: Variable, expression: Expression)
                                        (val position: InputPosition) extends SetProperty {
  def semanticCheck =
    SemanticExpressionCheck.simple(variable) chain
      expectType(CTNode.covariant | CTRelationship.covariant, variable) chain
      SemanticExpressionCheck.simple(expression) chain
      expectType(CTMap.covariant, expression)
}
