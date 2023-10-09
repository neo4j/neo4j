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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.ast.semantics.SemanticAnalysisTooling
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckable
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticPatternCheck
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship

sealed trait SetItem extends ASTNode with SemanticCheckable

case class SetLabelItem(variable: Variable, labels: Seq[LabelName])(val position: InputPosition) extends SetItem {

  def semanticCheck =
    SemanticExpressionCheck.simple(variable) chain
      SemanticPatternCheck.checkValidLabels(labels, position) chain
      SemanticExpressionCheck.expectType(CTNode.covariant, variable)
}

sealed trait SetProperty extends SetItem with SemanticAnalysisTooling

case class SetPropertyItem(property: LogicalProperty, expression: Expression)(val position: InputPosition)
    extends SetProperty {

  def semanticCheck =
    SemanticExpressionCheck.simple(property) chain
      SemanticPatternCheck.checkValidPropertyKeyNames(Seq(property.propertyKey)) chain
      SemanticExpressionCheck.simple(expression) chain
      expectType(CTNode.covariant | CTRelationship.covariant, property.map)

}

case class SetPropertyItems(map: Expression, items: Seq[(PropertyKeyName, Expression)])(val position: InputPosition)
    extends SetProperty {

  def semanticCheck = {

    val properties = items.map(_._1)
    val expressions = items.map(_._2)
    SemanticExpressionCheck.simple(map) chain
      semanticCheckFold(properties) { property =>
        SemanticPatternCheck.checkValidPropertyKeyNames(Seq(property))
      } chain
      SemanticExpressionCheck.simple(expressions) chain
      expectType(CTNode.covariant | CTRelationship.covariant, map)
  }
}

case class SetExactPropertiesFromMapItem(variable: Variable, expression: Expression)(val position: InputPosition)
    extends SetProperty {

  def semanticCheck =
    SemanticExpressionCheck.simple(variable) chain
      expectType(CTNode.covariant | CTRelationship.covariant, variable) chain
      SemanticExpressionCheck.simple(expression) chain
      expectType(CTMap.covariant, expression)
}

case class SetIncludingPropertiesFromMapItem(variable: Variable, expression: Expression)(val position: InputPosition)
    extends SetProperty {

  def semanticCheck =
    SemanticExpressionCheck.simple(variable) chain
      expectType(CTNode.covariant | CTRelationship.covariant, variable) chain
      SemanticExpressionCheck.simple(expression) chain
      expectType(CTMap.covariant, expression)
}
