/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.InputPosition
import org.neo4j.cypher.internal.frontend.v3_3.SemanticCheckable

sealed trait SetItem extends ASTNode with ASTPhrase with SemanticCheckable

case class SetLabelItem(variable: Variable, labels: Seq[LabelName])(val position: InputPosition) extends SetItem {
  def semanticCheck =
    variable.semanticCheck(Expression.SemanticContext.Simple) chain
      variable.expectType(CTNode.covariant)
}

sealed trait SetProperty extends SetItem

case class SetPropertyItem(property: Property, expression: Expression)(val position: InputPosition)
    extends SetProperty {
  def semanticCheck =
    property.semanticCheck(Expression.SemanticContext.Simple) chain
      expression.semanticCheck(Expression.SemanticContext.Simple) chain
      property.map.expectType(CTNode.covariant | CTRelationship.covariant)
}

case class SetExactPropertiesFromMapItem(variable: Variable, expression: Expression)(val position: InputPosition)
    extends SetProperty {
  def semanticCheck =
    variable.semanticCheck(Expression.SemanticContext.Simple) chain
      variable.expectType(CTNode.covariant | CTRelationship.covariant) chain
      expression.semanticCheck(Expression.SemanticContext.Simple) chain
      expression.expectType(CTMap.covariant)
}

case class SetIncludingPropertiesFromMapItem(variable: Variable, expression: Expression)(val position: InputPosition)
    extends SetProperty {
  def semanticCheck =
    variable.semanticCheck(Expression.SemanticContext.Simple) chain
      variable.expectType(CTNode.covariant | CTRelationship.covariant) chain
      expression.semanticCheck(Expression.SemanticContext.Simple) chain
      expression.expectType(CTMap.covariant)
}
