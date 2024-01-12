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

case class MapProjection(
  name: Variable, // Since this is always rewritten to DesugaredMapProjection this
  // (and in the elements below) may not need to be LogicalVariable
  items: Seq[MapProjectionElement]
)(val position: InputPosition) extends Expression {
  override def isConstantForQuery: Boolean = items.forall(_.isConstantForQuery)
}

case class DesugaredMapProjection(entity: Expression, items: Seq[LiteralEntry], includeAllProps: Boolean)(
  val position: InputPosition
) extends Expression {

  override def isConstantForQuery: Boolean =
    entity.isConstantForQuery && items.forall(_.isConstantForQuery)
}

sealed trait MapProjectionElement extends Expression

case class LiteralEntry(key: PropertyKeyName, exp: Expression)(val position: InputPosition)
    extends MapProjectionElement {
  override def isConstantForQuery: Boolean = exp.isConstantForQuery
}

case class VariableSelector(id: Variable)(val position: InputPosition) extends MapProjectionElement {
  // we need the variable to read
  override def isConstantForQuery: Boolean = false
}

case class PropertySelector(key: PropertyKeyName)(val position: InputPosition) extends MapProjectionElement {
  // we need the variable to read
  override def isConstantForQuery: Boolean = false
}

case class AllPropertiesSelector()(val position: InputPosition) extends MapProjectionElement {
  override def isConstantForQuery: Boolean = true
}
