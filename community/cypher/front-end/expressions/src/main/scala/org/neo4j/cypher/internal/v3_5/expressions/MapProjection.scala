/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.v3_5.expressions

import org.neo4j.cypher.internal.v3_5.util.InputPosition

case class MapProjection(
                          name: Variable, // Since this is always rewritten to DesugaredMapProjection this
                                          // (and in the elements below) may not need to be LogicalVariable
                          items: Seq[MapProjectionElement])
                        (val position: InputPosition, val definitionPos: Option[InputPosition])
  extends Expression {

  def withDefinitionPos(pos:InputPosition): MapProjection =
    copy()(position, Some(pos))

  override def dup(children: Seq[AnyRef]): this.type = {
    MapProjection(
      children(0).asInstanceOf[Variable],
      children(1).asInstanceOf[Seq[MapProjectionElement]]
    )(position, definitionPos).asInstanceOf[this.type]
  }
}

case class DesugaredMapProjection(
                                   name: LogicalVariable,
                                   items: Seq[LiteralEntry],
                                   includeAllProps: Boolean
                                 )(val position: InputPosition) extends Expression


sealed trait MapProjectionElement extends Expression

case class LiteralEntry(key: PropertyKeyName, exp: Expression)(val position: InputPosition) extends MapProjectionElement
case class VariableSelector(id: Variable)(val position: InputPosition) extends MapProjectionElement
case class PropertySelector(id: Variable)(val position: InputPosition) extends MapProjectionElement
case class AllPropertiesSelector()(val position: InputPosition) extends MapProjectionElement
