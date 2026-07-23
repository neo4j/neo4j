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
package org.neo4j.cypher.internal.util.symbols

import org.neo4j.cypher.internal.util.InputPosition

case class NodeReferenceValueType(
  labels: Set[String],
  override val fields: Map[String, CypherType],
  override val defaultFieldType: CypherType,
  override val isNullable: Boolean
)(val position: InputPosition) extends AbstractRecordType {
  val isOpen: Boolean = isFieldOpen

  override def parentType: CypherType = ???

  override def withIsNullable(isNullable: Boolean): NodeReferenceValueType =
    this.copy(isNullable = isNullable)(position)

  override def withPosition(newPosition: InputPosition): NodeReferenceValueType = this.copy()(position = newPosition)

  override def withFields(fields: Map[String, CypherType]): NodeReferenceValueType =
    this.copy(fields = fields)(position)

  override def sortOrder: Int = CypherTypeOrder.NODE_REFERENCE.id

  override def toCypherTypeString: String = {
    val semantics = if (isOpen) "ANY " else ""
    val labelsString = if (labels.isEmpty) "" else labels.mkString(":", "&", " ")
    val propertiesString = toFieldTypesString
    s"${semantics}NODE ($labelsString$propertiesString)"
  }

  override def toClassString: String = "NodeReferenceValue"
}

object NodeReferenceValueType {

  def apply(
    labels: Set[String],
    fields: Map[String, CypherType],
    isFieldOpen: Boolean,
    isNullable: Boolean
  )(position: InputPosition): NodeReferenceValueType = {
    val defaultFieldType = if (isFieldOpen) AnyType(isNullable = true)(position) else NothingType()(position)
    NodeReferenceValueType(labels, fields, defaultFieldType, isNullable)(position)
  }

  def any(isNullable: Boolean)(position: InputPosition): NodeReferenceValueType =
    NodeReferenceValueType(Set.empty, Map.empty, isFieldOpen = true, isNullable)(position)

  object Any {

    def unapply(nrt: NodeReferenceValueType): Option[Boolean] = nrt match {
      case NodeReferenceValueType(labels, fields, AnyType(true), isNullable) if labels.isEmpty && fields.isEmpty =>
        Some(isNullable)
      case _ => None
    }
  }
}
