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

case class RelationshipReferenceValueType(
  label: Option[String],
  override val fields: Map[String, CypherType],
  override val defaultFieldType: CypherType,
  source: NodeReferenceValueType,
  destination: NodeReferenceValueType,
  override val isNullable: Boolean
)(val position: InputPosition) extends AbstractRecordType {
  val isOpen: Boolean = isFieldOpen

  override def parentType: CypherType = ???

  override def withIsNullable(isNullable: Boolean): RelationshipReferenceValueType =
    this.copy(isNullable = isNullable)(position)

  override def withPosition(newPosition: InputPosition): RelationshipReferenceValueType =
    this.copy()(position = newPosition)

  override def withFields(fields: Map[String, CypherType]): RelationshipReferenceValueType =
    this.copy(fields = fields)(position)

  override def sortOrder: Int = CypherTypeOrder.RELATIONSHIP_REFERENCE.id

  override def toCypherTypeString: String = {
    val semantics = if (isOpen) "ANY " else ""
    val labelString = label.map(l => s"$l ").getOrElse("")
    val propertiesString = toFieldTypesString
    val sourceString = source.toCypherTypeString
    val destinationString = destination.toCypherTypeString
    s"${semantics}RELATIONSHIP ($sourceString)-[$labelString$propertiesString]->($destinationString)"
  }

  override def toClassString: String = "RelationshipReferenceValue"
}

object RelationshipReferenceValueType {

  def apply(
    label: Option[String],
    fields: Map[String, CypherType],
    isFieldOpen: Boolean,
    source: NodeReferenceValueType,
    destination: NodeReferenceValueType,
    isNullable: Boolean
  )(position: InputPosition): RelationshipReferenceValueType = {
    val defaultFieldType = if (isFieldOpen) AnyType(isNullable = true)(position) else NothingType()(position)
    RelationshipReferenceValueType(label, fields, defaultFieldType, source, destination, isNullable)(position)
  }

  def any(isNullable: Boolean)(position: InputPosition): RelationshipReferenceValueType = {
    val endpointType = NodeReferenceValueType.any(isNullable = false)(position)
    RelationshipReferenceValueType(
      None,
      Map.empty,
      isFieldOpen = true,
      endpointType,
      endpointType,
      isNullable
    )(position)
  }

  object Any {

    def unapply(rrt: RelationshipReferenceValueType): Option[Boolean] = rrt match {
      case RelationshipReferenceValueType(
          label,
          fields,
          AnyType(true),
          NodeReferenceValueType.Any(true),
          NodeReferenceValueType.Any(true),
          isNullable
        ) if label.isEmpty && fields.isEmpty => Some(isNullable)
      case _ => None
    }
  }
}
