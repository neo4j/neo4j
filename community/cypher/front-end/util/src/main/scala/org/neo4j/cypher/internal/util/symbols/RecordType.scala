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

case class RecordType(
  override val fields: Map[String, CypherType],
  override val defaultFieldType: CypherType,
  isBaseTypeOpen: Boolean,
  override val isNullable: Boolean
)(val position: InputPosition) extends AbstractRecordType {

  override def parentType: CypherType = ???

  override def withIsNullable(isNullable: Boolean): RecordType = this.copy(isNullable = isNullable)(position)

  override def withPosition(newPosition: InputPosition): RecordType = this.copy()(position = newPosition)

  override def withFields(fields: Map[String, CypherType]): RecordType = this.copy(fields = fields)(position)

  override def sortOrder: Int = CypherTypeOrder.RECORD.id

  override def toCypherTypeString: String =
    (if (isFieldOpen) "ANY " else "") + toFieldTypesString

  override def toClassString: String = "Record"
}

object RecordType {

  def apply(
    fields: Map[String, CypherType],
    isFieldOpen: Boolean,
    isBaseTypeOpen: Boolean,
    isNullable: Boolean
  )(position: InputPosition): RecordType = {
    val defaultFieldType = if (isFieldOpen) AnyType(isNullable = true)(position) else NothingType()(position)
    RecordType(fields, defaultFieldType, isBaseTypeOpen, isNullable)(position)
  }

  def any(isNullable: Boolean)(position: InputPosition): RecordType =
    RecordType(Map.empty, isFieldOpen = true, isBaseTypeOpen = true, isNullable)(position)

  object Any {

    def unapply(rt: RecordType): Option[Boolean] = rt match {
      case RecordType(fields, AnyType(true), true, isNullable) if fields.isEmpty => Some(isNullable)
      case _                                                                     => None
    }
  }
}
