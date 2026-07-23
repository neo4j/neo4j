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

trait AbstractRecordType extends CypherType {
  def fields: Map[String, CypherType]
  def defaultFieldType: CypherType
  def isNullable: Boolean

  val isFieldOpen: Boolean = defaultFieldType match {
    case NothingType() => false
    case _             => true
  }

  def withFields(fields: Map[String, CypherType]): AbstractRecordType

  def toFieldTypesString: String = {
    val fieldStrings = fields.map {
      case (name, typ) => s"$name :: ${typ.description}"
    }.toSeq
    val fieldStringsWithDefaults =
      defaultFieldType match {
        case NothingType() => fieldStrings
        case AnyType(true) => fieldStrings
        case t             => fieldStrings :+ s"_ :: ${t.description}"
      }
    fieldStringsWithDefaults.mkString("{", ", ", "}")
  }

  def fieldType(name: String): CypherType = fields.getOrElse(name, defaultFieldType)

  // type only contains the empty record
  def onlyEmpty: Boolean = fields.isEmpty && defaultFieldType.isNothing
}
