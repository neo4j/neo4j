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

import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.NameId
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RelTypeId

sealed abstract class NameToken[I <: NameId] {
  def name: String
  def nameId: I
}

object LabelToken {
  def apply(symbolicName: LabelName, nameId: LabelId): LabelToken = LabelToken(symbolicName.name, nameId)
}

final case class LabelToken(name: String, nameId: LabelId) extends NameToken[LabelId]

object RelationshipTypeToken {

  def apply(symbolicName: RelTypeName, nameId: RelTypeId): RelationshipTypeToken =
    RelationshipTypeToken(symbolicName.name, nameId)
}

final case class RelationshipTypeToken(name: String, nameId: RelTypeId) extends NameToken[RelTypeId]

object PropertyKeyToken {

  def apply(symbolicName: PropertyKeyName, nameId: PropertyKeyId): PropertyKeyToken =
    PropertyKeyToken(symbolicName.name, nameId)
}

final case class PropertyKeyToken(name: String, nameId: PropertyKeyId) extends NameToken[PropertyKeyId]
