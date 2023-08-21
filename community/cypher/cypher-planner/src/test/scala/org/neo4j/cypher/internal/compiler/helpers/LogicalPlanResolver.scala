/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.helpers

import org.neo4j.cypher.internal.logical.builder.SimpleResolver
import org.neo4j.cypher.internal.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext

import scala.collection.mutable.ArrayBuffer

case class TokenContainer(
  labels: Set[String] = Set.empty,
  properties: Set[String] = Set.empty,
  relTypes: Set[String] = Set.empty
) {

  def addLabel(label: String): TokenContainer = this.copy(labels = labels + label)
  def addRelType(relType: String): TokenContainer = this.copy(relTypes = relTypes + relType)
  def addProperty(property: String): TokenContainer = this.copy(properties = properties + property)

  def getResolver(procedures: Set[ProcedureSignature]): LogicalPlanResolver = new LogicalPlanResolver(
    labels.to(ArrayBuffer),
    properties.to(ArrayBuffer),
    relTypes.to(ArrayBuffer),
    procedures
  )
}

class LogicalPlanResolver(
  labels: ArrayBuffer[String] = new ArrayBuffer[String](),
  properties: ArrayBuffer[String] = new ArrayBuffer[String](),
  relTypes: ArrayBuffer[String] = new ArrayBuffer[String](),
  procedures: Set[ProcedureSignature] = Set.empty
) extends SimpleResolver(labels, properties, relTypes, procedures)
    with ReadTokenContext {

  override def getLabelName(id: Int): String =
    if (id >= labels.size) throw new IllegalStateException(s"Label $id undefined") else labels(id)

  override def getOptLabelId(labelName: String): Option[Int] = Some(getLabelId(labelName))

  override def getPropertyKeyName(id: Int): String =
    if (id >= properties.size) throw new IllegalStateException(s"Property $id undefined") else properties(id)

  override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] = Some(getPropertyKeyId(propertyKeyName))

  override def getRelTypeName(id: Int): String =
    if (id >= relTypes.size) throw new IllegalStateException(s"RelType $id undefined") else relTypes(id)

  override def getOptRelTypeId(relType: String): Option[Int] = Some(getRelTypeId(relType))
}
