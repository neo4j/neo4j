/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.helpers

import org.neo4j.cypher.internal.logical.builder.Resolver
import org.neo4j.cypher.internal.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.logical.plans.UserFunctionSignature
import org.neo4j.cypher.internal.planner.spi.TokenContext

import scala.collection.mutable.ArrayBuffer

case class TokenContainer(
                           labels: Seq[String] = Seq.empty,
                           properties: Seq[String] = Seq.empty,
                           relTypes: Seq[String] = Seq.empty,
                         ) {

  def addLabel(label: String): TokenContainer = this.copy(labels = labels :+ label)
  def addRelType(relType: String): TokenContainer = this.copy(relTypes = relTypes :+ relType)
  def addProperty(property: String): TokenContainer = this.copy(properties = properties :+ property)

  def getResolver: LogicalPlanResolver = new LogicalPlanResolver(
    labels.to[ArrayBuffer],
    properties.to[ArrayBuffer],
    relTypes.to[ArrayBuffer]
  )
}

class LogicalPlanResolver(
                           labels: ArrayBuffer[String] = new ArrayBuffer[String](),
                           properties: ArrayBuffer[String] = new ArrayBuffer[String](),
                           relTypes: ArrayBuffer[String] = new ArrayBuffer[String]()
                         ) extends Resolver with TokenContext {

  override def getLabelId(label: String): Int = {
    val index = labels.indexOf(label)
    if (index == -1) {
      labels += label
      labels.size - 1
    } else {
      index
    }
  }

  override def getPropertyKeyId(prop: String): Int = {
    val index = properties.indexOf(prop)
    if (index == -1) {
      properties += prop
      properties.size - 1
    } else {
      index
    }
  }

  override def getRelTypeId(relType: String): Int = {
    val index = relTypes.indexOf(relType)
    if (index == -1) {
      relTypes += relType
      relTypes.size - 1
    } else {
      index
    }
  }

  override def getLabelName(id: Int): String = if (id >= labels.size) throw new IllegalStateException(s"Label $id undefined") else labels(id)

  override def getOptLabelId(labelName: String): Option[Int] = Some(getLabelId(labelName))

  override def getPropertyKeyName(id: Int): String = if (id >= properties.size) throw new IllegalStateException(s"Property $id undefined") else properties(id)

  override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] = Some(getPropertyKeyId(propertyKeyName))

  override def getRelTypeName(id: Int): String = if (id >= relTypes.size) throw new IllegalStateException(s"RelType $id undefined") else relTypes(id)

  override def getOptRelTypeId(relType: String): Option[Int] = Some(getRelTypeId(relType))

  override def procedureSignature(name: QualifiedName): ProcedureSignature = ???

  override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] = ???
}
