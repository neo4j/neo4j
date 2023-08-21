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
package org.neo4j.cypher.internal.logical.builder

import org.neo4j.cypher.internal.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.logical.plans.UserFunctionSignature

import scala.collection.mutable.ArrayBuffer

case class SimpleResolver(
  labels: ArrayBuffer[String] = new ArrayBuffer[String](),
  properties: ArrayBuffer[String] = new ArrayBuffer[String](),
  relTypes: ArrayBuffer[String] = new ArrayBuffer[String](),
  procedures: Set[ProcedureSignature] = Set.empty
) extends Resolver {

  override def getLabelId(label: String): Int = {
    safeIndexOf(label, labels)
  }

  override def getRelTypeId(label: String): Int = {
    safeIndexOf(label, relTypes)
  }

  override def getPropertyKeyId(prop: String): Int = {
    safeIndexOf(prop, properties)
  }

  private def safeIndexOf(token: String, idStore: ArrayBuffer[String]) = {
    val index = idStore.indexOf(token)
    if (index == -1) {
      idStore += token
      idStore.size - 1
    } else {
      index
    }
  }

  override def procedureSignature(name: QualifiedName): ProcedureSignature =
    procedures.find(_.name == name).getOrElse(throw new IllegalStateException(s"No procedure signature for $name"))

  override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] = ???
}
