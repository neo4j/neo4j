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
import org.neo4j.cypher.internal.logical.plans.{ProcedureSignature, QualifiedName, UserFunctionSignature}

import scala.collection.mutable.ArrayBuffer

class LogicalPlanResolver extends Resolver {
  private val labels = new ArrayBuffer[String]()
  private val properties = new ArrayBuffer[String]()

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

  override def procedureSignature(name: QualifiedName): ProcedureSignature = ???

  override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] = ???
}
