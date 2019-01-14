/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.planner.v3_4.spi

import org.neo4j.cypher.internal.frontend.v3_4.phases.InternalNotificationLogger
import org.neo4j.cypher.internal.v3_4.logical.plans.{ProcedureSignature, QualifiedName, UserFunctionSignature}

/**
 * PlanContext is an internal access layer to the graph that is solely used during plan building
 *
 * As such it is similar to QueryContext. The reason for separating both interfaces is that we
 * want to control what operations can be executed at runtime.  For example, we do not give access
 * to index rule lookup in QueryContext as that should happen at query compile time.
 */
trait PlanContext extends TokenContext with ProcedureSignatureResolver {

  def indexesGetForLabel(labelId: Int): Iterator[IndexDescriptor]

  def indexGet(labelName: String, propertyKeys: Seq[String]): Option[IndexDescriptor]

  def indexExistsForLabel(labelName: String): Boolean

  def uniqueIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor]

  def uniqueIndexGet(labelName: String, propertyKey: Seq[String]): Option[IndexDescriptor]

  def hasPropertyExistenceConstraint(labelName: String, propertyKey: String): Boolean

  def checkNodeIndex(idxName: String)

  def checkRelIndex(idxName: String)

  def getOrCreateFromSchemaState[T](key: Any, f: => T): T

  def txIdProvider: () => Long

  def statistics: GraphStatistics

  def notificationLogger(): InternalNotificationLogger

  def twoLayerTransactionState(): Boolean
}

trait ProcedureSignatureResolver {
  def procedureSignature(name: QualifiedName): ProcedureSignature
  def functionSignature(name: QualifiedName): Option[UserFunctionSignature]
}
