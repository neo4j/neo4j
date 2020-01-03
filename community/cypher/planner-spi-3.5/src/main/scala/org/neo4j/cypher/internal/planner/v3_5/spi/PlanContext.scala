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
package org.neo4j.cypher.internal.planner.v3_5.spi

import org.neo4j.cypher.internal.v3_5.logical.plans.{ProcedureSignature, QualifiedName, UserFunctionSignature}
import org.neo4j.cypher.internal.v3_5.frontend.phases.InternalNotificationLogger

/**
 * PlanContext is an internal access layer to the graph that is solely used during plan building
 *
 * As such it is similar to QueryContext. The reason for separating both interfaces is that we
 * want to control what operations can be executed at runtime.  For example, we do not give access
 * to index rule lookup in QueryContext as that should happen at query compile time.
 */
trait PlanContext extends TokenContext with ProcedureSignatureResolver {

  /**
    * Return all indexes (general and unique) for a given label
    */
  def indexesGetForLabel(labelId: Int): Iterator[IndexDescriptor]

  /**
    * Return all unique indexes for a given label
    */
  def uniqueIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor]

  /**
    * Checks if an index exists (general or unique) for a given label
    */
  def indexExistsForLabel(labelId: Int): Boolean

  /**
    * Gets an index index if it exists (general or unique) for a given label and properties
    */
  def indexGetForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Option[IndexDescriptor]

  /**
    * Checks if an index exists (general or unique) for a given label and properties
    */
  def indexExistsForLabelAndProperties(labelName: String, propertyKey: Seq[String]): Boolean

  def hasPropertyExistenceConstraint(labelName: String, propertyKey: String): Boolean

  def checkNodeIndex(idxName: String)

  def checkRelIndex(idxName: String)

  def txIdProvider: () => Long

  def statistics: InstrumentedGraphStatistics

  def notificationLogger(): InternalNotificationLogger
}

trait ProcedureSignatureResolver {
  def procedureSignature(name: QualifiedName): ProcedureSignature
  def functionSignature(name: QualifiedName): Option[UserFunctionSignature]
}
