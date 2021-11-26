/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.planner.spi

import org.neo4j.cypher.internal.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.logical.plans.UserFunctionSignature
import org.neo4j.cypher.internal.util.InternalNotificationLogger

/**
 * PlanContext is an internal access layer to the graph that is solely used during plan building.
 *
 * As such it is similar to QueryContext. The reason for separating both interfaces is that we
 * want to control what operations can be executed at runtime.  For example, we do not give access
 * to index rule lookup in QueryContext as that should happen at query compile time.
 *
 * None of the functions in PlanContext takes any schema lock.
 */
trait PlanContext extends ReadTokenContext with ProcedureSignatureResolver {

  /**
   * Return all BTREE indexes (general and unique) for a given label, without taking any schema locks.
   */
  def btreeIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor]

  /**
   * Return all BTREE indexes for a given relationship type, without taking any schema locks.
   */
  def btreeIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor]

  /**
   * Return all range indexes (general and unique) for a given label, without taking any schema locks.
   */
  def rangeIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor]

  /**
   * Return all range indexes for a given relationship type, without taking any schema locks.
   */
  def rangeIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor]

  /**
   * Return all text indexes for a given label, without taking any schema locks.
   */
  def textIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor]

  /**
   * Return all text indexes for a given relationship type, without taking any schema locks.
   */
  def textIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor]

  /**
   * Return all property indexes present in the database, without taking any schema locks.
   */
  def propertyIndexesGetAll(): Iterator[IndexDescriptor]

  /**
   * Checks if an index exists (general or unique) for a given label, without taking any schema locks.
   */
  def indexExistsForLabel(labelId: Int): Boolean

  /**
   * Gets a BTREE index if it exists (general or unique) for a given label and properties, without taking any schema locks.
   */
  def btreeIndexGetForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Option[IndexDescriptor]

  /**
   * Gets a TEXT index if it exists (general or unique) for a given label and properties, without taking any schema locks.
   */
  def textIndexGetForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Option[IndexDescriptor]

  /**
   * Gets a BTREE index if it exists for a given relationship type and properties, without taking any schema locks.
   */
  def btreeIndexGetForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Option[IndexDescriptor]

  /**
   * Gets a TEXT index if it exists for a given relationship type and properties, without taking any schema locks.
   */
  def textIndexGetForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Option[IndexDescriptor]

  /**
   * Checks if a BTREE index exists (general or unique) for a given label and properties, without taking any schema locks.
   */
  def btreeIndexExistsForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Boolean

  /**
   * Checks if a TEXT index exists (general or unique) for a given label and properties, without taking any schema locks.
   */
  def textIndexExistsForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Boolean

  /**
   * Checks if a BTREE exists for a given relationship type and properties, without taking any schema locks.
   */
  def btreeIndexExistsForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Boolean

  /**
   * Checks if a TEXT index exists for a given relationship type and properties, without taking any schema locks.
   */
  def textIndexExistsForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Boolean

  /**
   * Checks if it is possible to lookup nodes by their labels (either through the scan store or a lookup index). Does not take any schema locks.
   */
  def canLookupNodesByLabel: Boolean

  /**
   * Checks if it is possible to lookup relationships by their types, without taking any schema locks.
   */
  def canLookupRelationshipsByType: Boolean

  def hasNodePropertyExistenceConstraint(labelName: String, propertyKey: String): Boolean

  def getNodePropertiesWithExistenceConstraint(labelName: String): Set[String]

  def hasRelationshipPropertyExistenceConstraint(relationshipTypeName: String, propertyKey: String): Boolean

  def getRelationshipPropertiesWithExistenceConstraint(relationshipTypeName: String): Set[String]

  def getPropertiesWithExistenceConstraint: Set[String]

  /**
   * @return a provider for the highest seen committed transaction id.
   */
  def lastCommittedTxIdProvider: () => Long

  def statistics: InstrumentedGraphStatistics

  def notificationLogger(): InternalNotificationLogger

  /**
   * Checks if there are uncommitted changes in the transaction state.
   */
  def txStateHasChanges(): Boolean
}

trait ProcedureSignatureResolver {
  def procedureSignature(name: QualifiedName): ProcedureSignature
  def functionSignature(name: QualifiedName): Option[UserFunctionSignature]
}
