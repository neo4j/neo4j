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
package org.neo4j.cypher.internal.planner.spi

import org.neo4j.cypher.internal.frontend.phases.ProcedureSignatureResolver
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.internal.schema.constraints.SchemaValueType

/**
 * This is used to determine the kind of database that is being used.
 * SINGLE: A standard database
 * COMPOSITE: A composite database
 * SHARDED: A sharded database (which means properties are stored in a separate store and not in the graph store)
 */
object DatabaseMode extends Enumeration {
  type DatabaseMode = Value
  val SINGLE, COMPOSITE, SHARDED = Value
}

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
   * Return all point indexes for a given label, without taking any schema locks.
   */
  def pointIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor]

  /**
   * Return all point indexes for a given relationship type, without taking any schema locks.
   */
  def pointIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor]

  /**
   * Return all property indexes present in the database, without taking any schema locks.
   */
  def propertyIndexesGetAll(): Iterator[IndexDescriptor]

  /**
   * Checks if an index exists (general or unique) for a given label, without taking any schema locks.
   */
  def indexExistsForLabel(labelId: Int): Boolean

  /**
   * Checks if an index exists (general or unique) for a given label, without taking any schema locks.
   */
  def indexExistsForRelType(relTypeId: Int): Boolean

  /**
   * Gets a TEXT index if it exists (general or unique) for a given label and properties, without taking any schema locks.
   */
  def textIndexGetForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Option[IndexDescriptor]

  /**
   * Gets a RANGE index if it exists (general or unique) for a given label and properties, without taking any schema locks.
   */
  def rangeIndexGetForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Option[IndexDescriptor]

  /**
   * Gets a POINT index if it exists (general or unique) for a given label and properties, without taking any schema locks.
   */
  def pointIndexGetForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Option[IndexDescriptor]

  /**
   * Gets a TEXT index if it exists for a given relationship type and properties, without taking any schema locks.
   */
  def textIndexGetForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Option[IndexDescriptor]

  /**
   * Gets a RANGE index if it exists for a given relationship type and properties, without taking any schema locks.
   */
  def rangeIndexGetForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Option[IndexDescriptor]

  /**
   * Gets a POINT index if it exists for a given relationship type and properties, without taking any schema locks.
   */
  def pointIndexGetForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Option[IndexDescriptor]

  /**
   * Checks if a TEXT index exists (general or unique) for a given label and properties, without taking any schema locks.
   */
  def textIndexExistsForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Boolean

  /**
   * Checks if a RANGE index exists (general or unique) for a given label and properties, without taking any schema locks.
   */
  def rangeIndexExistsForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Boolean

  /**
   * Checks if a POINT index exists (general or unique) for a given label and properties, without taking any schema locks.
   */
  def pointIndexExistsForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Boolean

  /**
   * Checks if a TEXT index exists for a given relationship type and properties, without taking any schema locks.
   */
  def textIndexExistsForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Boolean

  /**
   * Checks if a RANGE index exists for a given relationship type and properties, without taking any schema locks.
   */
  def rangeIndexExistsForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Boolean

  /**
   * Checks if a POINT index exists for a given relationship type and properties, without taking any schema locks.
   */
  def pointIndexExistsForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Boolean

  /**
   * Checks if it is possible to lookup nodes by their labels (either through the scan store or a lookup index). Does not take any schema locks.
   * Returns an IndexDescriptor if it is possible, otherwise None.
   */
  def nodeTokenIndex: Option[TokenIndexDescriptor]

  /**
   * Checks if it is possible to lookup relationships by their types, without taking any schema locks.
   * Returns an IndexDescriptor if it is possible, otherwise None.
   */
  def relationshipTokenIndex: Option[TokenIndexDescriptor]

  def hasNodePropertyExistenceConstraint(labelName: String, propertyKey: String): Boolean

  def getNodePropertiesWithExistenceConstraint(labelName: String): Set[String]

  def hasRelationshipPropertyExistenceConstraint(relationshipTypeName: String, propertyKey: String): Boolean

  def getRelationshipPropertiesWithExistenceConstraint(relationshipTypeName: String): Set[String]

  def getPropertiesWithExistenceConstraint: Set[String]

  def getNodePropertiesWithTypeConstraint(labelName: String): Map[String, Seq[SchemaValueType]]

  def hasNodePropertyTypeConstraint(labelName: String, propertyKey: String, cypherType: SchemaValueType): Boolean

  def getRelationshipPropertiesWithTypeConstraint(relTypeName: String): Map[String, Seq[SchemaValueType]]

  def hasRelationshipPropertyTypeConstraint(
    relTypeName: String,
    propertyKey: String,
    cypherType: SchemaValueType
  ): Boolean

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

  /**
   * Return a copy with the given notificationLogger
   */
  def withNotificationLogger(notificationLogger: InternalNotificationLogger): PlanContext

  /**
   * Return the database mode
   */
  def databaseMode: DatabaseMode.DatabaseMode
}
