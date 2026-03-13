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
package org.neo4j.cypher.internal.runtime

import org.neo4j.kernel.api.query.ExtendedQueryStatistics

import scala.beans.BeanProperty

// Whenever you add a field here, please also update
// org.neo4j.graphdb.QueryStatistics
// org.neo4j.bolt.runtime.AbstractCypherAdapterStream#queryStats (team drivers does that)
// org.neo4j.cypher.QueryStatisticsTestSupport

case class QueryStatistics(
  nodesCreated: Long = 0,
  relationshipsCreated: Long = 0,
  propertiesSet: Long = 0,
  nodesDeleted: Long = 0,
  relationshipsDeleted: Long = 0,
  labelsAdded: Long = 0,
  labelsRemoved: Long = 0,
  @BeanProperty indexesAdded: Int = 0,
  @BeanProperty indexesRemoved: Int = 0,
  nodePropUniquenessConstraintsAdded: Int = 0,
  relPropUniquenessConstraintsAdded: Int = 0,
  nodePropExistenceConstraintsAdded: Int = 0,
  relPropExistenceConstraintsAdded: Int = 0,
  nodePropTypeConstraintsAdded: Int = 0,
  relPropTypeConstraintsAdded: Int = 0,
  nodekeyConstraintsAdded: Int = 0,
  relkeyConstraintsAdded: Int = 0,
  nodeLabelExistenceConstraintsAdded: Int = 0,
  relSourceLabelConstraintsAdded: Int = 0,
  relTargetLabelConstraintsAdded: Int = 0,
  @BeanProperty constraintsRemoved: Int = 0,
  @BeanProperty transactionsStarted: Long = 0,
  @BeanProperty transactionsCommitted: Long = 0,
  @BeanProperty transactionsRolledBack: Long = 0,
  @BeanProperty fileLinesRead: Long = 0,
  @BeanProperty systemUpdates: Int = 0
) extends org.neo4j.graphdb.QueryStatistics with ExtendedQueryStatistics {

  @BeanProperty
  val constraintsAdded: Int = nodePropUniquenessConstraintsAdded + relPropUniquenessConstraintsAdded +
    nodePropExistenceConstraintsAdded + relPropExistenceConstraintsAdded +
    nodekeyConstraintsAdded + relkeyConstraintsAdded +
    nodePropTypeConstraintsAdded + relPropTypeConstraintsAdded +
    nodeLabelExistenceConstraintsAdded +
    relSourceLabelConstraintsAdded + relTargetLabelConstraintsAdded

  override def containsUpdates: Boolean =
    nodesCreated > 0L ||
      relationshipsCreated > 0L ||
      propertiesSet > 0L ||
      nodesDeleted > 0L ||
      relationshipsDeleted > 0L ||
      labelsAdded > 0L ||
      labelsRemoved > 0L ||
      indexesAdded > 0 ||
      indexesRemoved > 0 ||
      constraintsAdded > 0 ||
      constraintsRemoved > 0

  override def containsSystemUpdates: Boolean = systemUpdates > 0

  override def getNodesCreated: Int = Math.min(nodesCreated, Integer.MAX_VALUE).toInt

  override def getNodesDeleted: Int = Math.min(nodesDeleted, Integer.MAX_VALUE).toInt

  override def getRelationshipsCreated: Int = Math.min(relationshipsCreated, Integer.MAX_VALUE).toInt

  override def getRelationshipsDeleted: Int = Math.min(relationshipsDeleted, Integer.MAX_VALUE).toInt

  override def getPropertiesSet: Int = Math.min(propertiesSet, Integer.MAX_VALUE).toInt

  override def getLabelsAdded: Int = Math.min(labelsAdded, Integer.MAX_VALUE).toInt

  override def getLabelsRemoved: Int = Math.min(labelsRemoved, Integer.MAX_VALUE).toInt

  override def toString: String = {
    val builder = new StringBuilder

    if (containsSystemUpdates) {
      includeIfNonZero(builder, "System updates: ", systemUpdates)
    } else {
      includeIfNonZero(builder, "Nodes created: ", nodesCreated)
      includeIfNonZero(builder, "Relationships created: ", relationshipsCreated)
      includeIfNonZero(builder, "Properties set: ", propertiesSet)
      includeIfNonZero(builder, "Nodes deleted: ", nodesDeleted)
      includeIfNonZero(builder, "Relationships deleted: ", relationshipsDeleted)
      includeIfNonZero(builder, "Labels added: ", labelsAdded)
      includeIfNonZero(builder, "Labels removed: ", labelsRemoved)
      includeIfNonZero(builder, "Indexes added: ", indexesAdded)
      includeIfNonZero(builder, "Indexes removed: ", indexesRemoved)
      includeIfNonZero(builder, "Node property uniqueness constraints added: ", nodePropUniquenessConstraintsAdded)
      includeIfNonZero(
        builder,
        "Relationship property uniqueness constraints added: ",
        relPropUniquenessConstraintsAdded
      )
      includeIfNonZero(builder, "Node property existence constraints added: ", nodePropExistenceConstraintsAdded)
      includeIfNonZero(builder, "Relationship property existence constraints added: ", relPropExistenceConstraintsAdded)
      includeIfNonZero(builder, "Node property type constraints added: ", nodePropTypeConstraintsAdded)
      includeIfNonZero(builder, "Relationship property type constraints added: ", relPropTypeConstraintsAdded)
      includeIfNonZero(builder, "Node key constraints added: ", nodekeyConstraintsAdded)
      includeIfNonZero(builder, "Relationship key constraints added: ", relkeyConstraintsAdded)
      includeIfNonZero(builder, "Node label existence constraints added: ", nodeLabelExistenceConstraintsAdded)
      includeIfNonZero(builder, "Relationship source label constraints added: ", relSourceLabelConstraintsAdded)
      includeIfNonZero(builder, "Relationship target label constraints added: ", relTargetLabelConstraintsAdded)
      includeIfNonZero(builder, "Constraints removed: ", constraintsRemoved)
      includeIfNonZero(builder, "Transactions started: ", transactionsStarted)
      includeIfNonZero(builder, "Transactions committed: ", transactionsCommitted)
      includeIfNonZero(builder, "Transactions rolled back: ", transactionsRolledBack)
      includeIfNonZero(builder, "File lines read: ", fileLinesRead)
    }
    val result = builder.toString()

    if (result.isEmpty) "<Nothing happened>" else result
  }

  private def includeIfNonZero(builder: StringBuilder, message: String, count: Long) = if (count > 0) {
    builder.append(message + count.toString + "\n")
  }

  def +(other: QueryStatistics): QueryStatistics = {
    QueryStatistics(
      nodesCreated = this.nodesCreated + other.nodesCreated,
      relationshipsCreated = this.relationshipsCreated + other.relationshipsCreated,
      propertiesSet = this.propertiesSet + other.propertiesSet,
      nodesDeleted = this.nodesDeleted + other.nodesDeleted,
      relationshipsDeleted = this.relationshipsDeleted + other.relationshipsDeleted,
      labelsAdded = this.labelsAdded + other.labelsAdded,
      labelsRemoved = this.labelsRemoved + other.labelsRemoved,
      indexesAdded = this.indexesAdded + other.indexesAdded,
      indexesRemoved = this.indexesRemoved + other.indexesRemoved,
      nodePropUniquenessConstraintsAdded =
        this.nodePropUniquenessConstraintsAdded + other.nodePropUniquenessConstraintsAdded,
      relPropUniquenessConstraintsAdded =
        this.relPropUniquenessConstraintsAdded + other.relPropUniquenessConstraintsAdded,
      nodePropExistenceConstraintsAdded =
        this.nodePropExistenceConstraintsAdded + other.nodePropExistenceConstraintsAdded,
      relPropExistenceConstraintsAdded = this.relPropExistenceConstraintsAdded + other.relPropExistenceConstraintsAdded,
      nodePropTypeConstraintsAdded = this.nodePropTypeConstraintsAdded + other.nodePropTypeConstraintsAdded,
      relPropTypeConstraintsAdded = this.relPropTypeConstraintsAdded + other.relPropTypeConstraintsAdded,
      nodekeyConstraintsAdded = this.nodekeyConstraintsAdded + other.nodekeyConstraintsAdded,
      relkeyConstraintsAdded = this.relkeyConstraintsAdded + other.relkeyConstraintsAdded,
      nodeLabelExistenceConstraintsAdded =
        this.nodeLabelExistenceConstraintsAdded + other.nodeLabelExistenceConstraintsAdded,
      relSourceLabelConstraintsAdded = this.relSourceLabelConstraintsAdded + other.relSourceLabelConstraintsAdded,
      relTargetLabelConstraintsAdded = this.relTargetLabelConstraintsAdded + other.relTargetLabelConstraintsAdded,
      constraintsRemoved = this.constraintsRemoved + other.constraintsRemoved,
      transactionsCommitted = this.transactionsCommitted + other.transactionsCommitted,
      transactionsStarted = this.transactionsStarted + other.transactionsStarted,
      transactionsRolledBack = this.transactionsRolledBack + other.transactionsRolledBack,
      fileLinesRead = this.fileLinesRead + other.fileLinesRead,
      systemUpdates = this.systemUpdates + other.systemUpdates
    )
  }

  def -(other: QueryStatistics): QueryStatistics = {
    QueryStatistics(
      nodesCreated = this.nodesCreated - other.nodesCreated,
      relationshipsCreated = this.relationshipsCreated - other.relationshipsCreated,
      propertiesSet = this.propertiesSet - other.propertiesSet,
      nodesDeleted = this.nodesDeleted - other.nodesDeleted,
      relationshipsDeleted = this.relationshipsDeleted - other.relationshipsDeleted,
      labelsAdded = this.labelsAdded - other.labelsAdded,
      labelsRemoved = this.labelsRemoved - other.labelsRemoved,
      indexesAdded = this.indexesAdded - other.indexesAdded,
      indexesRemoved = this.indexesRemoved - other.indexesRemoved,
      nodePropUniquenessConstraintsAdded =
        this.nodePropUniquenessConstraintsAdded - other.nodePropUniquenessConstraintsAdded,
      relPropUniquenessConstraintsAdded =
        this.relPropUniquenessConstraintsAdded - other.relPropUniquenessConstraintsAdded,
      nodePropExistenceConstraintsAdded =
        this.nodePropExistenceConstraintsAdded - other.nodePropExistenceConstraintsAdded,
      relPropExistenceConstraintsAdded = this.relPropExistenceConstraintsAdded - other.relPropExistenceConstraintsAdded,
      nodePropTypeConstraintsAdded = this.nodePropTypeConstraintsAdded - other.nodePropTypeConstraintsAdded,
      relPropTypeConstraintsAdded = this.relPropTypeConstraintsAdded - other.relPropTypeConstraintsAdded,
      nodekeyConstraintsAdded = this.nodekeyConstraintsAdded - other.nodekeyConstraintsAdded,
      relkeyConstraintsAdded = this.relkeyConstraintsAdded - other.relkeyConstraintsAdded,
      nodeLabelExistenceConstraintsAdded =
        this.nodeLabelExistenceConstraintsAdded - other.nodeLabelExistenceConstraintsAdded,
      relSourceLabelConstraintsAdded = this.relSourceLabelConstraintsAdded - other.relSourceLabelConstraintsAdded,
      relTargetLabelConstraintsAdded = this.relTargetLabelConstraintsAdded - other.relTargetLabelConstraintsAdded,
      constraintsRemoved = this.constraintsRemoved - other.constraintsRemoved,
      transactionsCommitted = this.transactionsCommitted - other.transactionsCommitted,
      transactionsStarted = this.transactionsStarted - other.transactionsStarted,
      transactionsRolledBack = this.transactionsRolledBack - other.transactionsRolledBack,
      fileLinesRead = this.fileLinesRead - other.fileLinesRead,
      systemUpdates = this.systemUpdates - other.systemUpdates
    )
  }
}

object QueryStatistics {
  val empty: QueryStatistics = QueryStatistics()

  def apply(statistics: org.neo4j.graphdb.QueryStatistics): QueryStatistics = statistics match {
    case q: QueryStatistics => q
    case q: org.neo4j.graphdb.QueryStatistics with ExtendedQueryStatistics =>
      QueryStatistics(
        nodesCreated = q.nodesCreated,
        nodesDeleted = q.nodesDeleted,
        relationshipsCreated = q.relationshipsCreated,
        relationshipsDeleted = q.relationshipsDeleted,
        propertiesSet = q.propertiesSet,
        labelsAdded = q.labelsAdded,
        labelsRemoved = q.labelsRemoved,
        indexesAdded = q.getIndexesAdded,
        indexesRemoved = q.getIndexesRemoved,
        nodePropUniquenessConstraintsAdded = q.getConstraintsAdded,
        constraintsRemoved = q.getConstraintsRemoved,
        systemUpdates = q.getSystemUpdates,
        transactionsCommitted = q.getTransactionsCommitted,
        transactionsStarted = q.getTransactionsStarted,
        transactionsRolledBack = q.getTransactionsRolledBack,
        fileLinesRead = q.getFileLinesRead
      )
    case null => empty
    case _    => withoutExtended(statistics)
  }

  def withoutExtended(statistics: org.neo4j.graphdb.QueryStatistics): QueryStatistics = QueryStatistics(
    nodesCreated = statistics.getNodesCreated,
    nodesDeleted = statistics.getNodesDeleted,
    relationshipsCreated = statistics.getRelationshipsCreated,
    relationshipsDeleted = statistics.getRelationshipsDeleted,
    propertiesSet = statistics.getPropertiesSet,
    labelsAdded = statistics.getLabelsAdded,
    labelsRemoved = statistics.getLabelsRemoved,
    indexesAdded = statistics.getIndexesAdded,
    indexesRemoved = statistics.getIndexesRemoved,
    nodePropUniquenessConstraintsAdded = statistics.getConstraintsAdded,
    constraintsRemoved = statistics.getConstraintsRemoved,
    systemUpdates = statistics.getSystemUpdates
  )
}
