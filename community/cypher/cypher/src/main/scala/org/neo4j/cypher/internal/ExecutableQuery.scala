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
package org.neo4j.cypher.internal

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.graphdb.ExecutionPlanDescription
import org.neo4j.kernel.api.query.CompilerInfo
import org.neo4j.kernel.api.query.QueryObfuscator
import org.neo4j.kernel.api.query.RelationshipTypeIndexUsage
import org.neo4j.kernel.api.query.SchemaIndexUsage
import org.neo4j.kernel.impl.query.QueryExecution
import org.neo4j.kernel.impl.query.QueryExecutionMonitor
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.values.virtual.MapValue

import java.util.function.Supplier

import scala.jdk.CollectionConverters.ListHasAsScala

/**
 * A fully compiled query in executable form.
 */
trait ExecutableQuery extends CacheabilityInfo {

  /**
   * Execute this executable query.
   *
   * @param transactionalContext           the transaction in which to execute
   * @param isOutermostQuery               provide `true` if this is the outer-most query and should close the transaction when finished or error
   * @param queryOptions                   execution options
   * @param params                         the parameters
   * @param prePopulateResults             if false, nodes and relationships might be returned as references in the results
   * @param input                          stream of existing records as input
   * @param queryMonitor                   monitor to submit query events to
   * @param subscriber                     The subscriber where results should be streamed to.
   * @return the QueryExecution that controls the demand to the subscriber
   */
  def execute(
    transactionalContext: TransactionalContext,
    isOutermostQuery: Boolean,
    queryOptions: QueryOptions,
    params: MapValue,
    prePopulateResults: Boolean,
    input: InputDataStream,
    queryMonitor: QueryExecutionMonitor,
    subscriber: QuerySubscriber
  ): QueryExecution

  /**
   * The reusability state of this executable query.
   */
  def reusabilityState(lastCommittedTxId: () => Long, ctx: TransactionalContext): ReusabilityState

  /**
   * Plan desc.
   */
  def planDescriptionSupplier(): Supplier[ExecutionPlanDescription]

  /**
   * Meta-data about the compiled used for this query.
   */
  val compilerInfo: CompilerInfo // val to force eager calculation

  /**
   * Returns label ids paired with the properties of the indexes used by this executable query, excluding lookup indexes.
   * Precomputed to reduce execution latency for very fast queries.
   */
  val labelIdsOfUsedIndexes: Map[Long, Array[Int]] = compilerInfo.indexes().asScala
    .collect { case item: SchemaIndexUsage => item.getLabelId.toLong -> item.getPropertyKeys }
    .toMap

  /**
   * Returns the relationship type id paired with the property keys of the indexes used by this executable query, excluding lookup indexes.
   * Precomputed to reduce execution latency for very fast queries.
   */
  val relationshipsOfUsedIndexes: Map[Long, Array[Int]] = compilerInfo.relationshipTypeIndexes().asScala
    .collect { case item: RelationshipTypeIndexUsage => (item.getRelationshipTypeId.toLong -> item.getPropertyKeyIds) }
    .toMap

  /**
   * Lookup entity types used by this executable query.
   */
  val lookupEntityTypes: Array[EntityType] = compilerInfo.lookupIndexes().asScala.map(_.getEntityType).toArray

  /**
   * Names of all parameters for this query, explicit and auto-parametrized.
   */
  val paramNames: Array[String]

  /**
   * The names and values of the auto-parametrized parameters for this query.
   */
  val extractedParams: MapValue

  /**
   * Obfuscator to be used on this query's raw text and parameters before logging.
   */
  def queryObfuscator: QueryObfuscator
}
