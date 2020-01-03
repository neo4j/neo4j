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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.plandescription.InternalPlanDescription
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.graphdb.QueryExecutionType.QueryType
import org.neo4j.kernel.api.query.CompilerInfo
import org.neo4j.kernel.impl.query.{QueryExecution, QuerySubscriber, TransactionalContext}
import org.neo4j.values.virtual.MapValue

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
    * @param subscriber                     The subscriber where results should be streamed to.
    * @return the QueryExecution that controls the demand to the subscriber
    */
  def execute(transactionalContext: TransactionalContext,
              isOutermostQuery: Boolean,
              queryOptions: QueryOptions,
              params: MapValue,
              prePopulateResults: Boolean,
              input: InputDataStream,
              subscriber: QuerySubscriber): QueryExecution

  /**
    * The reusability state of this executable query.
    */
  def reusabilityState(lastCommittedTxId: () => Long, ctx: TransactionalContext): ReusabilityState

  /**
    * Plan desc.
    */
  def planDescription(): InternalPlanDescription

  /**
    * Meta-data about the compiled used for this query.
    */
  val compilerInfo: CompilerInfo // val to force eager calculation

  /**
    * Names of all parameters for this query, explicit and auto-parametrized.
    */
  val paramNames: Array[String]

  /**
    * The names and values of the auto-parametrized parameters for this query.
    */
  val extractedParams: MapValue

  /**
    * Type of this query.
    */
  def queryType: QueryType
}
