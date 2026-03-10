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

import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.options.CypherDerivedQueryOptions
import org.neo4j.cypher.internal.options.CypherPipelinedBatchReuseOption
import org.neo4j.cypher.internal.options.CypherQueryOptions
import org.neo4j.memory.HeapEstimatorCacheConfig

case class QueryRuntimeConfig(
  heapEstimatorCacheConfig: HeapEstimatorCacheConfig,
  morselReuseConfig: MorselReuseConfig = MorselReuseConfig.DEFAULT
) {

  def withHeapEstimatorCacheConfig(heapEstimatorCacheConfig: HeapEstimatorCacheConfig): QueryRuntimeConfig = {
    copy(heapEstimatorCacheConfig = heapEstimatorCacheConfig)
  }

  def withMorselReuseConfig(morselReuseConfig: MorselReuseConfig): QueryRuntimeConfig = {
    copy(morselReuseConfig = morselReuseConfig)
  }
}

object QueryRuntimeConfig {

  def createFrom(
    queryOptions: CypherQueryOptions,
    derivedOptions: CypherDerivedQueryOptions,
    config: CypherConfiguration
  ): QueryRuntimeConfig = {
    QueryRuntimeConfig(
      derivedOptions.heapEstimatorCacheConfig,
      morselReuseConfig = queryOptions.pipelinedBatchReuseOption match {
        case CypherPipelinedBatchReuseOption.default =>
          MorselReuseConfig.DEFAULT
        case CypherPipelinedBatchReuseOption.full =>
          MorselReuseConfig.FullReuse
        case CypherPipelinedBatchReuseOption.pack =>
          MorselReuseConfig.ReuseRemainingRowsOnly
        case CypherPipelinedBatchReuseOption.disabled =>
          MorselReuseConfig.NeverReuse
      }
    )
  }

  final val DEFAULT: QueryRuntimeConfig = QueryRuntimeConfig(
    HeapEstimatorCacheConfig.DEFAULT
  )
}

sealed trait MorselReuseConfig

object MorselReuseConfig {
  final val DEFAULT: MorselReuseConfig = MorselReuseConfig.FullReuse
  case object FullReuse extends MorselReuseConfig
  case object ReuseRemainingRowsOnly extends MorselReuseConfig
  case object NeverReuse extends MorselReuseConfig
}
