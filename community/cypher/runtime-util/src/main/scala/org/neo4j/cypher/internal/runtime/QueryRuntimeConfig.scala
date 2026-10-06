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
import org.neo4j.cypher.internal.options.CypherTransactionBatchSchedulerOption
import org.neo4j.memory.HeapEstimatorCacheConfig

case class QueryRuntimeConfig(
  heapEstimatorCacheConfig: HeapEstimatorCacheConfig,
  morselReuseConfig: MorselReuseConfig,
  memoryTrackingConfig: MemoryTrackingConfig,
  transactionBatchSchedulerConfig: TransactionBatchSchedulerConfig,
  transactionBatchMaxBatchesInFormation: Int,
  transactionBatchParkingBudgetRows: Int
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
      },
      memoryTrackingConfig = MemoryTrackingConfig(
        topOperatorTrackingStrategyThreshold = config.pipelinedTopOperatorMemoryTrackingStrategyThreshold
      ),
      transactionBatchSchedulerConfig = queryOptions.transactionBatchScheduler match {
        case CypherTransactionBatchSchedulerOption.default =>
          TransactionBatchSchedulerConfig.DEFAULT
        case CypherTransactionBatchSchedulerOption.greedy =>
          TransactionBatchSchedulerConfig.Greedy
        case CypherTransactionBatchSchedulerOption.affinity =>
          TransactionBatchSchedulerConfig.Affinity
        case CypherTransactionBatchSchedulerOption.parking =>
          TransactionBatchSchedulerConfig.Parking
      },
      transactionBatchMaxBatchesInFormation = config.transactionsMaxBatchesInFormation,
      transactionBatchParkingBudgetRows = config.transactionsParkingBudgetRows
    )
  }

  final val DEFAULT: QueryRuntimeConfig = QueryRuntimeConfig(
    HeapEstimatorCacheConfig.DEFAULT,
    MorselReuseConfig.DEFAULT,
    MemoryTrackingConfig.DEFAULT,
    TransactionBatchSchedulerConfig.DEFAULT,
    transactionBatchMaxBatchesInFormation = 0,
    transactionBatchParkingBudgetRows = 0
  )
}

sealed trait MorselReuseConfig

object MorselReuseConfig {
  final val DEFAULT: MorselReuseConfig = MorselReuseConfig.FullReuse
  case object FullReuse extends MorselReuseConfig
  case object ReuseRemainingRowsOnly extends MorselReuseConfig
  case object NeverReuse extends MorselReuseConfig
}

/**
 * Selects the batch formation and scheduling policy used by deadlock prevention
 * in CALL { ... } IN CONCURRENT TRANSACTIONS.
 */
sealed trait TransactionBatchSchedulerConfig

object TransactionBatchSchedulerConfig {
  final val DEFAULT: TransactionBatchSchedulerConfig = TransactionBatchSchedulerConfig.Greedy
  case object Greedy extends TransactionBatchSchedulerConfig
  case object Affinity extends TransactionBatchSchedulerConfig
  case object Parking extends TransactionBatchSchedulerConfig
}

case class MemoryTrackingConfig(
  topOperatorTrackingStrategyThreshold: Long
)

object MemoryTrackingConfig {
  val DEFAULT: MemoryTrackingConfig = MemoryTrackingConfig(10000L)
}
