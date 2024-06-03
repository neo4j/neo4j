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

import org.neo4j.configuration.Config
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.config.MemoryTrackingController
import org.neo4j.cypher.internal.options.CypherExpressionEngineOption
import org.neo4j.util.Preconditions

import java.io.File

object CypherRuntimeConfiguration {

  def fromCypherConfiguration(config: CypherConfiguration): CypherRuntimeConfiguration = {
    CypherRuntimeConfiguration(
      pipelinedBatchSizeSmall = config.pipelinedBatchSizeSmall,
      pipelinedBatchSizeBig = config.pipelinedBatchSizeBig,
      operatorFusionOverPipelineLimit = config.operatorFusionOverPipelineLimit,
      schedulerTracing = SchedulerTracingConfiguration.fromCypherConfiguration(config),
      lenientCreateRelationship = config.lenientCreateRelationship,
      memoryTrackingController = config.memoryTrackingController,
      enableMonitors = config.enableMonitors,
      executionPlanCacheSize = config.executionPlanCacheSize,
      renderPlanDescription = config.renderPlanDescription,
      varExpandRelationshipIdSetThreshold = config.varExpandRelationshipIdSetThreshold,
      compiledExpressionMethodLimit = config.compiledExpressionMethodLimit,
      operatorFusingMethodLimit = config.operatorFusingMethodLimit,
      freeMemoryOfUnusedColumns = config.freeMemoryOfUnusedColumns,
      expressionEngineOption = config.expressionEngineOption
    )
  }

  def defaultConfiguration: CypherRuntimeConfiguration =
    fromCypherConfiguration(CypherConfiguration.fromConfig(Config.defaults()))
}

case class CypherRuntimeConfiguration(
  pipelinedBatchSizeSmall: Int,
  pipelinedBatchSizeBig: Int,
  operatorFusionOverPipelineLimit: Int,
  schedulerTracing: SchedulerTracingConfiguration,
  lenientCreateRelationship: Boolean,
  memoryTrackingController: MemoryTrackingController,
  enableMonitors: Boolean,
  executionPlanCacheSize: Int,
  renderPlanDescription: Boolean,
  varExpandRelationshipIdSetThreshold: Int,
  compiledExpressionMethodLimit: Int,
  operatorFusingMethodLimit: Int,
  freeMemoryOfUnusedColumns: Boolean,
  expressionEngineOption: CypherExpressionEngineOption
) {

  Preconditions.checkArgument(
    pipelinedBatchSizeSmall <= pipelinedBatchSizeBig,
    s"pipelinedBatchSizeSmall (got $pipelinedBatchSizeSmall) must be <= pipelinedBatchSizeBig (got $pipelinedBatchSizeBig)"
  )

}

object SchedulerTracingConfiguration {

  def fromCypherConfiguration(config: CypherConfiguration): SchedulerTracingConfiguration = {
    create(config.doSchedulerTracing, config.schedulerTracingFile)
  }

  def create(doSchedulerTracing: Boolean, schedulerTracingFile: File): SchedulerTracingConfiguration = {
    if (doSchedulerTracing) {
      if (schedulerTracingFile.getName == "stdOut") {
        StdOutSchedulerTracing
      } else {
        FileSchedulerTracing(schedulerTracingFile)
      }
    } else {
      NoSchedulerTracing
    }
  }
}

sealed trait SchedulerTracingConfiguration
case object NoSchedulerTracing extends SchedulerTracingConfiguration
case object StdOutSchedulerTracing extends SchedulerTracingConfiguration
case class FileSchedulerTracing(file: File) extends SchedulerTracingConfiguration
