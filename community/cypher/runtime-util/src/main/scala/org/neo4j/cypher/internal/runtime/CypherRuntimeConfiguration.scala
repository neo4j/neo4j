package org.neo4j.cypher.internal.runtime

import org.neo4j.configuration.Config
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.config.MemoryTrackingController
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
      varExpandRelationshipIdSetThreshold = config.varExpandRelationshipIdSetThreshold
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
  varExpandRelationshipIdSetThreshold: Int
) {

  Preconditions.checkArgument(
    pipelinedBatchSizeSmall <= pipelinedBatchSizeBig,
    s"pipelinedBatchSizeSmall (got $pipelinedBatchSizeSmall) must be <= pipelinedBatchSizeBig (got $pipelinedBatchSizeBig)"
  )

}

object SchedulerTracingConfiguration {

  def fromCypherConfiguration(config: CypherConfiguration): SchedulerTracingConfiguration =
    if (config.doSchedulerTracing)
      if (config.schedulerTracingFile.getName == "stdOut") StdOutSchedulerTracing
      else FileSchedulerTracing(config.schedulerTracingFile)
    else NoSchedulerTracing
}

sealed trait SchedulerTracingConfiguration
case object NoSchedulerTracing extends SchedulerTracingConfiguration
case object StdOutSchedulerTracing extends SchedulerTracingConfiguration
case class FileSchedulerTracing(file: File) extends SchedulerTracingConfiguration
