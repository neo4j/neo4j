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
package org.neo4j.cypher.internal.options

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.options.CypherQueryOptions.ILLEGAL_EXPRESSION_ENGINE_RUNTIME_COMBINATIONS
import org.neo4j.cypher.internal.options.CypherQueryOptions.ILLEGAL_INTERPRETED_PIPES_FALLBACK_RUNTIME_COMBINATIONS
import org.neo4j.cypher.internal.options.CypherQueryOptions.ILLEGAL_OPERATOR_ENGINE_RUNTIME_COMBINATIONS
import org.neo4j.cypher.internal.options.CypherQueryOptions.ILLEGAL_PLANNER_RUNTIME_COMBINATIONS
import org.neo4j.cypher.internal.options.CypherQueryOptions.ILLEGAL_PLANNER_VERSION_COMBINATIONS
import org.neo4j.exceptions.InvalidArgumentException

/**
 * Collects all cypher options that can be set on query basis (pre-parser options)
 */
case class CypherQueryOptions(
  executionMode: CypherExecutionMode,
  version: CypherVersion,
  planner: CypherPlannerOption,
  runtime: CypherRuntimeOption,
  updateStrategy: CypherUpdateStrategy,
  expressionEngine: CypherExpressionEngineOption,
  operatorEngine: CypherOperatorEngineOption,
  interpretedPipesFallback: CypherInterpretedPipesFallbackOption,
  replan: CypherReplanOption,
  connectComponentsPlanner: CypherConnectComponentsPlannerOption,
  debugOptions: CypherDebugOptions
) {
  if (ILLEGAL_PLANNER_RUNTIME_COMBINATIONS((planner, runtime)))
    throw new InvalidCypherOption(s"Unsupported PLANNER - RUNTIME combination: ${planner.name} - ${runtime.name}")

  if (ILLEGAL_PLANNER_VERSION_COMBINATIONS((planner, version)))
    throw new InvalidCypherOption(s"Unsupported PLANNER - VERSION combination: ${planner.name} - ${version.name}")

  if (ILLEGAL_EXPRESSION_ENGINE_RUNTIME_COMBINATIONS((expressionEngine, runtime)))
    throw new InvalidCypherOption(s"Cannot combine EXPRESSION ENGINE '${expressionEngine.name}' with RUNTIME '${runtime.name}'")

  if (ILLEGAL_OPERATOR_ENGINE_RUNTIME_COMBINATIONS((operatorEngine, runtime)))
    throw new InvalidCypherOption(s"Cannot combine OPERATOR ENGINE '${operatorEngine.name}' with RUNTIME '${runtime.name}'")

  if (ILLEGAL_INTERPRETED_PIPES_FALLBACK_RUNTIME_COMBINATIONS((interpretedPipesFallback, runtime)))
    throw new InvalidCypherOption(s"Cannot combine INTERPRETED PIPES FALLBACK '${interpretedPipesFallback.name}' with RUNTIME '${runtime.name}'")
}

object CypherQueryOptions {

  def fromValues(config: CypherConfiguration, executionMode: Set[String], version: Set[String], keyValues: Set[(String, String)]): CypherQueryOptions = {

    val kvs = new KeyValueExtractor(keyValues)

    val result = CypherQueryOptions(
      executionMode = resolveSingle(CypherExecutionMode, executionMode, config),
      version = resolveSingle(CypherVersion, version, config),
      planner = kvs.resolveSingle(CypherPlannerOption, config),
      runtime = kvs.resolveSingle(CypherRuntimeOption, config),
      updateStrategy = kvs.resolveSingle(CypherUpdateStrategy, config),
      expressionEngine = kvs.resolveSingle(CypherExpressionEngineOption, config),
      operatorEngine = kvs.resolveSingle(CypherOperatorEngineOption, config),
      interpretedPipesFallback = kvs.resolveSingle(CypherInterpretedPipesFallbackOption, config),
      replan = kvs.resolveSingle(CypherReplanOption, config),
      connectComponentsPlanner = kvs.resolveSingle(CypherConnectComponentsPlannerOption, config),
      debugOptions = CypherDebugOptions(kvs.resolveAll(CypherDebugOption))
    )

    kvs.failOnUnknown()

    result
  }

  def fromConfiguration(config: CypherConfiguration): CypherQueryOptions =
    fromValues(config, Set.empty, Set.empty, Set.empty)

  private def resolveSingle[T <: CypherOption](option: CypherOptionCompanion[T], values: Set[String], config: CypherConfiguration): T =
    option.fromValues(values).headOption
          .getOrElse(option.fromCypherConfiguration(config))

  private class KeyValueExtractor(keyValues: Set[(String, String)]) {

    private var rest = keyValues

    def resolveSingle[T <: CypherOption](option: CypherKeyValueOptionCompanion[T], config: CypherConfiguration): T =
      resolveAll(option).headOption
                        .getOrElse(option.fromCypherConfiguration(config))

    def resolveAll[T <: CypherOption](option: CypherKeyValueOptionCompanion[T]): Set[T] = {
      val (matches, other) = rest.partition { case (k, _) => option.key == k }
      rest = other
      val values = matches.map { case (_, v) => v }
      option.fromValues(values)
    }

    def failOnUnknown(): Unit = rest.foreach { case (key, _) =>
      throw new InvalidCypherOption(s"Unsupported option: $key")
    }
  }

  private final val ILLEGAL_PLANNER_RUNTIME_COMBINATIONS: Set[(CypherPlannerOption, CypherRuntimeOption)] = Set.empty
  private final val ILLEGAL_PLANNER_VERSION_COMBINATIONS: Set[(CypherPlannerOption, CypherVersion)] = Set.empty
  private final val ILLEGAL_EXPRESSION_ENGINE_RUNTIME_COMBINATIONS: Set[(CypherExpressionEngineOption, CypherRuntimeOption)] =
    Set(
      (CypherExpressionEngineOption.compiled, CypherRuntimeOption.interpreted))
  private final val ILLEGAL_OPERATOR_ENGINE_RUNTIME_COMBINATIONS: Set[(CypherOperatorEngineOption, CypherRuntimeOption)] =
    Set(
      (CypherOperatorEngineOption.compiled, CypherRuntimeOption.slotted),
      (CypherOperatorEngineOption.compiled, CypherRuntimeOption.interpreted))
  private final val ILLEGAL_INTERPRETED_PIPES_FALLBACK_RUNTIME_COMBINATIONS: Set[(CypherInterpretedPipesFallbackOption, CypherRuntimeOption)] =
    Set(
      (CypherInterpretedPipesFallbackOption.disabled, CypherRuntimeOption.slotted),
      (CypherInterpretedPipesFallbackOption.disabled, CypherRuntimeOption.interpreted),
      (CypherInterpretedPipesFallbackOption.whitelistedPlansOnly, CypherRuntimeOption.slotted),
      (CypherInterpretedPipesFallbackOption.whitelistedPlansOnly, CypherRuntimeOption.interpreted),
      (CypherInterpretedPipesFallbackOption.allPossiblePlans, CypherRuntimeOption.slotted),
      (CypherInterpretedPipesFallbackOption.allPossiblePlans, CypherRuntimeOption.interpreted)
    )
}

sealed abstract class CypherExecutionMode(modeName: String) extends CypherOption(modeName) {
  override def companion: CypherExecutionMode.type = CypherExecutionMode
}

case object CypherExecutionMode extends CypherOptionCompanion[CypherExecutionMode](
  name = "execution mode",
  setting = None,
  cypherConfigField = None
) {

  case object default extends CypherExecutionMode("normal")
  case object profile extends CypherExecutionMode("profile")
  case object explain extends CypherExecutionMode("explain")

  val values: Set[CypherExecutionMode] = Set(profile, explain)
}

sealed abstract class CypherVersion(versionName: String) extends CypherOption(versionName) {
  override def companion: CypherVersion.type = CypherVersion
}

case object CypherVersion extends CypherOptionCompanion[CypherVersion](
  name = "version",
  setting = Some(GraphDatabaseSettings.cypher_parser_version),
  cypherConfigField = Some(_.version),
) {
  case object v3_5 extends CypherVersion("3.5")
  case object v4_2 extends CypherVersion("4.2")
  case object v4_3 extends CypherVersion("4.3")

  val default: CypherVersion = v4_3
  val values: Set[CypherVersion] = Set(v3_5, v4_2, v4_3)
}

sealed abstract class CypherPlannerOption(plannerName: String) extends CypherKeyValueOption(plannerName) {
  override def companion: CypherPlannerOption.type = CypherPlannerOption
}

case object CypherPlannerOption extends CypherKeyValueOptionCompanion[CypherPlannerOption](
  key = "planner",
  setting = Some(GraphDatabaseSettings.cypher_planner),
  cypherConfigField = Some(_.planner),
) {

  case object default extends CypherPlannerOption(CypherOption.DEFAULT)
  case object cost extends CypherPlannerOption("cost")
  case object idp extends CypherPlannerOption("idp")
  case object dp extends CypherPlannerOption("dp")

  val values: Set[CypherPlannerOption] = Set(cost, idp, dp)
}

sealed abstract class CypherRuntimeOption(runtimeName: String) extends CypherKeyValueOption(runtimeName) {
  override val companion: CypherRuntimeOption.type = CypherRuntimeOption
}

case object CypherRuntimeOption extends CypherKeyValueOptionCompanion[CypherRuntimeOption](
  key = "runtime",
  setting = Some(GraphDatabaseInternalSettings.cypher_runtime),
  cypherConfigField = Some(_.runtime),
) {

  case object default extends CypherRuntimeOption(CypherOption.DEFAULT)
  case object interpreted extends CypherRuntimeOption("interpreted")
  case object slotted extends CypherRuntimeOption("slotted")
  case object pipelined extends CypherRuntimeOption("pipelined")
  case object parallel extends CypherRuntimeOption("parallel")

  val values: Set[CypherRuntimeOption] = Set(interpreted, slotted, pipelined, parallel)
}

sealed abstract class CypherUpdateStrategy(strategy: String) extends CypherKeyValueOption(strategy) {
  override def companion: CypherUpdateStrategy.type = CypherUpdateStrategy
}

case object CypherUpdateStrategy extends CypherKeyValueOptionCompanion[CypherUpdateStrategy](
  key = "updateStrategy",
  setting = None,
  cypherConfigField = None,
) {

  case object default extends CypherUpdateStrategy(CypherOption.DEFAULT)
  case object eager extends CypherUpdateStrategy("eager")

  val values: Set[CypherUpdateStrategy] = Set(default, eager)
}

sealed abstract class CypherExpressionEngineOption(engineName: String) extends CypherKeyValueOption(engineName) {
  override def companion: CypherExpressionEngineOption.type = CypherExpressionEngineOption
}

case object CypherExpressionEngineOption extends CypherKeyValueOptionCompanion[CypherExpressionEngineOption](
  key = "expressionEngine",
  setting = Some(GraphDatabaseInternalSettings.cypher_expression_engine),
  cypherConfigField = Some(_.expressionEngineOption)
) {

  case object default extends CypherExpressionEngineOption(CypherOption.DEFAULT)
  case object interpreted extends CypherExpressionEngineOption("interpreted")
  case object compiled extends CypherExpressionEngineOption("compiled")
  case object onlyWhenHot extends CypherExpressionEngineOption("only_when_hot")

  val values: Set[CypherExpressionEngineOption] = Set(interpreted, compiled, onlyWhenHot)
}

sealed abstract class CypherOperatorEngineOption(mode: String) extends CypherKeyValueOption(mode) {
  override def companion: CypherOperatorEngineOption.type = CypherOperatorEngineOption
}

case object CypherOperatorEngineOption extends CypherKeyValueOptionCompanion[CypherOperatorEngineOption](
  key = "operatorEngine",
  setting = Some(GraphDatabaseInternalSettings.cypher_operator_engine),
  cypherConfigField = Some(_.operatorEngine),
) {
  case object default extends CypherOperatorEngineOption(CypherOption.DEFAULT)
  case object compiled extends CypherOperatorEngineOption("compiled")
  case object interpreted extends CypherOperatorEngineOption("interpreted")

  val values: Set[CypherOperatorEngineOption] = Set(compiled, interpreted)
}

sealed abstract class CypherInterpretedPipesFallbackOption(mode: String) extends CypherKeyValueOption(mode) {
  override def companion: CypherInterpretedPipesFallbackOption.type = CypherInterpretedPipesFallbackOption
}

case object CypherInterpretedPipesFallbackOption extends CypherKeyValueOptionCompanion[CypherInterpretedPipesFallbackOption](
  key = "interpretedPipesFallback",
  setting = Some(GraphDatabaseInternalSettings.cypher_pipelined_interpreted_pipes_fallback),
  cypherConfigField = Some(_.interpretedPipesFallback),
) {

  case object default extends CypherInterpretedPipesFallbackOption(CypherOption.DEFAULT)
  case object disabled extends CypherInterpretedPipesFallbackOption("disabled")
  case object whitelistedPlansOnly extends CypherInterpretedPipesFallbackOption("whitelisted_plans_only")
  case object allPossiblePlans extends CypherInterpretedPipesFallbackOption("all")

  val values: Set[CypherInterpretedPipesFallbackOption] = Set(disabled, whitelistedPlansOnly, allPossiblePlans)
}

sealed abstract class CypherReplanOption(strategy: String) extends CypherKeyValueOption(strategy) {
  override def companion: CypherReplanOption.type = CypherReplanOption
}

case object CypherReplanOption extends CypherKeyValueOptionCompanion[CypherReplanOption](
  key = "replan",
  setting = None,
  cypherConfigField = None,
) {

  case object default extends CypherReplanOption(CypherOption.DEFAULT)
  case object force extends CypherReplanOption("force")
  case object skip extends CypherReplanOption("skip")

  val values: Set[CypherReplanOption] = Set(force, skip)
}

sealed abstract class CypherConnectComponentsPlannerOption(planner: String) extends CypherKeyValueOption(planner) {
  override def companion: CypherConnectComponentsPlannerOption.type = CypherConnectComponentsPlannerOption
}

case object CypherConnectComponentsPlannerOption extends CypherKeyValueOptionCompanion[CypherConnectComponentsPlannerOption](
  key = "connectComponentsPlanner",
  setting = None,
  cypherConfigField = None,
) {

  case object default extends CypherConnectComponentsPlannerOption(CypherOption.DEFAULT)
  case object greedy extends CypherConnectComponentsPlannerOption("greedy")
  case object idp extends CypherConnectComponentsPlannerOption("idp")

  val values: Set[CypherConnectComponentsPlannerOption] = Set(greedy, idp)
}

sealed abstract class CypherDebugOption(flag: String) extends CypherKeyValueOption(flag) {
  override def companion: CypherDebugOption.type = CypherDebugOption
}

case object CypherDebugOption extends CypherKeyValueOptionCompanion[CypherDebugOption](
  key = "debug",
  setting = None,
  cypherConfigField = None,
) {
  // Unused. We need to have a default
  case object default extends CypherDebugOption("none")
  case object tostring extends CypherDebugOption("tostring")
  case object reportCostComparisonsAsRows extends CypherDebugOption("reportcostcomparisonsasrows")
  case object printCostComparisons extends CypherDebugOption("printcostcomparisons")
  case object generateJavaSource extends CypherDebugOption("generate_java_source")
  case object showJavaSource extends CypherDebugOption("show_java_source")
  case object showBytecode extends CypherDebugOption("show_bytecode")
  case object visualizePipelines extends CypherDebugOption("visualizepipelines")
  case object inverseCost extends CypherDebugOption("inverse_cost")
  case object queryGraph extends CypherDebugOption("querygraph")
  case object ast extends CypherDebugOption("ast")
  case object semanticState extends CypherDebugOption("semanticstate")
  case object logicalPlan extends CypherDebugOption("logicalplan")
  case object fabricLogPlan extends CypherDebugOption("fabriclogplan")
  case object fabricLogRecords extends CypherDebugOption("fabriclogrecords")

  override def values: Set[CypherDebugOption] = Set(
    tostring,
    reportCostComparisonsAsRows,
    printCostComparisons,
    generateJavaSource,
    showJavaSource,
    showBytecode,
    visualizePipelines,
    inverseCost,
    queryGraph,
    ast,
    semanticState,
    logicalPlan,
    fabricLogPlan,
    fabricLogRecords
  )

  override def fromValues(values: Set[String]): Set[CypherDebugOption] =
    values.map(fromValue)
}

object CypherDebugOptions {
  def default: CypherDebugOptions = CypherDebugOptions(Set.empty)
}

case class CypherDebugOptions(enabledOptions: Set[CypherDebugOption]) {

  def enabledOptionsSeq: Seq[CypherDebugOption] = enabledOptions.toSeq.sortBy(_.name)

  def isEmpty: Boolean = enabledOptions.isEmpty

  private def isEnabled(option: CypherDebugOption): Boolean = enabledOptions.contains(option)

  val toStringEnabled: Boolean = isEnabled(CypherDebugOption.tostring)
  val reportCostComparisonsAsRowsEnabled: Boolean = isEnabled(CypherDebugOption.reportCostComparisonsAsRows)
  val printCostComparisonsEnabled: Boolean = isEnabled(CypherDebugOption.printCostComparisons)
  val generateJavaSourceEnabled: Boolean = isEnabled(CypherDebugOption.generateJavaSource)
  val showJavaSourceEnabled: Boolean = isEnabled(CypherDebugOption.showJavaSource)
  val showBytecodeEnabled: Boolean = isEnabled(CypherDebugOption.showBytecode)
  val visualizePipelinesEnabled: Boolean = isEnabled(CypherDebugOption.visualizePipelines)
  val inverseCostEnabled: Boolean = isEnabled(CypherDebugOption.inverseCost)
  val queryGraphEnabled: Boolean = isEnabled(CypherDebugOption.queryGraph)
  val astEnabled: Boolean = isEnabled(CypherDebugOption.ast)
  val semanticStateEnabled: Boolean = isEnabled(CypherDebugOption.semanticState)
  val logicalPlanEnabled: Boolean = isEnabled(CypherDebugOption.logicalPlan)
  val fabricLogPlanEnabled: Boolean = isEnabled(CypherDebugOption.fabricLogPlan)
  val fabricLogRecordsEnabled: Boolean = isEnabled(CypherDebugOption.fabricLogRecords)
}
