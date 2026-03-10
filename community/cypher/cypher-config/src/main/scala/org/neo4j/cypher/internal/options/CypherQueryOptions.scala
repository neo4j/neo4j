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
package org.neo4j.cypher.internal.options

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseInternalSettings.CypherPipelinedBatchSizePreset
import org.neo4j.configuration.GraphDatabaseInternalSettings.HeapEstimatorCachePreset
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.options.CypherPlanMode.default
import org.neo4j.cypher.internal.options.CypherQueryOptions.ILLEGAL_EXPRESSION_ENGINE_RUNTIME_COMBINATIONS
import org.neo4j.cypher.internal.options.CypherQueryOptions.ILLEGAL_INTERPRETED_PIPES_FALLBACK_RUNTIME_COMBINATIONS
import org.neo4j.cypher.internal.options.CypherQueryOptions.ILLEGAL_OPERATOR_ENGINE_RUNTIME_COMBINATIONS
import org.neo4j.cypher.internal.options.CypherQueryOptions.ILLEGAL_PARALLEL_RUNTIME_COMBINATIONS
import org.neo4j.exceptions.InvalidCypherOption
import org.neo4j.memory.HeapEstimatorCacheConfig

import java.util.Locale

/**
 * Collects all cypher options that can be set on query basis (pre-parser options)
 */
case class CypherQueryOptions(
  cypherVersion: CypherVersionOption, // This is NOT the resolved query language, only the pre-parser option.
  executionMode: CypherExecutionMode,
  planMode: CypherPlanMode,
  planner: CypherPlannerOption,
  runtime: CypherRuntimeOption,
  updateStrategy: CypherUpdateStrategy,
  expressionEngine: CypherExpressionEngineOption,
  operatorEngine: CypherOperatorEngineOption,
  interpretedPipesFallback: CypherInterpretedPipesFallbackOption,
  replan: CypherReplanOption,
  cache: CypherCacheOption,
  connectComponentsPlanner: CypherConnectComponentsPlannerOption,
  debugOptions: CypherDebugOptions,
  parallelRuntimeSupportOption: CypherParallelRuntimeSupportOption,
  parallelRuntimeConfigOption: CypherParallelRuntimeConfigOption,
  eagerAnalyzer: CypherEagerAnalyzerOption,
  inferSchemaParts: CypherInferSchemaPartsOption,
  statefulShortestPlanningModeOption: CypherStatefulShortestPlanningModeOption,
  planVarExpandInto: CypherPlanVarExpandInto,
  pipelinedBatchSizePresetOption: CypherPipelinedBatchSizePresetOption,
  pipelinedBatchReuseOption: CypherPipelinedBatchReuseOption,
  heapEstimatorCacheOption: CypherHeapEstimatorCacheOption
) {

  if (ILLEGAL_EXPRESSION_ENGINE_RUNTIME_COMBINATIONS((expressionEngine, runtime)))
    throw InvalidCypherOption.invalidCombination("EXPRESSION ENGINE", expressionEngine.name, "RUNTIME", runtime.name)

  if (ILLEGAL_OPERATOR_ENGINE_RUNTIME_COMBINATIONS((operatorEngine, runtime)))
    throw InvalidCypherOption.invalidCombination("OPERATOR ENGINE", operatorEngine.name, "RUNTIME", runtime.name)

  if (ILLEGAL_INTERPRETED_PIPES_FALLBACK_RUNTIME_COMBINATIONS((interpretedPipesFallback, runtime)))
    throw InvalidCypherOption.invalidCombination(
      "INTERPRETED PIPES FALLBACK",
      interpretedPipesFallback.name,
      "RUNTIME",
      runtime.name
    )

  if (ILLEGAL_PARALLEL_RUNTIME_COMBINATIONS((parallelRuntimeSupportOption, runtime))) {
    throw InvalidCypherOption.parallelRuntimeIsDisabled()
  }

  def renderCypherOptions: String = {
    // For Cypher query rendering purposes, execution & plan mode and Cypher options are two separate things.
    // Default execution mode renders to nothing, so let's use it as a part of a trick for rendering
    // just Cypher options with execution mode.
    CypherQueryOptions.renderer.render(this.copy(executionMode = CypherExecutionMode.default, planMode = default))
  }

  def renderExecutionMode: String = executionMode.render

  def renderPlanMode: String = planMode.render

  /**
   * Cache key used for executableQueryCache, astCache, and exeuctionPlanCache.
   */
  def cacheKey: String = CypherQueryOptions.cacheKey.cacheKey(this)

  /**
   * Cache key used for logicalPlanCache.
   */
  def logicalPlanCacheKey: String = CypherQueryOptions.logicalPlanCacheKey.logicalPlanCacheKey(this)
}

case class CypherDerivedQueryOptions(
  pipelinedBatchSize: CypherPipelinedBatchSize,
  heapEstimatorCacheConfig: HeapEstimatorCacheConfig
) {

  /**
   * Cache key used for executableQueryCache, astCache, and exeuctionPlanCache.
   */
  def cacheKey: String = {
    pipelinedBatchSize.cacheKey
  }

  /**
   * Cache key used for logicalPlanCache.
   */
  def logicalPlanCacheKey: String = {
    pipelinedBatchSize.cacheKey
  }
}

object CypherQueryOptions {

  private val hasDefault = OptionDefault.derive[CypherQueryOptions]
  private val renderer = OptionRenderer.derive[CypherQueryOptions]
  private val cacheKey = OptionCacheKey.derive[CypherQueryOptions]
  private val logicalPlanCacheKey = OptionLogicalPlanCacheKey.derive[CypherQueryOptions]
  private val reader = OptionReader.derive[CypherQueryOptions]

  val defaultOptions: CypherQueryOptions = hasDefault.default

  def fromValues(config: CypherConfiguration, keyValues: Set[(String, String)]): CypherQueryOptions = {
    reader.read(OptionReader.Input(config, keyValues)) match {
      case OptionReader.Result(remainder, _) if remainder.keyValues.nonEmpty =>
        throw InvalidCypherOption.unsupportedOptions(remainder.keyValues.map(_._1).toArray: _*)
      case OptionReader.Result(_, options) =>
        if (options.planMode.isScope && !config.enableScopeQueries) {
          throw InvalidCypherOption.invalidOption(
            CypherPlanMode.scope.modeName,
            CypherPlanMode.name,
            CypherPlanMode.plan.modeName
          )
        }
        if (options.debugOptions.generateJavaSourceEnabled && !config.allowSourceGeneration) {
          throw InvalidCypherOption.sourceGenerationDisabled(this.getClass.getSimpleName)
        }
        if (options.cypherVersion.explicitVersion.exists(_.experimental) && !config.enableExperimentalCypherVersions) {
          throw InvalidCypherOption.invalidOption(
            options.cypherVersion.name,
            CypherVersionOption.name,
            CypherVersionOption.supportedValues.map(_.name): _*
          )
        }
        options
    }
  }

  def derivedOptions(queryOptions: CypherQueryOptions, config: CypherConfiguration): CypherDerivedQueryOptions = {
    val batchSize = CypherPipelinedBatchSizePresetOption.batchSizeConfigFrom(
      queryOptions.pipelinedBatchSizePresetOption,
      config.pipelinedBatchSizeSmall,
      config.pipelinedBatchSizeBig
    )
    val heapEstimatorCacheConfig = CypherHeapEstimatorCacheOption.heapEstimatorCacheConfigFrom(
      queryOptions.heapEstimatorCacheOption,
      config
    )
    CypherDerivedQueryOptions(batchSize, heapEstimatorCacheConfig)
  }

  // Test-only
  def defaultDerivedOptions: CypherDerivedQueryOptions = {
    derivedOptions(defaultOptions, CypherConfiguration.fromConfig(Config.defaults()))
  }

  final private def ILLEGAL_EXPRESSION_ENGINE_RUNTIME_COMBINATIONS
    : Set[(CypherExpressionEngineOption, CypherRuntimeOption)] =
    Set(
      (CypherExpressionEngineOption.compiled, CypherRuntimeOption.legacy),
      (CypherExpressionEngineOption.compiled, CypherRuntimeOption.interpreted)
    )

  final private def ILLEGAL_OPERATOR_ENGINE_RUNTIME_COMBINATIONS
    : Set[(CypherOperatorEngineOption, CypherRuntimeOption)] =
    Set(
      (CypherOperatorEngineOption.compiled, CypherRuntimeOption.slotted),
      (CypherOperatorEngineOption.compiled, CypherRuntimeOption.interpreted),
      (CypherOperatorEngineOption.compiled, CypherRuntimeOption.legacy)
    )

  final private def ILLEGAL_INTERPRETED_PIPES_FALLBACK_RUNTIME_COMBINATIONS
    : Set[(CypherInterpretedPipesFallbackOption, CypherRuntimeOption)] =
    Set(
      (CypherInterpretedPipesFallbackOption.disabled, CypherRuntimeOption.slotted),
      (CypherInterpretedPipesFallbackOption.disabled, CypherRuntimeOption.interpreted),
      (CypherInterpretedPipesFallbackOption.disabled, CypherRuntimeOption.legacy),
      (CypherInterpretedPipesFallbackOption.whitelistedPlansOnly, CypherRuntimeOption.slotted),
      (CypherInterpretedPipesFallbackOption.whitelistedPlansOnly, CypherRuntimeOption.interpreted),
      (CypherInterpretedPipesFallbackOption.whitelistedPlansOnly, CypherRuntimeOption.legacy),
      (CypherInterpretedPipesFallbackOption.allPossiblePlans, CypherRuntimeOption.slotted),
      (CypherInterpretedPipesFallbackOption.allPossiblePlans, CypherRuntimeOption.interpreted),
      (CypherInterpretedPipesFallbackOption.allPossiblePlans, CypherRuntimeOption.legacy)
    )

  final private def ILLEGAL_PARALLEL_RUNTIME_COMBINATIONS
    : Set[(CypherParallelRuntimeSupportOption, CypherRuntimeOption)] =
    Set(
      (CypherParallelRuntimeSupportOption.disabled, CypherRuntimeOption.parallel)
    )
}

sealed abstract class CypherExecutionMode(val modeName: String) extends CypherOption(modeName) {
  override def companion: CypherExecutionMode.type = CypherExecutionMode
  override def render: String = super.render.toUpperCase(Locale.ROOT)
  override def cacheKey: String = super.cacheKey.toUpperCase(Locale.ROOT)
  def isExplain: Boolean = this == CypherExecutionMode.explain
  def isProfile: Boolean = this == CypherExecutionMode.profile

  /** Does not affect the plan we produce. */
  override def relevantForLogicalPlanCacheKey: Boolean = false
}

case object CypherExecutionMode extends CypherOptionCompanion[CypherExecutionMode](
      name = "execution mode"
    ) {

  case object default extends CypherExecutionMode("normal")

  case object profile extends CypherExecutionMode("PROFILE") {
    override def render: String = modeName
  }

  case object explain extends CypherExecutionMode("EXPLAIN") {
    override def render: String = modeName
    override def cacheKey: String = ""
  }

  def values: Set[CypherExecutionMode] = Set(profile, explain)

  implicit val hasDefault: OptionDefault[CypherExecutionMode] = OptionDefault.create(default)
  implicit val renderer: OptionRenderer[CypherExecutionMode] = OptionRenderer.create(_.render)
  implicit val cacheKey: OptionCacheKey[CypherExecutionMode] = OptionCacheKey.create(_.cacheKey)

  implicit val logicalPlanCacheKey: OptionLogicalPlanCacheKey[CypherExecutionMode] =
    OptionLogicalPlanCacheKey.create(_.logicalPlanCacheKey)
  implicit val reader: OptionReader[CypherExecutionMode] = singleOptionReader()
}

sealed abstract class CypherPlanMode(val modeName: String) extends CypherOption(modeName) {
  override def companion: CypherPlanMode.type = CypherPlanMode
  override def render: String = super.render.toUpperCase(Locale.ROOT)
  override def cacheKey: String = super.cacheKey.toUpperCase(Locale.ROOT)
  def isScope: Boolean = this == CypherPlanMode.scope

  /** Does not affect the plan we produce. */
  override def relevantForLogicalPlanCacheKey: Boolean = false
}

case object CypherPlanMode extends CypherOptionCompanion[CypherPlanMode](
      name = "plan mode"
    ) {
  case object default extends CypherPlanMode("")

  case object plan extends CypherPlanMode("PLAN") {
    override def render: String = modeName
    override def cacheKey: String = ""
  }

  case object scope extends CypherPlanMode("SCOPE") {
    override def render: String = modeName
    override def cacheKey: String = ""
  }

  def values: Set[CypherPlanMode] = Set(scope, plan)

  implicit val hasDefault: OptionDefault[CypherPlanMode] = OptionDefault.create(default)
  implicit val renderer: OptionRenderer[CypherPlanMode] = OptionRenderer.create(_.render)
  implicit val cacheKey: OptionCacheKey[CypherPlanMode] = OptionCacheKey.create(_.cacheKey)

  implicit val logicalPlanCacheKey: OptionLogicalPlanCacheKey[CypherPlanMode] =
    OptionLogicalPlanCacheKey.create(_.logicalPlanCacheKey)
  implicit val reader: OptionReader[CypherPlanMode] = singleOptionReader()
}

/**
 * The pre-parser option to select query language, NOT the actual language to use.
 * The final resolved language for a query depend on:
 * - The pre-parser option (this class).
 * - The database default language (persisted in system db).
 * - The system default language setting, `db.query.default_language`.
 */
sealed abstract class CypherVersionOption(val version: String) extends CypherOption(version) {
  override def companion: CypherExecutionMode.type = CypherExecutionMode
  override def render: String = super.render.toUpperCase(Locale.ROOT)
  override def cacheKey: String = "" // We include the resolved version in the cache key
  override def relevantForLogicalPlanCacheKey: Boolean = false // We include the resolved version in the cache key
  def fromPreParserOption: Boolean
  def explicitVersion: Option[CypherVersion]
}

case object CypherVersionOption extends CypherOptionCompanion[CypherVersionOption](name = "cypher version") {

  /** No cypher version specified in pre-parser options => we should use the db default version. */
  case object default extends CypherVersionOption("") {
    override def fromPreParserOption: Boolean = false
    override def explicitVersion: Option[CypherVersion] = None
  }

  case object cypher5 extends CypherVersionOption("5") {
    override def fromPreParserOption: Boolean = true
    override def explicitVersion: Option[CypherVersion] = Some(CypherVersion.Cypher5)
  }

  case object cypher25 extends CypherVersionOption("25") {
    override def fromPreParserOption: Boolean = true
    override def explicitVersion: Option[CypherVersion] = Some(CypherVersion.Cypher25)
  }

  override def values: Set[CypherVersionOption] = Set(cypher5, cypher25)

  override def supportedValues: Seq[CypherVersionOption] =
    super.supportedValues.filter(_.explicitVersion.forall(v => !v.experimental))

  implicit val hasDefault: OptionDefault[CypherVersionOption] = OptionDefault.create(default)
  implicit val renderer: OptionRenderer[CypherVersionOption] = OptionRenderer.create(_.render)
  implicit val cacheKey: OptionCacheKey[CypherVersionOption] = OptionCacheKey.create(_.cacheKey)

  implicit val logicalPlanCacheKey: OptionLogicalPlanCacheKey[CypherVersionOption] =
    OptionLogicalPlanCacheKey.create(_.logicalPlanCacheKey)
  implicit val reader: OptionReader[CypherVersionOption] = singleOptionReader()
}

sealed abstract class CypherPlannerOption(plannerName: String) extends CypherKeyValueOption(plannerName) {
  override def companion: CypherPlannerOption.type = CypherPlannerOption

  /**
   * We create different compilers for different planner options.
   * See [[org.neo4j.cypher.internal.CompilerFactory]].
   * Each compiler has their own cache.
   * Therefore, we don't need to also include this in the cache key.
   */
  override def relevantForLogicalPlanCacheKey: Boolean = false
}

case object CypherPlannerOption extends CypherOptionCompanion[CypherPlannerOption](
      name = "planner",
      setting = Some(GraphDatabaseSettings.cypher_planner),
      cypherConfigField = Some(_.planner)
    ) {

  case object default extends CypherPlannerOption(CypherOption.DEFAULT)
  case object cost extends CypherPlannerOption("cost")
  case object idp extends CypherPlannerOption("idp")
  case object dp extends CypherPlannerOption("dp")

  def values: Set[CypherPlannerOption] = Set(cost, idp, dp)

  implicit val hasDefault: OptionDefault[CypherPlannerOption] = OptionDefault.create(default)
  implicit val renderer: OptionRenderer[CypherPlannerOption] = OptionRenderer.create(_.render)
  implicit val cacheKey: OptionCacheKey[CypherPlannerOption] = OptionCacheKey.create(_.cacheKey)

  implicit val logicalPlanCacheKey: OptionLogicalPlanCacheKey[CypherPlannerOption] =
    OptionLogicalPlanCacheKey.create(_.logicalPlanCacheKey)
  implicit val reader: OptionReader[CypherPlannerOption] = singleOptionReader()
}

sealed abstract class CypherRuntimeOption(runtimeName: String) extends CypherKeyValueOption(runtimeName) {
  override val companion: CypherRuntimeOption.type = CypherRuntimeOption

  /**
   * We create different compilers for different runtime options.
   * See [[org.neo4j.cypher.internal.CompilerFactory]].
   * Each compiler has their own cache.
   * Therefore, we don't need to also include this in the cache key.
   */
  override def relevantForLogicalPlanCacheKey: Boolean = false
}

case object CypherRuntimeOption extends CypherOptionCompanion[CypherRuntimeOption](
      name = "runtime",
      setting = Some(GraphDatabaseInternalSettings.cypher_runtime),
      cypherConfigField = Some(_.runtime)
    ) {

  case object default extends CypherRuntimeOption(CypherOption.DEFAULT)
  case object legacy extends CypherRuntimeOption("legacy")
  case object interpreted extends CypherRuntimeOption("interpreted")
  case object slotted extends CypherRuntimeOption("slotted")
  case object pipelined extends CypherRuntimeOption("pipelined")
  case object parallel extends CypherRuntimeOption("parallel")

  def values: Set[CypherRuntimeOption] = Set(interpreted, slotted, pipelined, parallel, legacy)

  implicit val hasDefault: OptionDefault[CypherRuntimeOption] = OptionDefault.create(default)
  implicit val renderer: OptionRenderer[CypherRuntimeOption] = OptionRenderer.create(_.render)
  implicit val cacheKey: OptionCacheKey[CypherRuntimeOption] = OptionCacheKey.create(_.cacheKey)

  implicit val logicalPlanCacheKey: OptionLogicalPlanCacheKey[CypherRuntimeOption] =
    OptionLogicalPlanCacheKey.create(_.logicalPlanCacheKey)
  implicit val reader: OptionReader[CypherRuntimeOption] = singleOptionReader()
}

sealed abstract class CypherUpdateStrategy(strategy: String) extends CypherKeyValueOption(strategy) {
  override def companion: CypherUpdateStrategy.type = CypherUpdateStrategy
  override def relevantForLogicalPlanCacheKey: Boolean = true
}

case object CypherUpdateStrategy extends CypherOptionCompanion[CypherUpdateStrategy](
      name = "updateStrategy"
    ) {

  case object default extends CypherUpdateStrategy(CypherOption.DEFAULT)
  case object eager extends CypherUpdateStrategy("eager")

  def values: Set[CypherUpdateStrategy] = Set(default, eager)

  implicit val hasDefault: OptionDefault[CypherUpdateStrategy] = OptionDefault.create(default)
  implicit val renderer: OptionRenderer[CypherUpdateStrategy] = OptionRenderer.create(_.render)
  implicit val cacheKey: OptionCacheKey[CypherUpdateStrategy] = OptionCacheKey.create(_.cacheKey)

  implicit val logicalPlanCacheKey: OptionLogicalPlanCacheKey[CypherUpdateStrategy] =
    OptionLogicalPlanCacheKey.create(_.logicalPlanCacheKey)
  implicit val reader: OptionReader[CypherUpdateStrategy] = singleOptionReader()
}

sealed abstract class CypherInferSchemaPartsOption(option: String) extends CypherKeyValueOption(option) {
  override def companion: CypherInferSchemaPartsOption.type = CypherInferSchemaPartsOption
  override def relevantForLogicalPlanCacheKey: Boolean = true
}

case object CypherInferSchemaPartsOption extends CypherOptionCompanion[CypherInferSchemaPartsOption](
      name = "inferSchemaParts",
      setting = Some(GraphDatabaseSettings.cypher_infer_schema_parts_strategy),
      cypherConfigField = Some(_.labelInference)
    ) {

  case object mostSelectiveLabel extends CypherInferSchemaPartsOption("most_selective_label")
  case object off extends CypherInferSchemaPartsOption("off")
  override def default: CypherInferSchemaPartsOption = off

  def values: Set[CypherInferSchemaPartsOption] = Set(default, mostSelectiveLabel, off)

  implicit val hasDefault: OptionDefault[CypherInferSchemaPartsOption] = OptionDefault.create(default)
  implicit val renderer: OptionRenderer[CypherInferSchemaPartsOption] = OptionRenderer.create(_.render)
  implicit val cacheKey: OptionCacheKey[CypherInferSchemaPartsOption] = OptionCacheKey.create(_.cacheKey)

  implicit val logicalPlanCacheKey: OptionLogicalPlanCacheKey[CypherInferSchemaPartsOption] =
    OptionLogicalPlanCacheKey.create(_.logicalPlanCacheKey)
  implicit val reader: OptionReader[CypherInferSchemaPartsOption] = singleOptionReader()
}

sealed abstract class CypherExpressionEngineOption(engineName: String) extends CypherKeyValueOption(engineName) {
  override def companion: CypherExpressionEngineOption.type = CypherExpressionEngineOption

  /**
   * The expression engine does not affect logical planning, only physical planning.
   */
  override def relevantForLogicalPlanCacheKey: Boolean = false
}

case object CypherExpressionEngineOption extends CypherOptionCompanion[CypherExpressionEngineOption](
      name = "expressionEngine",
      setting = Some(GraphDatabaseInternalSettings.cypher_expression_engine),
      cypherConfigField = Some(_.expressionEngineOption)
    ) {

  case object default extends CypherExpressionEngineOption(CypherOption.DEFAULT)
  case object interpreted extends CypherExpressionEngineOption("interpreted")
  case object compiled extends CypherExpressionEngineOption("compiled")
  case object onlyWhenHot extends CypherExpressionEngineOption("only_when_hot")

  def values: Set[CypherExpressionEngineOption] = Set(interpreted, compiled, onlyWhenHot)

  implicit val hasDefault: OptionDefault[CypherExpressionEngineOption] = OptionDefault.create(default)
  implicit val renderer: OptionRenderer[CypherExpressionEngineOption] = OptionRenderer.create(_.render)
  implicit val cacheKey: OptionCacheKey[CypherExpressionEngineOption] = OptionCacheKey.create(_.cacheKey)

  implicit val logicalPlanCacheKey: OptionLogicalPlanCacheKey[CypherExpressionEngineOption] =
    OptionLogicalPlanCacheKey.create(_.logicalPlanCacheKey)
  implicit val reader: OptionReader[CypherExpressionEngineOption] = singleOptionReader()
}

sealed abstract class CypherOperatorEngineOption(mode: String) extends CypherKeyValueOption(mode) {
  override def companion: CypherOperatorEngineOption.type = CypherOperatorEngineOption

  /**
   * The operator engine does not affect logical planning, only physical planning.
   */
  override def relevantForLogicalPlanCacheKey: Boolean = false
}

case object CypherOperatorEngineOption extends CypherOptionCompanion[CypherOperatorEngineOption](
      name = "operatorEngine",
      setting = Some(GraphDatabaseInternalSettings.cypher_operator_engine),
      cypherConfigField = Some(_.operatorEngine)
    ) {
  case object default extends CypherOperatorEngineOption(CypherOption.DEFAULT)
  case object compiled extends CypherOperatorEngineOption("compiled")
  case object interpreted extends CypherOperatorEngineOption("interpreted")

  def values: Set[CypherOperatorEngineOption] = Set(compiled, interpreted)

  implicit val hasDefault: OptionDefault[CypherOperatorEngineOption] = OptionDefault.create(default)
  implicit val renderer: OptionRenderer[CypherOperatorEngineOption] = OptionRenderer.create(_.render)
  implicit val cacheKey: OptionCacheKey[CypherOperatorEngineOption] = OptionCacheKey.create(_.cacheKey)

  implicit val logicalPlanCacheKey: OptionLogicalPlanCacheKey[CypherOperatorEngineOption] =
    OptionLogicalPlanCacheKey.create(_.logicalPlanCacheKey)
  implicit val reader: OptionReader[CypherOperatorEngineOption] = singleOptionReader()
}

sealed abstract class CypherParallelRuntimeSupportOption(mode: String) extends CypherKeyValueOption(mode) {
  override def companion: CypherParallelRuntimeSupportOption.type = CypherParallelRuntimeSupportOption

  /**
   * The expression engine does not affect logical planning, only physical planning.
   */
  override def relevantForLogicalPlanCacheKey: Boolean = false
}

case object CypherParallelRuntimeSupportOption extends CypherOptionCompanion[CypherParallelRuntimeSupportOption](
      name = "parallelRuntimeSupport",
      setting = Some(GraphDatabaseInternalSettings.cypher_parallel_runtime_support),
      cypherConfigField = Some(_.parallelRuntimeSupport)
    ) {

  case object disabled extends CypherParallelRuntimeSupportOption("disabled")
  case object all extends CypherParallelRuntimeSupportOption("all")
  override def default: CypherParallelRuntimeSupportOption = all

  override def values: Set[CypherParallelRuntimeSupportOption] = Set(disabled, all)

  implicit val hasDefault: OptionDefault[CypherParallelRuntimeSupportOption] = OptionDefault.create(default)
  implicit val renderer: OptionRenderer[CypherParallelRuntimeSupportOption] = OptionRenderer.create(_.render)
  implicit val cacheKey: OptionCacheKey[CypherParallelRuntimeSupportOption] = OptionCacheKey.create(_.cacheKey)

  implicit val logicalPlanCacheKey: OptionLogicalPlanCacheKey[CypherParallelRuntimeSupportOption] =
    OptionLogicalPlanCacheKey.create(_.logicalPlanCacheKey)
  implicit val reader: OptionReader[CypherParallelRuntimeSupportOption] = singleOptionReader()
}

sealed abstract class CypherParallelRuntimeConfigOption(selection: String) extends CypherKeyValueOption(selection) {
  override def companion: CypherParallelRuntimeConfigOption.type = CypherParallelRuntimeConfigOption

  def leverageOrder: Boolean

  /**
   * Allowing for leveraged order will change the logical plan.
   */
  override def relevantForLogicalPlanCacheKey: Boolean = true
}

case object CypherParallelRuntimeConfigOption extends CypherOptionCompanion[CypherParallelRuntimeConfigOption](
      name = "parallelRuntimeConfig",
      setting = Some(GraphDatabaseInternalSettings.parallel_runtime_config),
      cypherConfigField = Some(_.parallel_runtime_config)
    ) {

  case object none extends CypherParallelRuntimeConfigOption("none") {
    override def leverageOrder: Boolean = false
  }

  case object leverageOrder extends CypherParallelRuntimeConfigOption("leverageOrder") {
    override def leverageOrder: Boolean = true
  }
  override def default: CypherParallelRuntimeConfigOption = none

  override def values: Set[CypherParallelRuntimeConfigOption] = Set(none, leverageOrder)

  implicit val hasDefault: OptionDefault[CypherParallelRuntimeConfigOption] = OptionDefault.create(default)
  implicit val renderer: OptionRenderer[CypherParallelRuntimeConfigOption] = OptionRenderer.create(_.render)
  implicit val cacheKey: OptionCacheKey[CypherParallelRuntimeConfigOption] = OptionCacheKey.create(_.cacheKey)

  implicit val logicalPlanCacheKey: OptionLogicalPlanCacheKey[CypherParallelRuntimeConfigOption] =
    OptionLogicalPlanCacheKey.create(_.logicalPlanCacheKey)
  implicit val reader: OptionReader[CypherParallelRuntimeConfigOption] = singleOptionReader()
}

sealed abstract class CypherInterpretedPipesFallbackOption(mode: String) extends CypherKeyValueOption(mode) {
  override def companion: CypherInterpretedPipesFallbackOption.type = CypherInterpretedPipesFallbackOption

  /**
   * The expression engine does not affect logical planning, only physical planning.
   */
  override def relevantForLogicalPlanCacheKey: Boolean = false
}

case object CypherInterpretedPipesFallbackOption extends CypherOptionCompanion[CypherInterpretedPipesFallbackOption](
      name = "interpretedPipesFallback",
      setting = Some(GraphDatabaseInternalSettings.cypher_pipelined_interpreted_pipes_fallback),
      cypherConfigField = Some(_.interpretedPipesFallback)
    ) {

  case object default extends CypherInterpretedPipesFallbackOption(CypherOption.DEFAULT)
  case object disabled extends CypherInterpretedPipesFallbackOption("disabled")
  case object whitelistedPlansOnly extends CypherInterpretedPipesFallbackOption("whitelisted_plans_only")
  case object allPossiblePlans extends CypherInterpretedPipesFallbackOption("all")

  def values: Set[CypherInterpretedPipesFallbackOption] = Set(disabled, whitelistedPlansOnly, allPossiblePlans)

  implicit val hasDefault: OptionDefault[CypherInterpretedPipesFallbackOption] = OptionDefault.create(default)
  implicit val renderer: OptionRenderer[CypherInterpretedPipesFallbackOption] = OptionRenderer.create(_.render)
  implicit val cacheKey: OptionCacheKey[CypherInterpretedPipesFallbackOption] = OptionCacheKey.create(_.cacheKey)

  implicit val logicalPlanCacheKey: OptionLogicalPlanCacheKey[CypherInterpretedPipesFallbackOption] =
    OptionLogicalPlanCacheKey.create(_.logicalPlanCacheKey)
  implicit val reader: OptionReader[CypherInterpretedPipesFallbackOption] = singleOptionReader()
}

sealed abstract class CypherReplanOption(strategy: String) extends CypherKeyValueOption(strategy) {
  override def companion: CypherReplanOption.type = CypherReplanOption
  override def cacheKey: String = ""

  /**
   * This option affects replanning itself and it handled outside the cache.
   */
  override def relevantForLogicalPlanCacheKey: Boolean = false
}

case object CypherReplanOption extends CypherOptionCompanion[CypherReplanOption](
      name = "replan"
    ) {

  case object default extends CypherReplanOption(CypherOption.DEFAULT)
  case object force extends CypherReplanOption("force")
  case object skip extends CypherReplanOption("skip")

  def values: Set[CypherReplanOption] = Set(force, skip)

  implicit val hasDefault: OptionDefault[CypherReplanOption] = OptionDefault.create(default)
  implicit val renderer: OptionRenderer[CypherReplanOption] = OptionRenderer.create(_.render)
  implicit val cacheKey: OptionCacheKey[CypherReplanOption] = OptionCacheKey.create(_.cacheKey)

  implicit val logicalPlanCacheKey: OptionLogicalPlanCacheKey[CypherReplanOption] =
    OptionLogicalPlanCacheKey.create(_.logicalPlanCacheKey)
  implicit val reader: OptionReader[CypherReplanOption] = singleOptionReader()
}

sealed abstract class CypherCacheOption(strategy: String) extends CypherKeyValueOption(strategy) {
  override def companion: CypherCacheOption.type = CypherCacheOption
  override def cacheKey: String = ""

  /**
   * This option affects caching itself and it handled outside the cache.
   */
  override def relevantForLogicalPlanCacheKey: Boolean = false
}

case object CypherCacheOption extends CypherOptionCompanion[CypherCacheOption](
      name = "cache"
    ) {

  case object default extends CypherCacheOption(CypherOption.DEFAULT)
  case object force extends CypherCacheOption("force")
  case object skip extends CypherCacheOption("skip")

  def values: Set[CypherCacheOption] = Set(force, skip)

  implicit val hasDefault: OptionDefault[CypherCacheOption] = OptionDefault.create(default)
  implicit val renderer: OptionRenderer[CypherCacheOption] = OptionRenderer.create(_.render)
  implicit val cacheKey: OptionCacheKey[CypherCacheOption] = OptionCacheKey.create(_.cacheKey)

  implicit val logicalPlanCacheKey: OptionLogicalPlanCacheKey[CypherCacheOption] =
    OptionLogicalPlanCacheKey.create(_.logicalPlanCacheKey)
  implicit val reader: OptionReader[CypherCacheOption] = singleOptionReader()
}

sealed abstract class CypherConnectComponentsPlannerOption(planner: String) extends CypherKeyValueOption(planner) {
  override def companion: CypherConnectComponentsPlannerOption.type = CypherConnectComponentsPlannerOption
  override def relevantForLogicalPlanCacheKey: Boolean = true
}

case object CypherConnectComponentsPlannerOption extends CypherOptionCompanion[CypherConnectComponentsPlannerOption](
      name = "connectComponentsPlanner"
    ) {

  case object default extends CypherConnectComponentsPlannerOption(CypherOption.DEFAULT)
  case object greedy extends CypherConnectComponentsPlannerOption("greedy")
  case object idp extends CypherConnectComponentsPlannerOption("idp")

  def values: Set[CypherConnectComponentsPlannerOption] = Set(greedy, idp)

  implicit val hasDefault: OptionDefault[CypherConnectComponentsPlannerOption] = OptionDefault.create(default)
  implicit val renderer: OptionRenderer[CypherConnectComponentsPlannerOption] = OptionRenderer.create(_.render)
  implicit val cacheKey: OptionCacheKey[CypherConnectComponentsPlannerOption] = OptionCacheKey.create(_.cacheKey)

  implicit val logicalPlanCacheKey: OptionLogicalPlanCacheKey[CypherConnectComponentsPlannerOption] =
    OptionLogicalPlanCacheKey.create(_.logicalPlanCacheKey)
  implicit val reader: OptionReader[CypherConnectComponentsPlannerOption] = singleOptionReader()
}

sealed abstract class CypherEagerAnalyzerOption(name: String) extends CypherKeyValueOption(name) {
  override def companion: CypherEagerAnalyzerOption.type = CypherEagerAnalyzerOption
  override def relevantForLogicalPlanCacheKey: Boolean = true
}

case object CypherEagerAnalyzerOption extends CypherOptionCompanion[CypherEagerAnalyzerOption](
      name = "eagerAnalyzer"
    ) {

  case object lp extends CypherEagerAnalyzerOption("lp")
  case object ir extends CypherEagerAnalyzerOption("ir")

  override def default: CypherEagerAnalyzerOption = lp

  def values: Set[CypherEagerAnalyzerOption] = Set(lp, ir)

  implicit val hasDefault: OptionDefault[CypherEagerAnalyzerOption] = OptionDefault.create(default)
  implicit val renderer: OptionRenderer[CypherEagerAnalyzerOption] = OptionRenderer.create(_.render)
  implicit val cacheKey: OptionCacheKey[CypherEagerAnalyzerOption] = OptionCacheKey.create(_.cacheKey)

  implicit val logicalPlanCacheKey: OptionLogicalPlanCacheKey[CypherEagerAnalyzerOption] =
    OptionLogicalPlanCacheKey.create(_.logicalPlanCacheKey)
  implicit val reader: OptionReader[CypherEagerAnalyzerOption] = singleOptionReader()
}

sealed abstract class CypherStatefulShortestPlanningModeOption(name: String) extends CypherKeyValueOption(name) {
  override def companion: CypherStatefulShortestPlanningModeOption.type = CypherStatefulShortestPlanningModeOption
  override def relevantForLogicalPlanCacheKey: Boolean = true
}

case object CypherStatefulShortestPlanningModeOption
    extends CypherOptionCompanion[CypherStatefulShortestPlanningModeOption](
      name = "statefulShortestPlanningMode",
      setting = Some(GraphDatabaseInternalSettings.stateful_shortest_planning_mode),
      cypherConfigField = Some(_.statefulShortestPlanningMode)
    ) {

  // If you get a bad CartesianProduct, see the comment on StatefulShortestPlanningMode.INTO_ONLY
  case object intoOnly extends CypherStatefulShortestPlanningModeOption("into_only")
  case object allIfPossible extends CypherStatefulShortestPlanningModeOption("all_if_possible")
  case object cardinalityHeuristic extends CypherStatefulShortestPlanningModeOption("cardinality_heuristic")

  override def default: CypherStatefulShortestPlanningModeOption = cardinalityHeuristic

  def values: Set[CypherStatefulShortestPlanningModeOption] = Set(intoOnly, allIfPossible, cardinalityHeuristic)

  implicit val hasDefault: OptionDefault[CypherStatefulShortestPlanningModeOption] = OptionDefault.create(default)
  implicit val renderer: OptionRenderer[CypherStatefulShortestPlanningModeOption] = OptionRenderer.create(_.render)
  implicit val cacheKey: OptionCacheKey[CypherStatefulShortestPlanningModeOption] = OptionCacheKey.create(_.cacheKey)

  implicit val logicalPlanCacheKey: OptionLogicalPlanCacheKey[CypherStatefulShortestPlanningModeOption] =
    OptionLogicalPlanCacheKey.create(_.logicalPlanCacheKey)
  implicit val reader: OptionReader[CypherStatefulShortestPlanningModeOption] = singleOptionReader()
}

sealed abstract class CypherPlanVarExpandInto(name: String) extends CypherKeyValueOption(name) {
  override def companion: CypherPlanVarExpandInto.type = CypherPlanVarExpandInto
  override def relevantForLogicalPlanCacheKey: Boolean = true
}

/**
 * Ideally, we want to use 'minimum_cost' (and get rid of the 'planVarExpandInto Heuristic').
 * However, cardinality estimation errors for variable length patterns sometimes lead to very expensive Cartesian Products followed by an ExpandInto.
 * The 'single_row' tries to mitigate those situations by planning an ExpandAll when the source-plan has a cardinality estimate larger than 1.
 */
case object CypherPlanVarExpandInto
    extends CypherOptionCompanion[CypherPlanVarExpandInto](
      name = "planVarExpandInto",
      setting = Some(GraphDatabaseInternalSettings.plan_var_expand_into),
      cypherConfigField = Some(_.planVarExpandInto)
    ) {
  case object minimumCost extends CypherPlanVarExpandInto("minimum_cost")
  case object singleRow extends CypherPlanVarExpandInto("single_row")

  override def default: CypherPlanVarExpandInto = singleRow

  override def values: Set[CypherPlanVarExpandInto] = Set(minimumCost, singleRow)

  implicit val hasDefault: OptionDefault[CypherPlanVarExpandInto] = OptionDefault.create(default)
  implicit val renderer: OptionRenderer[CypherPlanVarExpandInto] = OptionRenderer.create(_.render)
  implicit val cacheKey: OptionCacheKey[CypherPlanVarExpandInto] = OptionCacheKey.create(_.cacheKey)

  implicit val logicalPlanCacheKey: OptionLogicalPlanCacheKey[CypherPlanVarExpandInto] =
    OptionLogicalPlanCacheKey.create(_.logicalPlanCacheKey)
  implicit val reader: OptionReader[CypherPlanVarExpandInto] = singleOptionReader()

}

sealed abstract class CypherPipelinedBatchSizePresetOption(val preset: String) extends CypherKeyValueOption(preset) {
  override def companion: CypherPipelinedBatchSizePresetOption.type = CypherPipelinedBatchSizePresetOption
  override def cacheKey: String = "" // This is option only has effect via derivedOptionsCacheKey

  override def relevantForLogicalPlanCacheKey: Boolean =
    false // This is option only has effect via derivedOptionsCacheKey
}

case object CypherPipelinedBatchSizePresetOption extends CypherOptionCompanion[CypherPipelinedBatchSizePresetOption](
      name = "batchSizePreset",
      setting = Some(GraphDatabaseInternalSettings.cypher_pipelined_batch_size_preset),
      cypherConfigField = Some(_.pipelinedBatchSizePreset)
    ) {
  case object default extends CypherPipelinedBatchSizePresetOption("default")
  case object disabled extends CypherPipelinedBatchSizePresetOption("disabled")
  case object small extends CypherPipelinedBatchSizePresetOption("small")
  case object medium extends CypherPipelinedBatchSizePresetOption("medium")
  case object large extends CypherPipelinedBatchSizePresetOption("large")
  case object custom extends CypherPipelinedBatchSizePresetOption("custom")

  def values: Set[CypherPipelinedBatchSizePresetOption] = Set(default, disabled, small, medium, large, custom)

  implicit val hasDefault: OptionDefault[CypherPipelinedBatchSizePresetOption] = OptionDefault.create(default)
  implicit val renderer: OptionRenderer[CypherPipelinedBatchSizePresetOption] = OptionRenderer.create(_.render)
  implicit val cacheKey: OptionCacheKey[CypherPipelinedBatchSizePresetOption] = OptionCacheKey.create(_.cacheKey)

  implicit val logicalPlanCacheKey: OptionLogicalPlanCacheKey[CypherPipelinedBatchSizePresetOption] =
    OptionLogicalPlanCacheKey.create(_.logicalPlanCacheKey)
  implicit val reader: OptionReader[CypherPipelinedBatchSizePresetOption] = singleOptionReader()

  override def fromConfig(configuration: Config): CypherPipelinedBatchSizePresetOption = {
    configuration.get(GraphDatabaseInternalSettings.cypher_pipelined_batch_size_preset) match {
      case CypherPipelinedBatchSizePreset.DEFAULT =>
        CypherPipelinedBatchSizePresetOption.default
      case CypherPipelinedBatchSizePreset.DISABLED =>
        CypherPipelinedBatchSizePresetOption.disabled
      case CypherPipelinedBatchSizePreset.SMALL =>
        CypherPipelinedBatchSizePresetOption.small
      case CypherPipelinedBatchSizePreset.MEDIUM =>
        CypherPipelinedBatchSizePresetOption.medium
      case CypherPipelinedBatchSizePreset.LARGE =>
        CypherPipelinedBatchSizePresetOption.large
      case CypherPipelinedBatchSizePreset.CUSTOM =>
        CypherPipelinedBatchSizePresetOption.custom
    }
  }

  // This is included in derivedOptionsCacheKey
  def batchSizeConfigFrom(
    option: CypherPipelinedBatchSizePresetOption,
    defaultBatchSizeSmall: Int,
    defaultBatchSizeBig: Int
  ): CypherPipelinedBatchSize = {
    option match {
      case CypherPipelinedBatchSizePresetOption.default | CypherPipelinedBatchSizePresetOption.custom =>
        CypherPipelinedBatchSize(defaultBatchSizeSmall, defaultBatchSizeBig)
      case CypherPipelinedBatchSizePresetOption.disabled =>
        CypherPipelinedBatchSize(1, 1)
      case CypherPipelinedBatchSizePresetOption.small =>
        CypherPipelinedBatchSize(128, 128)
      case CypherPipelinedBatchSizePresetOption.medium =>
        CypherPipelinedBatchSize(1024, 1024)
      case CypherPipelinedBatchSizePresetOption.large =>
        CypherPipelinedBatchSize(16384, 16384)
    }
  }
}

object CypherPipelinedBatchSize {

  // Used to avoid polluting the cache key when nothing has been reconfigured.
  final val defaultDefaultBatchSize: CypherPipelinedBatchSize =
    CypherPipelinedBatchSize(
      GraphDatabaseInternalSettings.cypher_pipelined_batch_size_small.defaultValue(),
      GraphDatabaseInternalSettings.cypher_pipelined_batch_size_big.defaultValue()
    )
}

case class CypherPipelinedBatchSize(small: Int, big: Int) {

  def cacheKey: String = {
    if (this == CypherPipelinedBatchSize.defaultDefaultBatchSize) {
      "" // Do not pollute the cache key with default values
    } else {
      String.format("BatchSize(%s,%s)", small, big)
    }
  }
}

sealed abstract class CypherPipelinedBatchReuseOption(val preset: String) extends CypherKeyValueOption(preset) {
  override def companion: CypherPipelinedBatchReuseOption.type = CypherPipelinedBatchReuseOption
  override def cacheKey: String = "" // Does not affect the cached query

  /** Does not affect the plan we produce. */
  override def relevantForLogicalPlanCacheKey: Boolean = false
}

case object CypherPipelinedBatchReuseOption extends CypherOptionCompanion[CypherPipelinedBatchReuseOption](
      name = "batchReuse",
      setting = Some(GraphDatabaseInternalSettings.cypher_pipelined_batch_reuse),
      cypherConfigField = Some(_.pipelinedBatchReuse)
    ) {
  case object default extends CypherPipelinedBatchReuseOption("default")

  case object disabled extends CypherPipelinedBatchReuseOption("disabled")

  case object pack extends CypherPipelinedBatchReuseOption("pack")

  case object full extends CypherPipelinedBatchReuseOption("full")

  def values: Set[CypherPipelinedBatchReuseOption] = Set(default, disabled, pack, full)

  implicit val hasDefault: OptionDefault[CypherPipelinedBatchReuseOption] = OptionDefault.create(default)
  implicit val renderer: OptionRenderer[CypherPipelinedBatchReuseOption] = OptionRenderer.create(_.render)
  implicit val cacheKey: OptionCacheKey[CypherPipelinedBatchReuseOption] = OptionCacheKey.create(_.cacheKey)

  implicit val logicalPlanCacheKey: OptionLogicalPlanCacheKey[CypherPipelinedBatchReuseOption] =
    OptionLogicalPlanCacheKey.create(_.logicalPlanCacheKey)
  implicit val reader: OptionReader[CypherPipelinedBatchReuseOption] = singleOptionReader()
}

sealed abstract class CypherHeapEstimatorCacheOption(val preset: String) extends CypherKeyValueOption(preset) {
  override def companion: CypherHeapEstimatorCacheOption.type = CypherHeapEstimatorCacheOption
  override def cacheKey: String = "" // Does not affect the cached query

  /** Does not affect the plan we produce. */
  override def relevantForLogicalPlanCacheKey: Boolean = false
}

case object CypherHeapEstimatorCacheOption extends CypherOptionCompanion[CypherHeapEstimatorCacheOption](
      name = "heapEstimatorCache",
      setting = Some(GraphDatabaseInternalSettings.heap_estimator_cache_preset),
      cypherConfigField = Some(_.heapEstimatorCacheOption)
    ) {

  case object default extends CypherHeapEstimatorCacheOption("default")
  case object disabled extends CypherHeapEstimatorCacheOption("disabled")
  case object small extends CypherHeapEstimatorCacheOption("small")
  case object large extends CypherHeapEstimatorCacheOption("large")
  case object custom extends CypherHeapEstimatorCacheOption("custom")

  def values: Set[CypherHeapEstimatorCacheOption] = Set(default, disabled, small, large, custom)

  implicit val hasDefault: OptionDefault[CypherHeapEstimatorCacheOption] = OptionDefault.create(default)
  implicit val renderer: OptionRenderer[CypherHeapEstimatorCacheOption] = OptionRenderer.create(_.render)
  implicit val cacheKey: OptionCacheKey[CypherHeapEstimatorCacheOption] = OptionCacheKey.create(_.cacheKey)

  implicit val logicalPlanCacheKey: OptionLogicalPlanCacheKey[CypherHeapEstimatorCacheOption] =
    OptionLogicalPlanCacheKey.create(_.logicalPlanCacheKey)
  implicit val reader: OptionReader[CypherHeapEstimatorCacheOption] = singleOptionReader()

  override def fromConfig(configuration: Config): CypherHeapEstimatorCacheOption = {
    configuration.get(GraphDatabaseInternalSettings.heap_estimator_cache_preset) match {
      case HeapEstimatorCachePreset.DEFAULT =>
        CypherHeapEstimatorCacheOption.default
      case HeapEstimatorCachePreset.DISABLED =>
        CypherHeapEstimatorCacheOption.disabled
      case HeapEstimatorCachePreset.SMALL =>
        CypherHeapEstimatorCacheOption.small
      case HeapEstimatorCachePreset.LARGE =>
        CypherHeapEstimatorCacheOption.large
      case HeapEstimatorCachePreset.CUSTOM =>
        CypherHeapEstimatorCacheOption.custom
    }
  }

  def heapEstimatorCacheConfigFrom(
    option: CypherHeapEstimatorCacheOption,
    cypherConfig: CypherConfiguration
  ): HeapEstimatorCacheConfig = {
    option match {
      case CypherHeapEstimatorCacheOption.default =>
        HeapEstimatorCacheConfig.DEFAULT
      case CypherHeapEstimatorCacheOption.disabled =>
        HeapEstimatorCacheConfig.DISABLED
      case CypherHeapEstimatorCacheOption.small =>
        HeapEstimatorCacheConfig.SMALL
      case CypherHeapEstimatorCacheOption.large =>
        HeapEstimatorCacheConfig.LARGE
      case CypherHeapEstimatorCacheOption.custom =>
        cypherConfig.customHeapEstimatorCacheConfig
    }
  }
}

sealed abstract class CypherDebugOption(flag: String) extends CypherKeyValueOption(flag) {
  override def companion: CypherDebugOption.type = CypherDebugOption

  /**
   * Queries with debug flags are never cached.
   */
  override def relevantForLogicalPlanCacheKey: Boolean = false
}

case object CypherDebugOption extends CypherOptionCompanion[CypherDebugOption](
      name = "debug",
      cypherConfigBooleans = Map()
    ) {
  // Unused. We need to have a default
  case object default extends CypherDebugOption("none")
  case object tostring extends CypherDebugOption("tostring")
  case object printCostComparisons extends CypherDebugOption("printcostcomparisons")
  case object logCostComparisons extends CypherDebugOption("logcostcomparisons")
  case object generateJavaSource extends CypherDebugOption("generate_java_source")
  case object showJavaSource extends CypherDebugOption("show_java_source")
  case object showBytecode extends CypherDebugOption("show_bytecode")
  case object visualizePipelines extends CypherDebugOption("visualizepipelines")
  case object visualizePipelinesMermaid extends CypherDebugOption("mermaid")
  case object visualizePipelinesGraphviz extends CypherDebugOption("graphviz")
  case object inverseCost extends CypherDebugOption("inverse_cost")
  case object queryGraph extends CypherDebugOption("querygraph")
  case object ast extends CypherDebugOption("ast")
  case object semanticState extends CypherDebugOption("semanticstate")
  case object logicalPlan extends CypherDebugOption("logicalplan")
  case object logicalPlanBuilder extends CypherDebugOption("logicalplanbuilder")
  case object rawCardinalities extends CypherDebugOption("rawcardinalities")
  case object renderDistinctness extends CypherDebugOption("renderdistinctness")
  case object renderNestedPlanExpressions extends CypherDebugOption("rendernestedplanexpressions")
  case object warnOnCompilationErrors extends CypherDebugOption("warnoncompilationerrors")
  case object disableExistsSubqueryCaching extends CypherDebugOption("disableexistssubquerycaching")
  case object verboseEagernessReasons extends CypherDebugOption("verboseeagernessreasons")
  case object printIDPLog extends CypherDebugOption("printidplog")
  case object disablePropertyCaching extends CypherDebugOption("disablepropertycaching")
  case object logPlanningSteps extends CypherDebugOption("logplanningsteps")

  def values: Set[CypherDebugOption] = Set(
    tostring,
    printCostComparisons,
    logCostComparisons,
    generateJavaSource,
    showJavaSource,
    showBytecode,
    visualizePipelines,
    visualizePipelinesMermaid,
    visualizePipelinesGraphviz,
    inverseCost,
    queryGraph,
    ast,
    semanticState,
    logicalPlan,
    logicalPlanBuilder,
    rawCardinalities,
    renderDistinctness,
    renderNestedPlanExpressions,
    warnOnCompilationErrors,
    disableExistsSubqueryCaching,
    verboseEagernessReasons,
    printIDPLog,
    disablePropertyCaching,
    logPlanningSteps
  )

  implicit val hasDefault: OptionDefault[CypherDebugOption] = OptionDefault.create(default)
  implicit val renderer: OptionRenderer[CypherDebugOption] = OptionRenderer.create(_.render)
  implicit val cacheKey: OptionCacheKey[CypherDebugOption] = OptionCacheKey.create(_.cacheKey)

  implicit val logicalPlanCacheKey: OptionLogicalPlanCacheKey[CypherDebugOption] =
    OptionLogicalPlanCacheKey.create(_.logicalPlanCacheKey)
  implicit val reader: OptionReader[Set[CypherDebugOption]] = multiOptionReader()
}

object CypherDebugOptions {
  def default: CypherDebugOptions = CypherDebugOptions(Set.empty)
  implicit val hasDefault: OptionDefault[CypherDebugOptions] = OptionDefault.create(default)
  implicit val renderer: OptionRenderer[CypherDebugOptions] = OptionRenderer.create(_.render)
  implicit val cacheKey: OptionCacheKey[CypherDebugOptions] = OptionCacheKey.create(_.cacheKey)

  implicit val logicalPlanCacheKey: OptionLogicalPlanCacheKey[CypherDebugOptions] =
    OptionLogicalPlanCacheKey.create(_.logicalPlanCacheKey)
}

case class CypherDebugOptions(enabledOptions: Set[CypherDebugOption]) {

  def withOptionEnabled(option: CypherDebugOption): CypherDebugOptions = copy(enabledOptions + option)

  def withOptionDisabled(option: CypherDebugOption): CypherDebugOptions = copy(enabledOptions - option)

  def enabledOptionsSeq: Seq[CypherDebugOption] = enabledOptions.toSeq.sortBy(_.name)

  def render: String = enabledOptionsSeq.map(_.render).mkString(" ")

  def cacheKey: String = enabledOptionsSeq.map(_.cacheKey).mkString(" ")

  def logicalPlanCacheKey: String = enabledOptionsSeq.map(_.logicalPlanCacheKey).mkString(" ")

  def isEmpty: Boolean = enabledOptions.isEmpty

  private def isEnabled(option: CypherDebugOption): Boolean = enabledOptions.contains(option)

  val toStringEnabled: Boolean = isEnabled(CypherDebugOption.tostring)
  val printCostComparisonsEnabled: Boolean = isEnabled(CypherDebugOption.printCostComparisons)
  val logCostComparisonsEnabled: Boolean = isEnabled(CypherDebugOption.logCostComparisons)
  val generateJavaSourceEnabled: Boolean = isEnabled(CypherDebugOption.generateJavaSource)
  val showJavaSourceEnabled: Boolean = isEnabled(CypherDebugOption.showJavaSource)
  val showBytecodeEnabled: Boolean = isEnabled(CypherDebugOption.showBytecode)
  val visualizePipelinesEnabled: Boolean = isEnabled(CypherDebugOption.visualizePipelines)
  val visualizePipelinesMermaidEnabled: Boolean = isEnabled(CypherDebugOption.visualizePipelinesMermaid)
  val visualizePipelinesGraphvizEnabled: Boolean = isEnabled(CypherDebugOption.visualizePipelinesGraphviz)
  val inverseCostEnabled: Boolean = isEnabled(CypherDebugOption.inverseCost)
  val queryGraphEnabled: Boolean = isEnabled(CypherDebugOption.queryGraph)
  val astEnabled: Boolean = isEnabled(CypherDebugOption.ast)
  val semanticStateEnabled: Boolean = isEnabled(CypherDebugOption.semanticState)
  val logicalPlanEnabled: Boolean = isEnabled(CypherDebugOption.logicalPlan)
  val logicalPlanBuilderEnabled: Boolean = isEnabled(CypherDebugOption.logicalPlanBuilder)
  val rawCardinalitiesEnabled: Boolean = isEnabled(CypherDebugOption.rawCardinalities)
  val renderDistinctnessEnabled: Boolean = isEnabled(CypherDebugOption.renderDistinctness)
  val renderNestedPlanExpressions: Boolean = isEnabled(CypherDebugOption.renderNestedPlanExpressions)
  val warnOnCompilationErrors: Boolean = isEnabled(CypherDebugOption.warnOnCompilationErrors)
  val disableExistsSubqueryCaching: Boolean = isEnabled(CypherDebugOption.disableExistsSubqueryCaching)
  val verboseEagernessReasons: Boolean = isEnabled(CypherDebugOption.verboseEagernessReasons)
  val printIDPLog: Boolean = isEnabled(CypherDebugOption.printIDPLog)
  val disablePropertyCaching: Boolean = isEnabled(CypherDebugOption.disablePropertyCaching)
  val logPlanningSteps: Boolean = isEnabled(CypherDebugOption.logPlanningSteps)
}
