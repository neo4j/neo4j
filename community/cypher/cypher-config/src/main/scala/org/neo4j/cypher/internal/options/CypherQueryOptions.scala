/*
 * Copyright (c) "Neo4j"
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
import org.neo4j.cypher.internal.options.CypherDebugOption.disallowSplittingTop
import org.neo4j.cypher.internal.options.CypherQueryOptions.ILLEGAL_EXPRESSION_ENGINE_RUNTIME_COMBINATIONS
import org.neo4j.cypher.internal.options.CypherQueryOptions.ILLEGAL_INTERPRETED_PIPES_FALLBACK_RUNTIME_COMBINATIONS
import org.neo4j.cypher.internal.options.CypherQueryOptions.ILLEGAL_OPERATOR_ENGINE_RUNTIME_COMBINATIONS

/**
 * Collects all cypher options that can be set on query basis (pre-parser options)
 */
case class CypherQueryOptions(
  version: CypherVersion,
  executionMode: CypherExecutionMode,
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
  if (ILLEGAL_EXPRESSION_ENGINE_RUNTIME_COMBINATIONS((expressionEngine, runtime)))
    throw new InvalidCypherOption(s"Cannot combine EXPRESSION ENGINE '${expressionEngine.name}' with RUNTIME '${runtime.name}'")

  if (ILLEGAL_OPERATOR_ENGINE_RUNTIME_COMBINATIONS((operatorEngine, runtime)))
    throw new InvalidCypherOption(s"Cannot combine OPERATOR ENGINE '${operatorEngine.name}' with RUNTIME '${runtime.name}'")

  if (ILLEGAL_INTERPRETED_PIPES_FALLBACK_RUNTIME_COMBINATIONS((interpretedPipesFallback, runtime)))
    throw new InvalidCypherOption(s"Cannot combine INTERPRETED PIPES FALLBACK '${interpretedPipesFallback.name}' with RUNTIME '${runtime.name}'")

  def render: String = CypherQueryOptions.renderer.render(this)

  /**
   * Cache key used for executableQueryCache, astCache, and exeuctionPlanCache.
   */
  def cacheKey: String = CypherQueryOptions.cacheKey.cacheKey(this)

  /**
   * Cache key used for logicalPlanCache.
   */
  def logicalPlanCacheKey: String = CypherQueryOptions.logicalPlanCacheKey.logicalPlanCacheKey(this)
}

object CypherQueryOptions {

  private val hasDefault = OptionDefault.derive[CypherQueryOptions]
  private val renderer = OptionRenderer.derive[CypherQueryOptions]
  private val cacheKey = OptionCacheKey.derive[CypherQueryOptions]
  private val logicalPlanCacheKey = OptionLogicalPlanCacheKey.derive[CypherQueryOptions]
  private val reader = OptionReader.derive[CypherQueryOptions]

  val default: CypherQueryOptions = hasDefault.default

  def fromValues(config: CypherConfiguration, keyValues: Set[(String, String)]): CypherQueryOptions = {
    reader.read(OptionReader.Input(config, keyValues)) match {

      case OptionReader.Result(remainder, _) if remainder.keyValues.nonEmpty =>
        val keys = remainder.keyValues.map(_._1).mkString(", ")
        throw new InvalidCypherOption(s"Unsupported options: $keys")

      case OptionReader.Result(_, options) =>
        if (options.debugOptions.generateJavaSourceEnabled && !config.allowSourceGeneration) {
          throw new InvalidCypherOption("In order to use source generation you need to enable " +
            "`internal.cypher.pipelined.allow_source_generation`")
        }
        options
    }
  }

  private final def ILLEGAL_EXPRESSION_ENGINE_RUNTIME_COMBINATIONS: Set[(CypherExpressionEngineOption, CypherRuntimeOption)] =
    Set(
      (CypherExpressionEngineOption.compiled, CypherRuntimeOption.interpreted))
  private final def ILLEGAL_OPERATOR_ENGINE_RUNTIME_COMBINATIONS: Set[(CypherOperatorEngineOption, CypherRuntimeOption)] =
    Set(
      (CypherOperatorEngineOption.compiled, CypherRuntimeOption.slotted),
      (CypherOperatorEngineOption.compiled, CypherRuntimeOption.interpreted))
  private final def ILLEGAL_INTERPRETED_PIPES_FALLBACK_RUNTIME_COMBINATIONS: Set[(CypherInterpretedPipesFallbackOption, CypherRuntimeOption)] =
    Set(
      (CypherInterpretedPipesFallbackOption.disabled, CypherRuntimeOption.slotted),
      (CypherInterpretedPipesFallbackOption.disabled, CypherRuntimeOption.interpreted),
      (CypherInterpretedPipesFallbackOption.whitelistedPlansOnly, CypherRuntimeOption.slotted),
      (CypherInterpretedPipesFallbackOption.whitelistedPlansOnly, CypherRuntimeOption.interpreted),
      (CypherInterpretedPipesFallbackOption.allPossiblePlans, CypherRuntimeOption.slotted),
      (CypherInterpretedPipesFallbackOption.allPossiblePlans, CypherRuntimeOption.interpreted)
    )
}

sealed abstract class CypherExecutionMode(val modeName: String) extends CypherOption(modeName) {
  override def companion: CypherExecutionMode.type = CypherExecutionMode
  override def render: String = super.render.toUpperCase
  override def cacheKey: String = super.cacheKey.toUpperCase

  /** Does not affect the plan we produce. */
  override def relevantForLogicalPlanCacheKey: Boolean = false
}

case object CypherExecutionMode extends CypherOptionCompanion[CypherExecutionMode](
  name = "execution mode",
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

sealed abstract class CypherVersion(versionName: String) extends CypherOption(versionName) {
  override def companion: CypherVersion.type = CypherVersion
  override def cacheKey: String = name

  /**
   * We create different compilers for different planner options.
   * See [[org.neo4j.cypher.internal.CompilerFactory]].
   * Each compiler has their own cache.
   * Therefore, we don't need to also include this in the cache key.
   */
  override def relevantForLogicalPlanCacheKey: Boolean = false
}

case object CypherVersion extends CypherOptionCompanion[CypherVersion](
  name = "version",
  setting = Some(GraphDatabaseSettings.cypher_parser_version),
  cypherConfigField = Some(_.version),
) {
  case object v3_5 extends CypherVersion("3.5")
  case object v4_3 extends CypherVersion("4.3")
  case object v4_4 extends CypherVersion("4.4")

  val default: CypherVersion = v4_4
  def values: Set[CypherVersion] = Set(v3_5, v4_3, v4_4)

  implicit val hasDefault: OptionDefault[CypherVersion] = OptionDefault.create(default)
  implicit val renderer: OptionRenderer[CypherVersion] = OptionRenderer.create(_.render)
  implicit val cacheKey: OptionCacheKey[CypherVersion] = OptionCacheKey.create(_.cacheKey)

  implicit val logicalPlanCacheKey: OptionLogicalPlanCacheKey[CypherVersion] =
    OptionLogicalPlanCacheKey.create(_.logicalPlanCacheKey)
  implicit val reader: OptionReader[CypherVersion] = singleOptionReader()
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
  cypherConfigField = Some(_.planner),
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
  cypherConfigField = Some(_.runtime),
) {

  case object default extends CypherRuntimeOption(CypherOption.DEFAULT)
  case object interpreted extends CypherRuntimeOption("interpreted")
  case object slotted extends CypherRuntimeOption("slotted")
  case object pipelined extends CypherRuntimeOption("pipelined")
  case object parallel extends CypherRuntimeOption("parallel")

  def values: Set[CypherRuntimeOption] = Set(interpreted, slotted, pipelined, parallel)

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
  name = "updateStrategy",
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
  cypherConfigField = Some(_.operatorEngine),
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
  cypherConfigField = Some(_.interpretedPipesFallback),
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
  name = "replan",
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

sealed abstract class CypherConnectComponentsPlannerOption(planner: String) extends CypherKeyValueOption(planner) {
  override def companion: CypherConnectComponentsPlannerOption.type = CypherConnectComponentsPlannerOption
  override def relevantForLogicalPlanCacheKey: Boolean = true
}

case object CypherConnectComponentsPlannerOption extends CypherOptionCompanion[CypherConnectComponentsPlannerOption](
  name = "connectComponentsPlanner",
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

sealed abstract class CypherDebugOption(flag: String) extends CypherKeyValueOption(flag) {
  override def companion: CypherDebugOption.type = CypherDebugOption

  /**
   * Queries with debug flags are never cached.
   */
  override def relevantForLogicalPlanCacheKey: Boolean = false
}

case object CypherDebugOption extends CypherOptionCompanion[CypherDebugOption](
  name = "debug",
  cypherConfigBooleans = Map(disallowSplittingTop -> (_.disallowSplittingTop))
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
  case object inverseCost extends CypherDebugOption("inverse_cost")
  case object queryGraph extends CypherDebugOption("querygraph")
  case object ast extends CypherDebugOption("ast")
  case object semanticState extends CypherDebugOption("semanticstate")
  case object logicalPlan extends CypherDebugOption("logicalplan")
  case object fabricLogPlan extends CypherDebugOption("fabriclogplan")
  case object fabricLogRecords extends CypherDebugOption("fabriclogrecords")
  case object logicalPlanBuilder extends CypherDebugOption("logicalplanbuilder")
  case object rawCardinalities extends CypherDebugOption("rawcardinalities")
  case object disallowSplittingTop extends CypherDebugOption("disallowsplittingtop")
  case object warnOnCompilationErrors extends CypherDebugOption("warnoncompilationerrors")

  def values: Set[CypherDebugOption] = Set(
    tostring,
    printCostComparisons,
    logCostComparisons,
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
    fabricLogRecords,
    logicalPlanBuilder,
    rawCardinalities,
    disallowSplittingTop,
    warnOnCompilationErrors
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
  val inverseCostEnabled: Boolean = isEnabled(CypherDebugOption.inverseCost)
  val queryGraphEnabled: Boolean = isEnabled(CypherDebugOption.queryGraph)
  val astEnabled: Boolean = isEnabled(CypherDebugOption.ast)
  val semanticStateEnabled: Boolean = isEnabled(CypherDebugOption.semanticState)
  val logicalPlanEnabled: Boolean = isEnabled(CypherDebugOption.logicalPlan)
  val fabricLogPlanEnabled: Boolean = isEnabled(CypherDebugOption.fabricLogPlan)
  val fabricLogRecordsEnabled: Boolean = isEnabled(CypherDebugOption.fabricLogRecords)
  val logicalPlanBuilderEnabled: Boolean = isEnabled(CypherDebugOption.logicalPlanBuilder)
  val rawCardinalitiesEnabled: Boolean = isEnabled(CypherDebugOption.rawCardinalities)
  val disallowSplittingTopEnabled: Boolean = isEnabled(CypherDebugOption.disallowSplittingTop)
  val warnOnCompilationErrors: Boolean = isEnabled(CypherDebugOption.warnOnCompilationErrors)
}
