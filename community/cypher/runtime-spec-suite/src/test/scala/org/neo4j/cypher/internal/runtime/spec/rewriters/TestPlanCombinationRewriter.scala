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
package org.neo4j.cypher.internal.runtime.spec.rewriters

import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LeveragedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanCombinationRewriter.TestPlanCombinationRewriterHint
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanCombinationRewriterConfig.PlanRewriterStep
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanCombinationRewriterConfig.PlanRewriterStepConfig
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.inSequence

import java.time.Clock
import java.time.ZoneOffset

import scala.util.Random

/*
 * Rewriters that live here are used to create test plan combinations that should not affect the result of
 * executing a plan.
 */
case object TestPlanCombinationRewriter {

  val VERBOSE = true

  def description: String = "Test plan combination rewriter"

  def apply(
    config: TestPlanCombinationRewriterConfig,
    query: LogicalQuery,
    parallelExecution: Boolean,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator
  ): LogicalQuery = {
    if (VERBOSE) {
      println(config)
    }

    val inputPlan = query.logicalPlan

    if (config.hints.contains(NoRewrites)) {
      if (VERBOSE) {
        println(
          s"""Input test plan:
             |$inputPlan
             |
             |Not rewritten
             |""".stripMargin
        )
      }
      return query
    }

    val planRewriterContext = PlanRewriterContext(
      config,
      query.effectiveCardinalities,
      query.providedOrders,
      query.leveragedOrders,
      parallelExecution,
      anonymousVariableNameGenerator,
      query.idGen
    )
    val toRewriterConverter: PlanRewriterStep => Seq[Rewriter] = planRewriterStepToRewriter(planRewriterContext)

    val preRewriters = config.preSteps.flatMap(toRewriterConverter)
    val middleRewriters = config.middleSteps.flatMap(toRewriterConverter)
    val postRewriters = config.postSteps.flatMap(toRewriterConverter)

    val orderedMiddleRewriters =
      if (config.randomizeMiddleStepOrdering) {
        Random.shuffle(middleRewriters)
      } else {
        middleRewriters
      }

    val orderedRewriters = preRewriters ++ orderedMiddleRewriters ++ postRewriters
    val rewrittenPlan = inputPlan.endoRewrite(inSequence(orderedRewriters: _*))

    if (VERBOSE) {
      println(
        s"""Input test plan:
           |$inputPlan
           |
           |Rewritten to:
           |$rewrittenPlan
           |""".stripMargin
      )
    }

    query.copy(logicalPlan = rewrittenPlan)
  }

  private def planRewriterStepToRewriter(planRewriterContext: PlanRewriterContext)(step: PlanRewriterStep)
    : Seq[Rewriter] = {
    val clazz = step.rewriter
    if (step.config.repetitions >= 1) {
      Seq.fill(step.config.repetitions)(clazz.getDeclaredConstructor(
        classOf[PlanRewriterContext],
        classOf[PlanRewriterStepConfig]
      ).newInstance(planRewriterContext, step.config))
    } else {
      Seq.empty[Rewriter]
    }
  }

  sealed trait TestPlanCombinationRewriterHint
  case object NoEager extends TestPlanCombinationRewriterHint
  case object NoRewrites extends TestPlanCombinationRewriterHint
}

object TestPlanCombinationRewriterConfig {

  /**
   * @param repetitions the number of repeated occurrences of a rewriter in the sequence of all rewriters
   * @param weight between 0.0 and 1.0. The chance of application at each applicable plan tree node
   */
  case class PlanRewriterStepConfig(repetitions: Int = 1, weight: Double = 1.0)

  case class PlanRewriterStep(rewriter: Class[_ <: Rewriter], config: PlanRewriterStepConfig) {

    override def toString: String = {
      s"${getClass.getSimpleName}(classOf[${rewriter.getSimpleName}], repetitions=${config.repetitions}, weight=${config.weight}),"
    }
  }

  object PlanRewriterStep {

    def apply(rewriter: Class[_ <: Rewriter], repetitions: Int = 1, weight: Double = 1.0): PlanRewriterStep = {
      PlanRewriterStep(rewriter, PlanRewriterStepConfig(repetitions, weight))
    }
  }

  def defaultSteps: Seq[PlanRewriterStep] = Seq(
    // PlanRewriterStep(classOf[CollectUnwindOnTop], repetitions = 1, weight = 1.0),
    PlanRewriterStep(classOf[ApplyUnwindLimitOnTop], repetitions = 1, weight = 1.0),
    PlanRewriterStep(classOf[LhsOfCartesianProductOnTop], repetitions = 1, weight = 1.0),
    PlanRewriterStep(classOf[ApplyOnTop], repetitions = 1, weight = 1.0),
    PlanRewriterStep(classOf[RhsOfCartesianProductOnTop], repetitions = 1, weight = 1.0),
    PlanRewriterStep(classOf[LimitEverywhere], repetitions = 1, weight = 0.2),
    PlanRewriterStep(classOf[SkipEverywhere], repetitions = 1, weight = 0.2),
    PlanRewriterStep(classOf[FilterEverywhere], repetitions = 1, weight = 0.2),
    PlanRewriterStep(classOf[UnwindEverywhere], repetitions = 1, weight = 0.2),
    PlanRewriterStep(classOf[NonFuseableEverywhere], repetitions = 1, weight = 0.1),
    PlanRewriterStep(classOf[LimitOnTop], repetitions = 1, weight = 1.0)
  )

  def cartesianProductNoUpdatingRhs: Seq[PlanRewriterStep] = defaultSteps.filterNot(r =>
    r.rewriter.isAssignableFrom(classOf[RhsOfCartesianProductOnTop]) ||
      r.rewriter.isAssignableFrom(
        classOf[LhsOfCartesianProductOnTop]
      )
  ) ++ Seq(
    PlanRewriterStep(classOf[LhsOfCartesianProductOnTopNoUpdatingRhs], repetitions = 1, weight = 1.0),
    PlanRewriterStep(classOf[RhsOfCartesianProductOnTopNoUpdatingRhs], repetitions = 1, weight = 1.0)
  )

  def default: TestPlanCombinationRewriterConfig = {
    TestPlanCombinationRewriterConfig(
      preSteps = Seq(
        // Executed first (non-randomized)
        PlanRewriterStep(classOf[ApplyOnTop], repetitions = 1, weight = 1.0)
      ),
      middleSteps = defaultSteps ++ Seq(
        // Executed in randomized order
      ),
      postSteps = Seq(
        // Executed last (non-randomized)
        PlanRewriterStep(classOf[LimitOnTop], repetitions = 1, weight = 1.0)
      )
    )
  }

  def pipelined: TestPlanCombinationRewriterConfig = {
    TestPlanCombinationRewriterConfig(
      preSteps = default.preSteps,
      middleSteps = cartesianProductNoUpdatingRhs,
      postSteps = default.postSteps
    )

  }
}

case class TestPlanCombinationRewriterConfig(
  preSteps: Seq[PlanRewriterStep] = Seq.empty[PlanRewriterStep],
  middleSteps: Seq[PlanRewriterStep],
  postSteps: Seq[PlanRewriterStep] = Seq.empty[PlanRewriterStep],
  seed: Option[Long] = None,
  randomizeMiddleStepOrdering: Boolean = true,
  hints: Set[TestPlanCombinationRewriterHint] = Set.empty[TestPlanCombinationRewriterHint]
) {
  val _seed: Long = seed.getOrElse(Clock.system(ZoneOffset.UTC).millis())
  Random.setSeed(_seed)

  override def toString: String = {
    val nl = System.lineSeparator()
    s"""TestPlanCombinationRewriterConfig(
       |  seed = Some(${_seed}L),
       |  randomizeMiddleStepOrdering = $randomizeMiddleStepOrdering,
       |  ${if (preSteps.nonEmpty) s"preSteps = Seq(${preSteps.mkString(s"$nl    ", s"$nl    ", "")}$nl  )," else ""}
       |  ${if (middleSteps.nonEmpty) s"middleSteps = Seq(${middleSteps.mkString(s"$nl    ", s"$nl    ", "")}$nl  ),"
      else ""}
       |  ${if (postSteps.nonEmpty) s"postSteps = Seq(${postSteps.mkString(s"$nl    ", s"$nl    ", "")}$nl  )" else ""}
       |  ${if (hints.nonEmpty) s"hints = Set(${hints.mkString(s"$nl    ", s"$nl    ", "")}$nl  )" else ""}
       |)""".stripMargin
  }

  def withHints(hints: Set[TestPlanCombinationRewriterHint]): TestPlanCombinationRewriterConfig = {
    this.copy(hints = this.hints.union(hints))
  }

  def withSeed(seed: Long): TestPlanCombinationRewriterConfig = {
    this.copy(seed = Some(seed))
  }
}

case class PlanRewriterContext(
  config: TestPlanCombinationRewriterConfig,
  effectiveCardinalities: EffectiveCardinalities,
  providedOrders: ProvidedOrders,
  leveragedOrders: LeveragedOrders,
  parallelExecution: Boolean,
  anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
  idGen: IdGen
) {

  def copyAttributes(source: LogicalPlan, target: LogicalPlan): Unit = {
    effectiveCardinalities.copy(source.id, target.id)
    providedOrders.copy(source.id, target.id)
    leveragedOrders.copy(source.id, target.id)
  }
}

object PlanRewriterContext {
  val pos: InputPosition = InputPosition.NONE
}
