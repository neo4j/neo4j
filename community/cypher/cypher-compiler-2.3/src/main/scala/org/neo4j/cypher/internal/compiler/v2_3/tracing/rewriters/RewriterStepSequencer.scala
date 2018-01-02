/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters

import org.neo4j.cypher.internal.frontend.v2_3._

import scala.collection.mutable

case class RewriterContract(childRewriters: Seq[Rewriter], postConditions: Set[RewriterCondition]) {
  val rewriter = inSequence(childRewriters: _*)
}

object RewriterStepSequencer {
  def newPlain(sequenceName: String) =
    PlainRewriterStepSequencer(sequenceName, DefaultRewriterTaskProcessor(sequenceName))

  def newValidating(sequenceName: String) =
    ValidatingRewriterStepSequencer(sequenceName, DefaultRewriterTaskProcessor(sequenceName))

  def newTracing(sequenceName: String, onlyIfChanged: Boolean = true) =
    ValidatingRewriterStepSequencer(sequenceName, TracingRewriterTaskProcessor(sequenceName, onlyIfChanged))
}

trait RewriterStepSequencer extends ((RewriterStep *) => RewriterContract) {
  self =>

  private val _steps: mutable.Builder[RewriterStep, Seq[RewriterStep]] = Seq.newBuilder

  def withPrecondition(conditions: Set[RewriterCondition]) = {
    conditions.foldLeft(_steps) { (acc, cond) => acc += EnableRewriterCondition(cond) }
    self
  }

  def apply(steps: RewriterStep*): RewriterContract =
    internalResult(steps.foldLeft(_steps) { (acc, step) => acc += step }.result())

  protected def internalResult(steps: Seq[RewriterStep]): RewriterContract
}

case class PlainRewriterStepSequencer(sequenceName: String, taskProcessor: RewriterTaskProcessor) extends RewriterStepSequencer {

  protected def internalResult(steps: Seq[RewriterStep]): RewriterContract = {
    val rewriters = steps.collect { case ApplyRewriter(name, rewriter) => taskProcessor(RunRewriter(name, rewriter)) }
    RewriterContract(rewriters, Set.empty)
  }
}

case class ValidatingRewriterStepSequencer(sequenceName: String, taskProcessor: RewriterTaskProcessor) extends RewriterStepSequencer {

  protected def internalResult(steps: Seq[RewriterStep]): RewriterContract = {
    val tasks = RewriterTaskBuilder(steps)
    val rewriters = tasks.map(taskProcessor)
    val postConditions = tasks.lastOption.collect { case task: RunConditions => task.conditions }.getOrElse(Set.empty)
    RewriterContract(rewriters, postConditions)
  }
}


