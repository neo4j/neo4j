/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_3.helpers.rewriting

import org.neo4j.cypher.internal.frontend.v3_3._

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
