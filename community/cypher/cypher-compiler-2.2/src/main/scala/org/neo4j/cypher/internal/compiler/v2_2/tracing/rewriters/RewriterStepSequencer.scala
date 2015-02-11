/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.tracing.rewriters

import org.neo4j.cypher.internal.compiler.v2_2._

import collection.mutable

case class RewriterContract(childRewriters: Seq[Rewriter], postConditions: Set[RewriterCondition] = Set.empty) {
  val rewriter = inSequence(childRewriters: _*)
}

object RewriterStepSequencer {
  def newDefault(sequenceName: String): RewriterStepSequencer =
    if (getClass.desiredAssertionStatus())
      newValidating(sequenceName)
    else
      newPlain(sequenceName)

  def newPlain(sequenceName: String) =
    PlainRewriterStepSequencer(sequenceName, DefaultRewriterTaskProcessor(sequenceName))

  def newValidating(sequenceName: String) =
    ValidatingRewriterStepSequencer(sequenceName, DefaultRewriterTaskProcessor(sequenceName))

  def newTracing(sequenceName: String, onlyIfChanged: Boolean = true) =
    ValidatingRewriterStepSequencer(sequenceName, TracingRewriterTaskProcessor(sequenceName, onlyIfChanged))
}

trait RewriterStepSequencer extends mutable.Builder[RewriterStep, RewriterContract] {

  self =>

  private var _steps: mutable.Builder[RewriterStep, Seq[RewriterStep]] = Seq.newBuilder

  def withPrecondition(conditions: Set[RewriterCondition]) = {
    conditions.foldLeft(self) {
      case (acc, cond) => acc += EnableRewriterCondition(cond)
    }
  }

  def apply(steps: RewriterStep*) = {
    steps.foldLeft(self) {
      case (acc, step) => acc += step
    }.result()
  }

  override def +=(elem: RewriterStep): this.type = {
    _steps += elem
    self
  }

  def result() = result(_steps.result())

  protected def result(steps: Seq[RewriterStep]): RewriterContract

  def clear(): Unit = {
    _steps.clear()
  }
}

case class PlainRewriterStepSequencer(sequenceName: String, taskProcessor: RewriterTaskProcessor) extends RewriterStepSequencer {

  protected def result(steps: Seq[RewriterStep]): RewriterContract = {
    val tasks = steps.collect { case ApplyRewriter(name, rewriter) => RunRewriter(name, rewriter) }
    val rewriters = tasks.map(taskProcessor)
    RewriterContract(rewriters)
  }
}

case class ValidatingRewriterStepSequencer(sequenceName: String, taskProcessor: RewriterTaskProcessor) extends RewriterStepSequencer {

  protected def result(steps: Seq[RewriterStep]): RewriterContract = {
    val tasks = RewriterTaskBuilder(steps)
    val rewriters = tasks.map(taskProcessor)
    val postConditions: Set[RewriterCondition] = tasks.lastOption match {
      case Some(task: RunConditions) => task.conditions
      case _                         => Set.empty
    }
    RewriterContract(rewriters, postConditions)
  }
}


