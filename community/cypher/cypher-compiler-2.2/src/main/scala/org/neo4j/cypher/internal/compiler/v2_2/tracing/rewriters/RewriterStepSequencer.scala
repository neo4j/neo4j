/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

object RewriterStepSequencer {

  def newDefault(sequenceName: String): RewriterStepSequencer = newValidating(sequenceName)

  def newPlain(sequenceName: String): PlainRewriterStepSequencer =
    PlainRewriterStepSequencer(sequenceName, DefaultRewriterTaskProcessor(sequenceName))

  def newValidating(sequenceName: String): ValidatingRewriterStepSequencer =
    ValidatingRewriterStepSequencer(sequenceName, DefaultRewriterTaskProcessor(sequenceName))

  def newTracing(sequenceName: String, onlyIfChanged: Boolean = true): ValidatingRewriterStepSequencer =
    ValidatingRewriterStepSequencer(sequenceName, TracingRewriterTaskProcessor(sequenceName, onlyIfChanged))
}

trait RewriterStepSequencer {
  def fromStepSeq(steps: Seq[RewriterStep]): Seq[Rewriter]

  def fromSteps(steps: RewriterStep*): Rewriter =
    inSequence(fromStepSeq(steps): _*)
}

case class PlainRewriterStepSequencer(sequenceName: String, taskProcessor: RewriterTaskProcessor) extends RewriterStepSequencer {
  def fromStepSeq(steps: Seq[RewriterStep]): Seq[Rewriter] = {
    val tasks = steps.collect { case ApplyRewriter(name, rewriter) => RunRewriter(name, rewriter) }
    val rewriters = tasks.map(taskProcessor)
    rewriters
  }
}

case class ValidatingRewriterStepSequencer(sequenceName: String, taskProcessor: RewriterTaskProcessor) extends RewriterStepSequencer {
  def fromStepSeq(steps: Seq[RewriterStep]): Seq[Rewriter] = {
    val tasks = RewriterTaskBuilder(steps)
    val rewriters = tasks.map(taskProcessor)
    rewriters
  }
}

