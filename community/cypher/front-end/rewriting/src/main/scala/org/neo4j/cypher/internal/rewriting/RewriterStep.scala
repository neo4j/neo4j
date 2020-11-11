/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.util.AssertionRunner
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Step

object RewritingStepSequencer extends StepSequencer(new StepSequencer.StepAccumulator[RewritingStep, Seq[RewritingStep]] {
  override def empty: Seq[RewritingStep] = Seq.empty
  override def addNext(acc: Seq[RewritingStep], step: RewritingStep): Seq[RewritingStep] = acc :+ step
})

trait RewritingStep extends Rewriter with Step {
  override def toString(): String = this.getClass.getSimpleName

  final override def apply(that: AnyRef): AnyRef = {
    val result = rewrite(that)
    if (AssertionRunner.isAssertionsEnabled) {
      validate(result)
    }
    result
  }

  private def validate(input: AnyRef): Unit = {
    val failures = postConditions.collect {
      case f:ValidatingCondition => f.name -> f(input)
    }
    if (failures.exists(_._2.nonEmpty)) {
      throw new IllegalStateException(buildErrorMessage(failures))
    }
  }

  private def buildErrorMessage(failures: Set[(String, Seq[String])]) = {
    val builder = new StringBuilder
    builder ++= s"Error during rewriting after ${toString()}. The following conditions where violated: \n"
    for {
      (condition, failure) <- failures
      problem <- failure
    } {
      builder ++= s"Condition '${condition}' violated. $problem\n"
    }
    builder.toString()
  }

  def rewrite(that:AnyRef): AnyRef
}

object RewriterStep {
  implicit def namedProductRewriter(p: Product with Rewriter): ApplyRewriter = ApplyRewriter(p.productPrefix, p)
}

sealed trait RewriterStep
final case class ApplyRewriter(name: String, rewriter: Rewriter) extends RewriterStep

trait ValidatingCondition extends (Any => Seq[String]) with StepSequencer.Condition {
   def name: String
}
