/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Step

case class ValidatingRewriter(inner: Rewriter, step: Step, cancellationChecker: CancellationChecker) extends Rewriter {

  final override def apply(that: AnyRef): AnyRef = {
    val result = inner(that)
    validate(result)
    result
  }

  private def validate(input: AnyRef): Unit = {
    val failures = step.postConditions.collect {
      case f: ValidatingCondition => f.name -> f(input)(cancellationChecker)
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
}

object RewriterStep {

  def validatingRewriter(inner: Rewriter, step: Step, cancellationChecker: CancellationChecker): Rewriter = {
    if (AssertionRunner.isAssertionsEnabled) {
      ValidatingRewriter(inner, step, cancellationChecker)
    } else {
      inner
    }
  }
}

trait ValidatingCondition extends StepSequencer.Condition {
  def apply(a: Any)(cancellationChecker: CancellationChecker): Seq[String]
  def name: String
  override def toString(): String = name
}
