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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.macros.AssertMacros.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.rewriting.ValidatingCondition
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.StepSequencer

trait Transformer[-C <: BaseContext, -FROM, +TO] {
  def transform(from: FROM, context: C): TO

  def andThen[D <: C, TO2](other: Transformer[D, TO, TO2]): Transformer[D, FROM, TO2] =
    new PipeLine(this, other)

  def name: String

  /**
   * @return the conditions that are guaranteed to be met after this step has run.
   */
  def postConditions: Set[StepSequencer.Condition]

  /**
   * @return the conditions that this step invalidates as a side-effect of its work.
   *         Must not intersect with postConditions.
   */
  def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  final protected[Transformer] def checkConditions(
    state: Any,
    conditions: Set[StepSequencer.Condition]
  )(cancellationChecker: CancellationChecker): Boolean =
    Transformer.checkConditions(state, conditions, name)(cancellationChecker)
}

object Transformer {

  /**
   * Transformer that can be inserted when debugging, to help detect
   * what part of the compilation introduces an ast issue.
   */
  def printAst(tag: String): Transformer[BaseContext, BaseState, BaseState] =
    new Transformer[BaseContext, BaseState, BaseState] {

      override def transform(from: BaseState, context: BaseContext): BaseState = {
        println("     |||||||| PRINT AST: " + tag)
        println(Prettifier(ExpressionStringifier()).asString(from.maybeStatement.get))
        from
      }

      override def postConditions: Set[StepSequencer.Condition] = Set.empty

      override def name: String = "print ast"
    }

  def checkConditions(
    state: Any,
    conditions: Set[StepSequencer.Condition],
    name: String
  )(cancellationChecker: CancellationChecker): Boolean = {
    val messages: Seq[String] = conditions.toSeq.collect {
      case v: ValidatingCondition => v(state)(cancellationChecker)
    }.flatten
    if (messages.nonEmpty) {
      val prefix = s"Conditions started failing after running these phases: $name\n"
      throw new IllegalStateException(prefix + messages.mkString(", "))
    }
    true
  }
}

class PipeLine[-C <: BaseContext, FROM, MID, TO](first: Transformer[C, FROM, MID], after: Transformer[C, MID, TO])
    extends Transformer[C, FROM, TO] {

  override lazy val postConditions: Set[StepSequencer.Condition] =
    first.postConditions ++
      after.postConditions --
      after.invalidatedConditions

  override lazy val invalidatedConditions: Set[StepSequencer.Condition] =
    after.invalidatedConditions ++
      first.invalidatedConditions --
      after.postConditions

  override def transform(from: FROM, context: C): TO = {
    val step1 = first.transform(from, context)
    val step2 = after.transform(step1, context)

    // We do not need to check `after.postConditions`, since executing
    // `after.transform` is already doing that.
    val conditionsToCheck = first.postConditions -- after.invalidatedConditions

    // Checking conditions inside assert so they are not run in production
    checkOnlyWhenAssertionsAreEnabled(checkConditions(step2, conditionsToCheck)(context.cancellationChecker))

    step2
  }

  override def name: String = first.name + ", " + after.name

  override def toString: String = name
}

case class If[-C <: BaseContext, FROM, STATE <: FROM](f: STATE => Boolean)(thenT: => Transformer[C, FROM, STATE])
    extends Transformer[C, STATE, STATE] {

  override def transform(from: STATE, context: C): STATE = {
    if (f(from))
      thenT.transform(from, context)
    else
      from
  }

  override def name: String = s"if(<f>) ${thenT.name}"

  override def toString: String = name

  // We cannot guarantee the postConditions of thenT, if it is never run.
  // Also we cannot check `f(state)` to determine if we should run the post-condition
  // (in a ConditionalValidatingCondition wrapper), since `state` might have changed.
  override def postConditions: Set[StepSequencer.Condition] = Set.empty

  override def invalidatedConditions: Set[StepSequencer.Condition] = thenT.invalidatedConditions
}
