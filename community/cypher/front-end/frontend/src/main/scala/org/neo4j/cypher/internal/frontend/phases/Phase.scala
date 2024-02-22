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

import org.neo4j.cypher.internal.frontend.helpers.closing
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.macros.AssertMacros.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.StepSequencer

/*
A phase is a leaf component of the tree structure that is the compilation pipe line.
It passes through the compilation state, and might add values to it
 */
trait Phase[-C <: BaseContext, FROM, +TO] extends Transformer[C, FROM, TO] {
  self: Product =>

  def phase: CompilationPhase

  override def transform(from: FROM, context: C): TO = {
    context.cancellationChecker.throwIfCancelled()
    closing(context.tracer.beginPhase(phase)) {
      val result = process(from, context)
      // Checking conditions inside assert so they are not run in production
      checkOnlyWhenAssertionsAreEnabled(checkConditions(result, postConditions)(context.cancellationChecker))
      result
    }
  }

  def process(from: FROM, context: C): TO

  def name: String = productPrefix
}

/*
A visitor is a phase that does not change the compilation state. All it's behaviour is side effects
 */
trait VisitorPhase[-C <: BaseContext, STATE] extends Phase[C, STATE, STATE] {
  self: Product =>

  override def process(from: STATE, context: C): STATE = {
    visit(from, context)
    from
  }

  def visit(value: STATE, context: C): Unit

  override def postConditions: Set[StepSequencer.Condition] = Set.empty
}
