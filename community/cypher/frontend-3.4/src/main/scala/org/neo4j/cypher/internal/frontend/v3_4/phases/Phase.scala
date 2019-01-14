/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.frontend.v3_4.phases

import org.neo4j.cypher.internal.frontend.v3_4.helpers.closing
import org.neo4j.cypher.internal.frontend.v3_4.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.v3_4.phases.CompilationPhaseTracer.CompilationPhase.PIPE_BUILDING

/*
A phase is a leaf component of the tree structure that is the compilation pipe line.
It passes through the compilation state, and might add values to it
 */
trait Phase[-C <: BaseContext, FROM, TO] extends Transformer[C, FROM, TO] {
  self =>

  def phase: CompilationPhase

  def description: String

  override def transform(from: FROM, context: C): TO =
    closing(context.tracer.beginPhase(phase)) {
      process(from, context)
    }

  def process(from: FROM, context: C): TO

  def postConditions: Set[Condition]

  def name = getClass.getSimpleName
}

/*
A visitor is a phase that does not change the compilation state. All it's behaviour is side effects
 */
trait VisitorPhase[-C <: BaseContext, STATE] extends Phase[C, STATE, STATE] {
  override def process(from: STATE, context: C): STATE = {
    visit(from, context)
    from
  }

  def visit(value: STATE, context: C): Unit

  override def postConditions = Set.empty
}

case class AddCondition[-C <: BaseContext, STATE](postCondition: Condition) extends Phase[C, STATE, STATE] {
  override def phase: CompilationPhase = PIPE_BUILDING

  override def description: String = "adds a condition"

  override def process(from: STATE, context: C): STATE = from

  override def postConditions = Set(postCondition)
}
