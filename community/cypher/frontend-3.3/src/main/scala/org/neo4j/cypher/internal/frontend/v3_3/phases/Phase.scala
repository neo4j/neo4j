/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_3.phases

import org.neo4j.cypher.internal.frontend.v3_3.helpers.closing
import org.neo4j.cypher.internal.frontend.v3_3.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.v3_3.phases.CompilationPhaseTracer.CompilationPhase.PIPE_BUILDING

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
