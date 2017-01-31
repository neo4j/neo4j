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
package org.neo4j.cypher.internal.compiler.v3_2.phases

import org.neo4j.cypher.internal.compiler.v3_2.AssertionRunner
import org.neo4j.cypher.internal.compiler.v3_2.AssertionRunner.Thunk
import org.neo4j.cypher.internal.compiler.v3_2.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.compiler.v3_2.CompilationPhaseTracer.CompilationPhase.PIPE_BUILDING
import org.neo4j.cypher.internal.compiler.v3_2.helpers.closing
import org.neo4j.cypher.internal.frontend.v3_2.InternalException

import scala.reflect.ClassTag

/*
A phase is a leaf component of the tree structure that is the compilation pipe line.
It passes through the compilation state, and might add values to it
 */
trait Phase[-C <: BaseContext] extends Transformer[C] {
  self =>

  def phase: CompilationPhase

  def description: String

  override def transform(from: CompilationState, context: C): CompilationState =
    closing(context.tracer.beginPhase(phase)) {
      process(from, context)
    }

  def process(from: CompilationState, context: C): CompilationState

  def postConditions: Set[Condition]

  def name = getClass.getSimpleName
}

/*
A visitor is a phase that does not change the compilation state. All it's behaviour is side effects
 */
trait VisitorPhase[-C <: BaseContext] extends Phase[C] {
  override def process(from: CompilationState, context: C): CompilationState = {
    visit(from, context)
    from
  }

  def visit(value: CompilationState, context: C): Unit

  override def postConditions: Set[Condition] = Set.empty
}

trait Transformer[-C <: BaseContext] {
  def transform(from: CompilationState, context: C): CompilationState

  def andThen[D <: C](other: Transformer[D]): Transformer[D] =
    new PipeLine(this, other)

  def adds[T: ClassTag](implicit manifest: Manifest[T]): Transformer[C] = this andThen AddCondition[C](Contains[T])

  def name: String
}

case class AddCondition[-C <: BaseContext](postCondition: Condition) extends Phase[C] {
  override def phase: CompilationPhase = PIPE_BUILDING

  override def description: String = "adds a condition"

  override def process(from: CompilationState, context: C): CompilationState = from

  override def postConditions: Set[Condition] = Set(postCondition)
}

object Transformer {
  val identity = new Transformer[BaseContext] {
    override def transform(from: CompilationState, context: BaseContext) = from

    override def name: String = "identity"
  }
}

class PipeLine[-C <: BaseContext](first: Transformer[C], after: Transformer[C]) extends Transformer[C] {

  override def transform(from: CompilationState, context: C): CompilationState = {
    var step = first.transform(from, context)

    // Checking conditions inside assert so they are not run in production
    ifAssertionsEnabled({ step = accumulateAndCheckConditions(step, first) })
    step = after.transform(step, context)
    ifAssertionsEnabled({ step = accumulateAndCheckConditions(step, after) })

    step
  }

  private def accumulateAndCheckConditions[D <: C](from: CompilationState, transformer: Transformer[D]): CompilationState = {
    // Checking conditions inside assert so they are not run in production
    val result = transformer match {
      case phase: Phase[_] => from.copy(accumulatedConditions = from.accumulatedConditions ++ phase.postConditions)
      case _ => from
    }

    val messages = result.accumulatedConditions.flatMap(condition => condition.check(result))
    if (messages.nonEmpty) {
      throw new InternalException(messages.mkString(", "))
    }

    result
  }
  private def addConditions[D <: C](state: CompilationState, transformer: Transformer[D]): CompilationState = {
    transformer match {
      case phase: Phase[_] => state.copy(accumulatedConditions = state.accumulatedConditions ++ phase.postConditions)
      case _ => state
    }
  }

  override def name: String = first.name + ", " + after.name

  private def ifAssertionsEnabled(f: => Unit): Unit = {
    AssertionRunner.runUnderAssertion(new Thunk {
      override def apply() = f
    })
  }
}

case class If[-C <: BaseContext](f: CompilationState => Boolean)(thenT: Transformer[C]) extends Transformer[C] {
  override def transform(from: CompilationState, context: C): CompilationState = {
    if (f(from))
      thenT.transform(from, context)
    else
      from
  }

  override def name: String = s"if(<f>) ${thenT.name}"
}

object Do {
  def apply[C <: BaseContext](voidFunction: C => Unit) = new Do[C]((from, context) => {
    voidFunction(context)
    from
  })
}

case class Do[-C <: BaseContext](f: (CompilationState, C) => CompilationState) extends Transformer[C] {
  override def transform(from: CompilationState, context: C): CompilationState =
    f(from, context)

  override def name: String = "do <f>"
}
