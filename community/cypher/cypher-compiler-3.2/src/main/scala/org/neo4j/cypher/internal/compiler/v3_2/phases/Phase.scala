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

import org.neo4j.cypher.internal.frontend.v3_2.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.v3_2.phases.CompilationPhaseTracer.CompilationPhase.PIPE_BUILDING
import org.neo4j.cypher.internal.frontend.v3_2.helpers.{AssertionRunner, closing}
import org.neo4j.cypher.internal.frontend.v3_2.InternalException
import org.neo4j.cypher.internal.frontend.v3_2.helpers.AssertionRunner.Thunk
import org.neo4j.cypher.internal.frontend.v3_2.phases.BaseContext

import scala.reflect.ClassTag

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

  override def postConditions: Set[Condition] = Set.empty
}

trait Transformer[-C <: BaseContext, FROM, TO] {
  def transform(from: FROM, context: C): TO

  def andThen[D <: C, TO2](other: Transformer[D, TO, TO2]): Transformer[D, FROM, TO2] =
    new PipeLine(this, other)

  def adds[T: ClassTag](implicit manifest: Manifest[T]): Transformer[C, FROM, TO] = this andThen AddCondition[C, TO](Contains[T])

  def name: String
}

case class AddCondition[-C <: BaseContext, STATE](postCondition: Condition) extends Phase[C, STATE, STATE] {
  override def phase: CompilationPhase = PIPE_BUILDING

  override def description: String = "adds a condition"

  override def process(from: STATE, context: C): STATE = from

  override def postConditions: Set[Condition] = Set(postCondition)
}

object Transformer {
  val identity = new Transformer[BaseContext, Unit, Unit] {
    override def transform(from: Unit, context: BaseContext) = ()

    override def name: String = "identity"
  }
}

class PipeLine[-C <: BaseContext, FROM, MID, TO](first: Transformer[C, FROM, MID], after: Transformer[C, MID, TO]) extends Transformer[C, FROM, TO] {

  override def transform(from: FROM, context: C): TO = {
    val step1 = first.transform(from, context)

    // Checking conditions inside assert so they are not run in production
    ifAssertionsEnabled(accumulateAndCheckConditions(step1, first))
    val step2 = after.transform(step1, context)
    ifAssertionsEnabled(accumulateAndCheckConditions(step2, after))

    step2
  }

  private def accumulateAndCheckConditions[D <: C, STATE](from: STATE, transformer: Transformer[D, _, _]): Unit = {
    (from, transformer) match {
      case (f: CompilationState, phase: Phase[_, _, _]) =>
        val conditions = f.accumulatedConditions ++ phase.postConditions
        val messages = conditions.flatMap(condition => condition.check(f))
        if (messages.nonEmpty) {
          throw new InternalException(messages.mkString(", "))
        }
      case _ =>
    }
  }

  override def name: String = first.name + ", " + after.name

  private def ifAssertionsEnabled(f: => Unit): Unit = {
    AssertionRunner.runUnderAssertion(new Thunk {
      override def apply() = f
    })
  }
}

case class If[-C <: BaseContext, STATE](f: STATE => Boolean)(thenT: Transformer[C, STATE, STATE]) extends Transformer[C, STATE, STATE] {
  override def transform(from: STATE, context: C): STATE = {
    if (f(from))
      thenT.transform(from, context)
    else
      from
  }

  override def name: String = s"if(<f>) ${thenT.name}"
}

object Do {
  def apply[C <: BaseContext, STATE](voidFunction: C => Unit) = new Do[C, STATE, STATE]((from, context) => {
    voidFunction(context)
    from
  })
}

case class Do[-C <: BaseContext, FROM, TO](f: (FROM, C) => TO) extends Transformer[C, FROM, TO] {
  override def transform(from: FROM, context: C): TO =
    f(from, context)

  override def name: String = "do <f>"
}
