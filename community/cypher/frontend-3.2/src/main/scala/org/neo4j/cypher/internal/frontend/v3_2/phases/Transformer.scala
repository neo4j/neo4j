package org.neo4j.cypher.internal.frontend.v3_2.phases

import org.neo4j.cypher.internal.frontend.v3_2.InternalException
import org.neo4j.cypher.internal.frontend.v3_2.helpers.AssertionRunner
import org.neo4j.cypher.internal.frontend.v3_2.helpers.AssertionRunner.Thunk

trait Transformer[-C <: BaseContext, -FROM, TO] {
  def transform(from: FROM, context: C): TO

  def andThen[D <: C, TO2](other: Transformer[D, TO, TO2]): Transformer[D, FROM, TO2] =
    new PipeLine(this, other)

  def adds(condition: Condition): Transformer[C, FROM, TO] = this andThen AddCondition[C, TO](condition)

  def name: String
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
      case (f: BaseState, phase: Phase[_, _, _]) =>
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


case class If[-C <: BaseContext, FROM, STATE <: FROM](f: STATE => Boolean)(thenT: Transformer[C, FROM, STATE]) extends Transformer[C, STATE, STATE] {
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
