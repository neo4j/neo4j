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

import org.neo4j.cypher.internal.util.v3_4.AssertionUtils.ifAssertionsEnabled
import org.neo4j.cypher.internal.util.v3_4.InternalException

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

  /**
    * Transformer that can be inserted when debugging, to help detect
    * what part of the compilation that introduces an ast issue.
    */
  def printAst(tag: String) = new Transformer[BaseContext, BaseState, BaseState] {
    override def transform(from: BaseState, context: BaseContext) = {
      println("     |||||||| PRINT AST: "+tag)
      println(from.maybeStatement.get)
      from
    }

    override def name: String = "print ast"
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
