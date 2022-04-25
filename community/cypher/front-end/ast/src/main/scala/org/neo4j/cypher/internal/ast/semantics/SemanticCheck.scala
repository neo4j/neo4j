/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.ast.semantics

import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.when
import org.neo4j.cypher.internal.expressions.Expression.SemanticContext
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.NotImplementedErrorMessageProvider

sealed trait SemanticCheck {

  @deprecated(message = "Use `run` instead", since = "5.0")
  def apply(state: SemanticState): SemanticCheckResult = {
    run(state, SemanticCheckContext.default)
  }

  def run(state: SemanticState, context: SemanticCheckContext): SemanticCheckResult = {
    SemanticCheckInterpreter.runCheck(this, state, context)
  }

  def chain(next: SemanticCheck): SemanticCheck = {
    for {
      a <- this
      b <- next
    } yield SemanticCheckResult(b.state, a.errors ++ b.errors)
  }

  def ifOkChain(next: => SemanticCheck): SemanticCheck = {
    for {
      a <- this
      b <- when(a.errors.isEmpty)(next)
    } yield SemanticCheckResult(b.state, a.errors ++ b.errors)
  }

  def map(f: SemanticCheckResult => SemanticCheckResult): SemanticCheck = SemanticCheck.Map(this, f)
  def flatMap(f: SemanticCheckResult => SemanticCheck): SemanticCheck = SemanticCheck.FlatMap(this, f)

  def annotate(annotation: => String): SemanticCheck = {
    if (SemanticCheck.DEBUG_ENABLED)
      SemanticCheck.Annotated(this, annotation)
    else
      this
  }

  def :|(annotation: => String): SemanticCheck = annotate(annotation)
  def |:(annotation: => String): SemanticCheck = annotate(annotation)
}

object SemanticCheck {

  val success: SemanticCheck = fromFunction(SemanticCheckResult.success)
  def error(error: SemanticErrorDef): SemanticCheck = fromFunction(SemanticCheckResult.error(_, error))

  def getState: SemanticCheck = success
  def setState(s: SemanticState): SemanticCheck = fromFunction(_ => SemanticCheckResult.success(s))

  def fromFunction(f: SemanticState => SemanticCheckResult): SemanticCheck = Leaf(f)
  def fromState(f: SemanticState => SemanticCheck): SemanticCheck = success.flatMap(res => f(res.state))
  def fromContext(f: SemanticCheckContext => SemanticCheck): SemanticCheck = CheckFromContext(f)

  def fromFunctionWithContext(f: (SemanticState, SemanticCheckContext) => SemanticCheckResult): SemanticCheck = {
    fromContext(context => fromFunction(state => f(state, context)))
  }

  def nestedCheck(check: => SemanticCheck): SemanticCheck = success.flatMap(_ => check)

  def when(condition: Boolean)(check: => SemanticCheck): SemanticCheck = {
    if (condition)
      check
    else
      SemanticCheck.success
  }

  private[semantics] val DEBUG_ENABLED = false

  final private[semantics] case class Leaf(f: SemanticState => SemanticCheckResult) extends SemanticCheck

  final private[semantics] case class Map(check: SemanticCheck, f: SemanticCheckResult => SemanticCheckResult)
      extends SemanticCheck

  final private[semantics] case class FlatMap(check: SemanticCheck, f: SemanticCheckResult => SemanticCheck)
      extends SemanticCheck

  final private[semantics] case class CheckFromContext(f: SemanticCheckContext => SemanticCheck) extends SemanticCheck
  final private[semantics] case class Annotated(check: SemanticCheck, annotation: String) extends SemanticCheck
}

final case class SemanticCheckResult(state: SemanticState, errors: Seq[SemanticErrorDef])

object SemanticCheckResult {
  def success(s: SemanticState): SemanticCheckResult = SemanticCheckResult(s, Vector.empty)

  def error(state: SemanticState, error: SemanticErrorDef): SemanticCheckResult =
    SemanticCheckResult(state, Vector(error))

  def error(state: SemanticState, msg: String, position: InputPosition): SemanticCheckResult =
    error(state, SemanticError(msg, position))

  def error(state: SemanticState, error: Option[SemanticErrorDef]): SemanticCheckResult =
    SemanticCheckResult(state, error.toVector)
}

trait SemanticCheckContext {
  def errorMessageProvider: ErrorMessageProvider
}

object SemanticCheckContext {

  object default extends SemanticCheckContext {
    override def errorMessageProvider: ErrorMessageProvider = NotImplementedErrorMessageProvider
  }
}

class OptionSemanticChecking[A](val option: Option[A]) extends AnyVal {

  def foldSemanticCheck(check: A => SemanticCheck): SemanticCheck =
    option.fold(SemanticCheck.success)(check)
}

class TraversableOnceSemanticChecking[A](val traversable: IterableOnce[A]) extends AnyVal {

  def foldSemanticCheck(check: A => SemanticCheck): SemanticCheck = {
    traversable.iterator.foldLeft(SemanticCheck.success) {
      (accCheck, o) => accCheck chain check(o)
    }
  }
}

trait SemanticCheckable {
  def semanticCheck: SemanticCheck
}

trait SemanticCheckableExpression {
  def semanticCheck(ctx: SemanticContext): SemanticCheck
}

class SemanticCheckableOption[A <: SemanticCheckable](val option: Option[A]) extends AnyVal {
  def semanticCheck: SemanticCheck = option.fold(SemanticCheck.success) { _.semanticCheck }
}

class SemanticCheckableTraversableOnce[A <: SemanticCheckable](val traversable: IterableOnce[A]) extends AnyVal {
  def semanticCheck: SemanticCheck = traversable.foldSemanticCheck { _.semanticCheck }
}
