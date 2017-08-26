/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_3

object SemanticCheckResult {
  val success: SemanticCheck = SemanticCheckResult(_, Vector())
  def error(state: SemanticState, error: SemanticErrorDef): SemanticCheckResult = SemanticCheckResult(state, Vector(error))
  def error(state: SemanticState, error: Option[SemanticErrorDef]): SemanticCheckResult = SemanticCheckResult(state, error.toVector)
}

case class SemanticCheckResult(state: SemanticState, errors: Seq[SemanticErrorDef])

trait SemanticChecking {

  protected def requireMultigraphSupport(position: InputPosition): SemanticCheck = {
    val error: SemanticCheck = FeatureError("Projecting / returning graphs is not supported by Neo4j", position)
    error.unlessFeatureEnabled('multigraph)
  }

  protected def when(condition: Boolean)(check: => SemanticCheck): SemanticCheck = state =>
    if (condition)
      check(state)
    else
      SemanticCheckResult.success(state)

  private val pushStateScope: SemanticCheck = state => SemanticCheckResult.success(state.newChildScope)
  private val popStateScope: SemanticCheck = state => SemanticCheckResult.success(state.popScope)
  protected def withScopedState(check: => SemanticCheck): SemanticCheck =
    pushStateScope chain check chain popStateScope
}


class OptionSemanticChecking[A](val option: Option[A]) extends AnyVal {
  def foldSemanticCheck(check: A => SemanticCheck): SemanticCheck =
    option.fold(SemanticCheckResult.success)(check)
}


class TraversableOnceSemanticChecking[A](val traversable: TraversableOnce[A]) extends AnyVal {
  def foldSemanticCheck(check: A => SemanticCheck): SemanticCheck = state => traversable.foldLeft(SemanticCheckResult.success(state)) {
    (r1, o) =>
      val r2 = check(o)(r1.state)
      SemanticCheckResult(r2.state, r1.errors ++ r2.errors)
  }
}

class ChainableSemanticCheck(val check: SemanticCheck) extends AnyVal {
  def chain(next: SemanticCheck): SemanticCheck = state => {
    val r1 = check(state)
    val r2 = next(r1.state)
    SemanticCheckResult(r2.state, r1.errors ++ r2.errors)
  }

  def ifOkChain(next: => SemanticCheck): SemanticCheck = state => {
    val r1 = check(state)
    if (r1.errors.nonEmpty)
      r1
    else
      next(r1.state)
  }
}

trait SemanticCheckable {
  def semanticCheck: SemanticCheck
}

class SemanticCheckableOption[A <: SemanticCheckable](val option: Option[A]) extends AnyVal {
  def semanticCheck: SemanticCheck = option.fold(SemanticCheckResult.success) { _.semanticCheck }
}

class SemanticCheckableTraversableOnce[A <: SemanticCheckable](val traversable: TraversableOnce[A]) extends AnyVal {
  def semanticCheck: SemanticCheck = traversable.foldSemanticCheck { _.semanticCheck }
}
