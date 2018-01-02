/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v2_3

import org.neo4j.cypher.internal.frontend.v2_3.ast.ASTNode

object SemanticCheckResult {
  val success: SemanticCheck = SemanticCheckResult(_, Vector())
  def error(state: SemanticState, error: SemanticError): SemanticCheckResult = SemanticCheckResult(state, Vector(error))
  def error(state: SemanticState, error: Option[SemanticError]): SemanticCheckResult = SemanticCheckResult(state, error.toVector)
}

case class SemanticCheckResult(state: SemanticState, errors: Seq[SemanticError])

trait SemanticChecking {
  protected def when(condition: Boolean)(check: => SemanticCheck): SemanticCheck = state =>
    if (condition)
      check(state)
    else
      SemanticCheckResult.success(state)

  private val pushStateScope: SemanticCheck = state => SemanticCheckResult.success(state.newChildScope)
  private val popStateScope: SemanticCheck = state => SemanticCheckResult.success(state.popScope)
  protected def withScopedState(check: => SemanticCheck): SemanticCheck = pushStateScope chain check chain popStateScope

  protected def noteScope(astNode: ASTNode): SemanticCheck =
    state => SemanticCheckResult.success(state.noteCurrentScope(astNode))
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
