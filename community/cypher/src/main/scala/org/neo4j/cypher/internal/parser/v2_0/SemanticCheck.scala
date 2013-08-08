/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.parser.v2_0

trait SemanticCheckable {
  def semanticCheck: SemanticCheck
}

case class SemanticCheckableOption[A <: SemanticCheckable](option: Option[A]) {
  def semanticCheck = option.fold(SemanticCheckResult.success) { _.semanticCheck }
}

case class SemanticCheckableTraversableOnce[A <: SemanticCheckable](traversable: TraversableOnce[A]) {
  def semanticCheck = {
    traversable.foldLeft(SemanticCheckResult.success) { (f, o) => f then o.semanticCheck }
  }
}


object SemanticCheckResult {
  def success : SemanticCheck = SemanticCheckResult(_, Vector())
  def error(state: SemanticState, error: SemanticError) : SemanticCheckResult = new SemanticCheckResult(state, Vector(error))
  def error(state: SemanticState, error: Option[SemanticError]) : SemanticCheckResult = new SemanticCheckResult(state, error.toVector)
}
case class SemanticCheckResult(state: SemanticState, errors: Seq[SemanticError])


case class ChainableSemanticCheck(check: SemanticCheck) {
  def then(next: SemanticCheck) : SemanticCheck = state => {
    val r1 = check(state)
    val r2 = next(r1.state)
    SemanticCheckResult(r2.state, r1.errors ++ r2.errors)
  }

  def ifOkThen(next: SemanticCheck) : SemanticCheck = state => {
    val r1 = check(state)
    if (!r1.errors.isEmpty)
      r1
    else
      next(r1.state)
  }
}


trait SemanticChecking {
  protected def when(pred: Boolean)(check: => SemanticCheck) : SemanticCheck = state => {
    if (pred)
      check(state)
    else
      SemanticCheckResult.success(state)
  }

  private def scopeState : SemanticCheck = state => SemanticCheckResult.success(state.newScope)
  private def popStateScope : SemanticCheck = state => SemanticCheckResult.success(state.popScope)
  protected def withScopedState(check: => SemanticCheck) : SemanticCheck = scopeState then check then popStateScope
}
