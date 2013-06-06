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
package org.neo4j.cypher.internal.parser.experimental

trait SemanticCheckable {
  def semanticCheck: SemanticCheck
}

case class SemanticCheckableOption[A <: SemanticCheckable](option: Option[A]) {
  def semanticCheck = option.fold(SemanticCheckResult.success) { _.semanticCheck }
}

case class SemanticCheckableTraversableOnce[A <: SemanticCheckable](traversable: TraversableOnce[A]) {
  def semanticCheck = {
    traversable.foldLeft(SemanticCheckResult.success) { (f, o) => f >>= o.semanticCheck }
  }
}


object SemanticCheckResult {
  def success : SemanticCheck = SemanticCheckResult(_, Vector())
  def error(state: SemanticState, error: SemanticError) : SemanticCheckResult = new SemanticCheckResult(state, Vector(error))
  def error(state: SemanticState, error: Option[SemanticError]) : SemanticCheckResult = new SemanticCheckResult(state, error.toVector)
}
case class SemanticCheckResult(state: SemanticState, errors: Seq[SemanticError])


case class ChainableSemanticCheck(func: SemanticCheck) {
  def >>=(next: SemanticCheck) : SemanticCheck = state => {
    val r1 = func(state)
    val r2 = next(r1.state)
    SemanticCheckResult(r2.state, r1.errors ++ r2.errors)
  }
}
