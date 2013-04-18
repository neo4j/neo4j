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
package org.neo4j.cypher.internal.parser

import org.neo4j.cypher.internal.symbols.CypherType

package object v2_0peg {
  type SemanticCheck = SemanticState => SemanticCheckResult

  implicit def chainableSemanticCheck(check: SemanticCheck) = ChainableSemanticCheck(check)
  implicit def chainableSemanticEitherFunc(func: SemanticState => Either[SemanticError, SemanticState]) = ChainableSemanticCheck(func)
  implicit def chainableSemanticOptionFunc(func: SemanticState => Option[SemanticError]) = ChainableSemanticCheck(func)

  implicit def liftSemanticEitherFunc(func: SemanticState => Either[SemanticError, SemanticState]) : SemanticCheck = state => {
    func(state).fold(error => SemanticCheckResult.error(state, error), s => SemanticCheckResult.success(s))
  }
  implicit def liftSemanticErrorsFunc(func: SemanticState => Seq[SemanticError]) : SemanticCheck = state => {
    SemanticCheckResult(state, func(state))
  }
  implicit def liftSemanticErrorFunc(func: SemanticState => Option[SemanticError]) : SemanticCheck = state => {
	SemanticCheckResult.error(state, func(state))
  }
  implicit def liftSemanticState(state: SemanticState) : SemanticCheck = SemanticCheckResult.success
  implicit def liftSemanticErrors(errors: Seq[SemanticError]) : SemanticCheck = SemanticCheckResult(_, errors)
  implicit def liftSemanticError(error: SemanticError) : SemanticCheck = SemanticCheckResult.error(_, error)
  implicit def liftSemanticErrorOption(error: Option[SemanticError]) : SemanticCheck = SemanticCheckResult.error(_, error)
  implicit def liftSemanticStateAndChain(state: SemanticState) : ChainableSemanticCheck = liftSemanticState(state)
  implicit def liftSemanticErrorsAndChain(errors: Seq[SemanticError]) : ChainableSemanticCheck = liftSemanticErrors(errors)
  implicit def liftSemanticErrorAndChain(error: SemanticError) : ChainableSemanticCheck = liftSemanticError(error)
  implicit def liftSemanticErrorOptionAndChain(error: Option[SemanticError]) : ChainableSemanticCheck = liftSemanticErrorOption(error)

  implicit def mergeableCypherTypeSet[T <: CypherType](set: Set[T]) = MergeableCypherTypeSet(set)
  implicit def formattableCypherTypeSet[T <: CypherType](set: Set[T]) = FormattableCypherTypeSet(set)
}
