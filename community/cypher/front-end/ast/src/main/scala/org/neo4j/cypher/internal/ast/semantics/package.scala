/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.util.symbols.TypeSpec

import scala.language.implicitConversions

package object semantics {

  type TypeGenerator = SemanticState => TypeSpec

  implicit def `Convert function to SemanticCheck`(f: SemanticState => SemanticCheckResult): SemanticCheck =
    SemanticCheck.fromFunction(f)

  // Allows using a (SemanticState => Either[SemanticErrorDef, SemanticState]) func, where a (SemanticState => SemanticCheckResult) func is expected
  implicit def liftSemanticEitherFunc(func: SemanticState => Either[SemanticErrorDef, SemanticState]): SemanticCheck = {
    state: SemanticState =>
      func(state).fold(
        error => SemanticCheckResult.error(state, error),
        s => SemanticCheckResult.success(s)
      )
  }

  // Allows using a (SemanticState => Seq[SemanticErrorDef]) func, where a (SemanticState => SemanticCheckResult) func is expected
  implicit def liftSemanticErrorDefsFunc(func: SemanticState => Seq[SemanticErrorDef]): SemanticCheck =
    (state: SemanticState) => {
      SemanticCheckResult(state, func(state))
    }

  // Allows using a (SemanticState => Option[SemanticErrorDef]) func, where a (SemanticState => SemanticCheckResult) func is expected
  implicit def liftSemanticErrorDefFunc(func: SemanticState => Option[SemanticErrorDef]): SemanticCheck =
    (state: SemanticState) => {
      SemanticCheckResult.error(state, func(state))
    }

  // Allows using a sequence of SemanticErrorDef, where a (SemanticState => SemanticCheckResult) func is expected
  implicit def liftSemanticErrorDefs(errors: Seq[SemanticErrorDef]): SemanticCheck = SemanticCheckResult(_, errors)

  // Allows using a single SemanticErrorDef, where a (SemanticState => SemanticCheckResult) func is expected
  implicit def liftSemanticErrorDef(error: SemanticErrorDef): SemanticCheck =
    SemanticCheck.fromFunction(SemanticCheckResult.error(_, error))

  // Allows using an optional SemanticErrorDef, where a (SemanticState => SemanticCheckResult) func is expected
  implicit def liftSemanticErrorDefOption(error: Option[SemanticErrorDef]): SemanticCheck =
    SemanticCheck.fromFunction(SemanticCheckResult.error(_, error))

  // Allows folding a semantic checking func over a collection
  implicit def optionSemanticChecking[A](option: Option[A]): OptionSemanticChecking[A] =
    new OptionSemanticChecking(option)

  implicit def iterableOnceSemanticChecking[A](iterable: IterableOnce[A]): IterableOnceSemanticChecking[A] =
    new IterableOnceSemanticChecking(iterable)

  // Allows calling semanticCheck on an optional SemanticCheckable object
  implicit def semanticCheckableOption[A <: SemanticCheckable](option: Option[A]): SemanticCheckableOption[A] =
    new SemanticCheckableOption(option)

  // Allows calling semanticCheck on a iterable sequence of SemanticCheckable objects
  implicit def semanticCheckableIterableOnce[A <: SemanticCheckable](iterable: IterableOnce[A])
    : SemanticCheckableIterableOnce[A] = new SemanticCheckableIterableOnce(iterable)
}
