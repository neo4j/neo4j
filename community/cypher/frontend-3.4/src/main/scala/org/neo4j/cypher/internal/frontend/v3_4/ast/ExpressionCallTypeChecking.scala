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
package org.neo4j.cypher.internal.frontend.v3_4.ast

import org.neo4j.cypher.internal.frontend.v3_4.{SemanticCheck, SemanticCheckResult}
import org.neo4j.cypher.internal.frontend.v3_4.symbols.TypeSpec

trait ExpressionCallTypeChecking {

  def signatures: Seq[ExpressionSignature] = Seq.empty

  protected final def signatureLengths = typeChecker.signatureLengths
  protected final lazy val typeChecker: ExpressionCallTypeChecker = ExpressionCallTypeChecker(signatures)
}

case class ExpressionCallTypeChecker(signatures: Seq[ExpressionSignature]) {

  val signatureLengths = signatures.map(_.argumentTypes.length)

  def checkTypes(invocation: Expression): SemanticCheck = s => {
    val initSignatures = signatures.filter(_.argumentTypes.length == invocation.arguments.length)

    val (remainingSignatures: Seq[ExpressionSignature], result) =
      invocation.arguments.foldLeft((initSignatures, SemanticCheckResult.success(s))) {
        case (accumulator@(Seq(), _), _) =>
          accumulator
        case ((possibilities, r1), arg)  =>
          val argTypes = possibilities.foldLeft(TypeSpec.none) { _ | _.argumentTypes.head.covariant }
          val r2 = arg.expectType(argTypes)(r1.state)

          val actualTypes = arg.types(r2.state)
          val remainingPossibilities = possibilities.filter {
            sig => actualTypes containsAny sig.argumentTypes.head.covariant
          } map {
            sig => sig.copy(argumentTypes = sig.argumentTypes.tail)
          }
          (remainingPossibilities, SemanticCheckResult(r2.state, r1.errors ++ r2.errors))
      }

    val outputType = remainingSignatures match {
      case Seq() => TypeSpec.all
      case _     => remainingSignatures.foldLeft(TypeSpec.none) { _ | _.outputType.invariant }
    }

    invocation.specifyType(outputType)(result.state) match {
      case Left(err)    => SemanticCheckResult(result.state, result.errors :+ err)
      case Right(state) => SemanticCheckResult(state, result.errors)
    }
  }
}
