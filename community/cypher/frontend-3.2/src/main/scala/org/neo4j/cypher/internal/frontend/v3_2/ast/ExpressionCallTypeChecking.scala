/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_2.ast

import org.neo4j.cypher.internal.frontend.v3_2.symbols.TypeSpec
import org.neo4j.cypher.internal.frontend.v3_2.{SemanticCheck, SemanticCheckResult}

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
