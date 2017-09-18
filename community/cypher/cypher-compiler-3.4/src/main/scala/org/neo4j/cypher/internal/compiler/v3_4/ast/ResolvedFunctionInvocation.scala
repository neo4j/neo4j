/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_4.ast

import org.neo4j.cypher.internal.frontend.v3_4.SemanticCheckResult._
import org.neo4j.cypher.internal.frontend.v3_4._
import org.neo4j.cypher.internal.frontend.v3_4.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.ast.functions.UserDefinedFunctionInvocation
import org.neo4j.cypher.internal.v3_4.logical.plans.{QualifiedName, UserFunctionSignature}

object ResolvedFunctionInvocation {

  def apply(signatureLookup: QualifiedName => Option[UserFunctionSignature])(unresolved: FunctionInvocation): ResolvedFunctionInvocation = {
    val position = unresolved.position
    val name = QualifiedName(unresolved)
    val signature = signatureLookup(name)
    ResolvedFunctionInvocation(name, signature, unresolved.args)(position)
  }
}

/**
  * A ResolvedFunctionInvocation is a user-defined function where the signature
  * has been resolve, i.e. verified that it exists in the database
  *
  * @param qualifiedName The qualified name of the function.
  * @param fcnSignature Either `Some(signature)` if the signature was resolved, or
  *                     `None` if the function didn't exist
  * @param callArguments The argument list to the function
  * @param position The position in the original query string.
  */
case class ResolvedFunctionInvocation(qualifiedName: QualifiedName,
                                      fcnSignature: Option[UserFunctionSignature],
                                      callArguments: IndexedSeq[Expression])
                                     (val position: InputPosition)
  extends Expression with UserDefinedFunctionInvocation {

  def coerceArguments: ResolvedFunctionInvocation = fcnSignature match {
    case Some(signature) =>
    val optInputFields = signature.inputSignature.map(Some(_)).toStream ++ Stream.continually(None)
    val coercedArguments =
      callArguments
        .zip(optInputFields)
        .map {
          case (arg, optField) =>
            optField.map { field => CoerceTo(arg, field.typ) }.getOrElse(arg)
        }
    copy(callArguments = coercedArguments)(position)
    case None => this
  }

  override def semanticCheck(ctx: SemanticContext): SemanticCheck = fcnSignature match {
    case None => SemanticError(s"Unknown function '$qualifiedName'", position)
    case Some(signature) =>
      val expectedNumArgs = signature.inputSignature.length
      val usedDefaultArgs = signature.inputSignature.drop(callArguments.length).flatMap(_.default)
      val actualNumArgs = callArguments.length + usedDefaultArgs.length

        if (expectedNumArgs == actualNumArgs) {
          //this zip is fine since it will only verify provided args in callArguments
          //default values are checked at load time
          signature.inputSignature.zip(callArguments).map {
            case (field, arg) =>
              arg.semanticCheck(SemanticContext.Results) chain arg.expectType(field.typ.covariant)
          }.foldLeft(success)(_ chain _)
        } else {
          val msg = (if (signature.inputSignature.isEmpty) "arguments"
          else if (signature.inputSignature.size == 1) s"argument of type ${signature.inputSignature.head.typ.toNeoTypeString}"
          else s"arguments of type ${signature.inputSignature.map(_.typ.toNeoTypeString).mkString(", ")}") +
            signature.description.map(d => s"${System.lineSeparator()}Description: $d").getOrElse("")
          error(_: SemanticState, SemanticError( s"""Function call does not provide the required number of arguments: expected $expectedNumArgs got $actualNumArgs.
             |
             |Function ${signature.name} has signature: $signature
             |meaning that it expects $expectedNumArgs $msg""".stripMargin, position))
        }
  }

  override def isAggregate: Boolean = fcnSignature.exists(_.isAggregate)
}
