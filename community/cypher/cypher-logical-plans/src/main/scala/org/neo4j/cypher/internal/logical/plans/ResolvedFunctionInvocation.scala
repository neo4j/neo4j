/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult.error
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult.success
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckableExpression
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.CoerceTo
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Expression.SemanticContext
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.functions.UserDefinedFunctionInvocation
import org.neo4j.cypher.internal.util.InputPosition

object ResolvedFunctionInvocation {

  def apply(signatureLookup: QualifiedName => Option[UserFunctionSignature])(unresolved: FunctionInvocation): ResolvedFunctionInvocation = {
    val position = unresolved.position
    val name = QualifiedName(unresolved)
    ResolvedFunctionInvocation(name, signatureLookup(name), unresolved.args)(position)
  }
}

/**
 * A ResolvedFunctionInvocation is a user-defined function where the signature
 * has been resolved, i.e. verified that it exists in the database
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
  extends Expression with UserDefinedFunctionInvocation with SemanticCheckableExpression {

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
    case None =>
      qualifiedName match {
        case QualifiedName(Seq(), qn) if qn.equalsIgnoreCase("not") =>
          SemanticError(s"Unknown function '$qualifiedName'. " +
            s"If you intended to use the negation expression, surround it with parentheses.", position)
        case QualifiedName(_, "toInt") =>
          SemanticError(s"The function toInt() is no longer supported. Please use toInteger() instead", position)
        case QualifiedName(_, "lower") =>
          SemanticError(s"The function lower() is no longer supported. Please use toLower() instead", position)
        case QualifiedName(_, "upper") =>
          SemanticError(s"The function upper() is no longer supported. Please use toUpper() instead", position)
        case QualifiedName(_, "rels") =>
          SemanticError(s"The function rels() is no longer supported. Please use relationships() instead", position)
        case _ => SemanticError(s"Unknown function '$qualifiedName'", position)
      }
    case Some(signature) =>
      val expectedNumArgs = signature.inputSignature.length
      val usedDefaultArgs = signature.inputSignature.drop(callArguments.length).flatMap(_.default)
      val actualNumArgs = callArguments.length + usedDefaultArgs.length

        if (expectedNumArgs == actualNumArgs) {
          //this zip is fine since it will only verify provided args in callArguments
          //default values are checked at load time
          signature.inputSignature.zip(callArguments).map {
            case (field, arg) =>
              SemanticExpressionCheck.check(SemanticContext.Results, arg) chain
                SemanticExpressionCheck.expectType(field.typ.covariant, arg)
          }.foldLeft(success)(_ chain _) chain
            SemanticExpressionCheck.specifyType(signature.outputType.covariant, this)
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

  def asUnresolvedFunction: FunctionInvocation = FunctionInvocation(
    namespace = Namespace(qualifiedName.namespace.toList)(position),
    functionName = FunctionName(qualifiedName.name)(position),
    distinct = false,
    args = arguments.toIndexedSeq,
  )(position)

}
