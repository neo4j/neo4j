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
package org.neo4j.cypher.internal.compiler.v3_0.ast

import org.neo4j.cypher.internal.compiler.v3_0.spi.{ProcedureReadOnlyAccess, ProcedureSignature, QualifiedProcedureName}
import org.neo4j.cypher.internal.frontend.v3_0.SemanticCheckResult._
import org.neo4j.cypher.internal.frontend.v3_0._
import org.neo4j.cypher.internal.frontend.v3_0.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_0.ast._
import org.neo4j.cypher.internal.frontend.v3_0.symbols.{CypherType, _}

object ResolvedCall {
  def apply(signatureLookup: QualifiedProcedureName => ProcedureSignature)(unresolved: UnresolvedCall): ResolvedCall = {
    val UnresolvedCall(_, _, declaredArguments, declaredResults) = unresolved
    val position = unresolved.position
    val signature = signatureLookup(QualifiedProcedureName(unresolved))
    val callArguments = declaredArguments.getOrElse(signatureArguments(signature, position))
    val callResults = declaredResults.getOrElse(signatureResults(signature, position))
    ResolvedCall(signature, callArguments, callResults, declaredArguments.nonEmpty, declaredResults.nonEmpty)(position)
  }

  private def signatureArguments(signature: ProcedureSignature, position: InputPosition): Seq[Parameter] =
    signature.inputSignature.map { field => Parameter(field.name, CTAny)(position) }

  private def signatureResults(signature: ProcedureSignature, position: InputPosition): Seq[ProcedureResultItem] =
    signature.outputSignature.getOrElse(Seq.empty).map { field => ProcedureResultItem(Variable(field.name)(position))(position) }
}

case class ResolvedCall(signature: ProcedureSignature,
                        callArguments: Seq[Expression],
                        callResults: Seq[ProcedureResultItem],
                        // true if given by the user originally
                        val declaredArguments: Boolean = true,
                        // true if given by the user originally
                        val declaredResults: Boolean = true)
                       (val position: InputPosition)
  extends CallClause {

  def qualifiedName: QualifiedProcedureName = signature.name

  def fullyDeclared: Boolean = declaredArguments && declaredResults

  def withFakedFullDeclarations: ResolvedCall =
    copy(declaredArguments = true, declaredResults = true)(position)

  def coerceArguments: ResolvedCall = {
    val optInputFields = signature.inputSignature.map(Some(_)).toStream ++ Stream.continually(None)
    val coercedArguments=
      callArguments
        .zip(optInputFields)
        .map {
          case (arg, optField) =>
            optField.map { field => CoerceTo(arg, field.typ) }.getOrElse(arg)
        }
    copy(callArguments = coercedArguments)(position)
  }

  override def returnColumns: List[String] =
    callResults.map(_.variable.name).toList

  def callResultIndices: Seq[(Int, String)] = {
    val outputIndices: Map[String, Int] = signature.outputSignature.map { outputs => outputs.map(_.name).zip(outputs.indices).toMap }.getOrElse(Map.empty)
    callResults.map(result => outputIndices(result.outputName) -> result.variable.name)
  }

  def callResultTypes: Seq[(String, CypherType)] = {
    val outputTypes = callOutputTypes
    callResults.map(result => result.variable.name -> outputTypes(result.outputName))
  }

  override def semanticCheck: SemanticCheck =
    argumentCheck chain resultCheck

  private def argumentCheck: SemanticCheck = {
    val expectedNumArgs = signature.inputSignature.length
    val actualNumArgs = callArguments.length

    if (declaredArguments) {
      if (expectedNumArgs == actualNumArgs) {
        signature.inputSignature.zip(callArguments).map {
          case (field, arg) =>
            arg.semanticCheck(SemanticContext.Results) chain arg.expectType(field.typ.covariant)
        }.foldLeft(success)(_ chain _)
      } else {
        error(_: SemanticState, SemanticError(s"Procedure call does not provide the required number of arguments ($expectedNumArgs)", position))
      }
    } else {
      error(_: SemanticState, SemanticError(s"Procedure call inside a query does not support passing arguments implicitly (pass explicitly after procedure name instead)", position))
    }
  }

  private def resultCheck: SemanticCheck =
    // CALL of VOID procedure => No need to name arguments, even in query
    // CALL of empty procedure => No need to name arguments, even in query
    if (signature.outputFields.isEmpty)
      success
    // CALL ... YIELD ... => Check named outputs
    else if (declaredResults)
      callResults.foldSemanticCheck(_.semanticCheck(callOutputTypes))
    // CALL wo YIELD of non-VOID or non-empty procedure in query => Error
    else
      error(_: SemanticState, SemanticError(s"Procedure call inside a query does not support naming results implicitly (name explicitly using `YIELD` instead)", position))

  private val callOutputTypes: Map[String, CypherType] =
    signature.outputSignature.map { _.map { field => field.name -> field.typ }.toMap }.getOrElse(Map.empty)

  override def containsNoUpdates = signature.accessMode match {
    case ProcedureReadOnlyAccess => true
    case _ => false
  }
}
