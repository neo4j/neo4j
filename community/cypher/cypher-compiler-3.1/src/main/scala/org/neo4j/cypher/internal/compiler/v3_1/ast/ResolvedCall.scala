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
package org.neo4j.cypher.internal.compiler.v3_1.ast

import org.neo4j.cypher.internal.compiler.v3_1.spi.{ProcedureReadOnlyAccess, ProcedureSignature, QualifiedName}
import org.neo4j.cypher.internal.frontend.v3_1.SemanticCheckResult._
import org.neo4j.cypher.internal.frontend.v3_1._
import org.neo4j.cypher.internal.frontend.v3_1.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_1.ast._
import org.neo4j.cypher.internal.frontend.v3_1.symbols.{CypherType, _}

object ResolvedCall {
  def apply(signatureLookup: QualifiedName => ProcedureSignature)(unresolved: UnresolvedCall): ResolvedCall = {
    val UnresolvedCall(_, _, declaredArguments, declaredResult) = unresolved
    val position = unresolved.position
    val signature = signatureLookup(QualifiedName(unresolved))
    val nonDefaults = signature.inputSignature.flatMap(s => if (s.default.isDefined) None else Some(Parameter(s.name, CTAny)(position)))
    val callArguments = declaredArguments.getOrElse(nonDefaults)
    val callResults = declaredResult.map(_.items).getOrElse(signatureResults(signature, position))
    val callFilter = declaredResult.flatMap(_.where)
    if (callFilter.nonEmpty)
      throw new IllegalArgumentException(s"Expected no unresolved call with WHERE but got: $unresolved")
    else
      ResolvedCall(signature, callArguments, callResults, declaredArguments.nonEmpty, declaredResult.nonEmpty)(position)
  }

  private def signatureResults(signature: ProcedureSignature, position: InputPosition): IndexedSeq[ProcedureResultItem] =
    signature.outputSignature.getOrElse(Seq.empty).map {
      field => ProcedureResultItem(Variable(field.name)(position))(position)
  }.toIndexedSeq
}

case class ResolvedCall(signature: ProcedureSignature,
                        callArguments: Seq[Expression],
                        callResults: IndexedSeq[ProcedureResultItem],
                        // true if given by the user originally
                        declaredArguments: Boolean = true,
                        // true if given by the user originally
                        declaredResults: Boolean = true)
                       (val position: InputPosition)
  extends CallClause {

  def qualifiedName: QualifiedName = signature.name

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
    val usedDefaultArgs = signature.inputSignature.drop(callArguments.length).flatMap(_.default)
    val actualNumArgs = callArguments.length + usedDefaultArgs.length

    if (declaredArguments) {
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
        error(_: SemanticState, SemanticError(
          s"""Procedure call does not provide the required number of arguments: got $actualNumArgs expected $expectedNumArgs.
             |
             |Procedure ${signature.name} has signature: $signature
             |meaning that it expects $expectedNumArgs $msg""".stripMargin, position))
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
    case ProcedureReadOnlyAccess(_) => true
    case _ => false
  }
}
