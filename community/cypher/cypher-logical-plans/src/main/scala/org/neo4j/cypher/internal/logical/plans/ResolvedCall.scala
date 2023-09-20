/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.ast.CallClause
import org.neo4j.cypher.internal.ast.ProcedureResult
import org.neo4j.cypher.internal.ast.ProcedureResultItem
import org.neo4j.cypher.internal.ast.ReturnItems.ReturnVariables
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.success
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.expressions.AutoExtractedParameter
import org.neo4j.cypher.internal.expressions.CoerceTo
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Expression.SemanticContext
import org.neo4j.cypher.internal.expressions.ImplicitProcedureArgument
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.ProcedureName
import org.neo4j.cypher.internal.expressions.SensitiveAutoParameter
import org.neo4j.cypher.internal.expressions.SensitiveLiteral
import org.neo4j.cypher.internal.expressions.SensitiveParameter
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.ZippableUtil.Zippable
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.exceptions.SyntaxException

object ResolvedCall {

  def apply(signatureLookup: QualifiedName => ProcedureSignature)(unresolved: UnresolvedCall): ResolvedCall = {
    val UnresolvedCall(_, _, declaredArguments, declaredResult, yieldAll) = unresolved
    val position = unresolved.position
    val signature = signatureLookup(QualifiedName(unresolved))
    def implicitArguments = signature.inputSignature.map(s =>
      s.default.map(d => ImplicitProcedureArgument(s.name, s.typ, d)).getOrElse(
        ExplicitParameter(s.name, s.typ)(position)
      )
    )
    val callArguments = declaredArguments.getOrElse(implicitArguments)
    val sensitiveArguments = signature.inputSignature.take(callArguments.length).map(_.sensitive)
    val callArgumentsWithSensitivityMarkers = callArguments.zipAll(sensitiveArguments, null, false).map {
      case (p: ExplicitParameter, true) =>
        new ExplicitParameter(p.name, p.parameterType, p.sizeHint)(p.position) with SensitiveParameter
      case (p: AutoExtractedParameter, true) =>
        new AutoExtractedParameter(p.name, p.parameterType, p.sizeHint)(p.position)
          with SensitiveAutoParameter
      case (p: StringLiteral, true) => new StringLiteral(p.value)(p.position) with SensitiveLiteral {
          override def literalLength: Option[Int] = None
        }
      case (p, _) => p
    }

    def implicitCallResults = signatureResults(signature, position)
    val callResults = declaredResult.map(_.items).getOrElse(implicitCallResults)

    val callFilter = declaredResult.flatMap(_.where)
    if (callFilter.nonEmpty)
      throw new IllegalArgumentException(s"Expected no unresolved call with WHERE but got: $unresolved")
    else
      ResolvedCall(
        signature,
        callArgumentsWithSensitivityMarkers,
        callResults,
        declaredArguments.nonEmpty,
        declaredResult.nonEmpty,
        yieldAll
      )(position)
  }

  private def signatureResults(
    signature: ProcedureSignature,
    position: InputPosition
  ): IndexedSeq[ProcedureResultItem] =
    signature.outputSignature.getOrElse(Seq.empty).map {
      field => ProcedureResultItem(Variable(field.name)(position))(position)
    }.toIndexedSeq
}

case class ResolvedCall(
  signature: ProcedureSignature,
  callArguments: Seq[Expression],
  callResults: IndexedSeq[ProcedureResultItem],
  // true if given by the user originally
  declaredArguments: Boolean = true,
  // true if given by the user originally
  declaredResults: Boolean = true,
  // YIELD *
  override val yieldAll: Boolean = false
)(val position: InputPosition)
    extends CallClause {

  def qualifiedName: QualifiedName = signature.name

  def fullyDeclared: Boolean = declaredArguments && declaredResults

  def withFakedFullDeclarations: ResolvedCall =
    // keep old YieldAll value for VOID procedure to be able to throw correct error if true
    copy(
      declaredArguments = true,
      declaredResults = true,
      yieldAll = if (signature.outputSignature.isEmpty) yieldAll else false
    )(position)

  def coerceArguments: ResolvedCall = {
    val optInputFields = signature.inputSignature.map(Some(_))
    val coercedArguments =
      callArguments.zipLeft(optInputFields, None)
        .map {
          case (arg, optField) =>
            // If type is CTAny we don't need any coercion
            optField.map { field => if (field.typ == symbols.CTAny) arg else CoerceTo(arg, field.typ) }.getOrElse(arg)
        }
    copy(callArguments = coercedArguments)(position)
  }

  override def returnVariables: ReturnVariables =
    ReturnVariables(includeExisting = false, callResults.map(_.variable).toList)

  def callResultIndices: IndexedSeq[(Int, (String, String))] = { // pos, newName, oldName
    val outputIndices: Map[String, Int] = signature.outputSignature.map { outputs =>
      outputs.map(_.name).zip(outputs.indices).toMap
    }.getOrElse(Map.empty)
    callResults.map(result => outputIndices(result.outputName) -> (result.variable.name -> result.outputName))
  }

  def callResultTypes: Seq[(String, CypherType)] = {
    if (signature.outputSignature.isEmpty && (callResults.nonEmpty || yieldAll)) {
      throw new SyntaxException("Cannot yield value from void procedure.")
    }
    val outputTypes = callOutputTypes
    callResults.map(result => result.variable.name -> outputTypes(result.outputName))
  }

  def mapCallArguments(f: Expression => Expression): ResolvedCall =
    copy(callArguments = callArguments.map(f))(this.position)

  override def clauseSpecificSemanticCheck: SemanticCheck =
    argumentCheck chain resultCheck

  private def argumentCheck: SemanticCheck = {
    val totalNumArgs = signature.inputSignature.length
    val numArgsWithDefaults = signature.inputSignature.flatMap(_.default).size
    val minNumArgs = totalNumArgs - numArgsWithDefaults
    val givenNumArgs = callArguments.length

    if (declaredArguments) {
      val tooFewArgs = givenNumArgs < minNumArgs
      val tooManyArgs = givenNumArgs > totalNumArgs
      if (!tooFewArgs && !tooManyArgs) {
        // this zip is fine since it will only verify provided args in callArguments
        // default values are checked at load time
        signature.inputSignature.zip(callArguments).map {
          case (field, arg) =>
            SemanticExpressionCheck.check(SemanticContext.Results, arg) chain
              SemanticExpressionCheck.expectType(field.typ.covariant, arg)
        }.foldLeft(success)(_ chain _)
      } else {
        val argTypes = minNumArgs match {
          case 0 => "no arguments"
          case 1 => s"at least 1 argument of type ${signature.inputSignature.head.typ.normalizedCypherTypeString()}"
          case _ =>
            s"at least $minNumArgs arguments of types ${signature.inputSignature.take(minNumArgs).map(_.typ.normalizedCypherTypeString()).mkString(", ")}"
        }
        val sigDesc =
          s"""Procedure ${signature.name} has signature: $signature
             |meaning that it expects $argTypes""".stripMargin
        val description = signature.description.fold("")(d => s"Description: $d")

        if (tooFewArgs) {
          error(
            s"""Procedure call does not provide the required number of arguments: got $givenNumArgs expected at least $minNumArgs (total: $totalNumArgs, $numArgsWithDefaults of which have default values).
               |
               |$sigDesc
               |$description""".stripMargin,
            position
          )
        } else {
          val maxExpectedMsg = totalNumArgs match {
            case 0 => "none"
            case _ => s"no more than $totalNumArgs"
          }
          error(
            s"""Procedure call provides too many arguments: got $givenNumArgs expected $maxExpectedMsg.
               |
               |$sigDesc
               |$description""".stripMargin,
            position
          )
        }
      }
    } else {
      if (totalNumArgs == 0)
        error("Procedure call is missing parentheses: " + signature.name, position)
      else
        error(
          "Procedure call inside a query does not support passing arguments implicitly. " +
            "Please pass arguments explicitly in parentheses after procedure name for " + signature.name,
          position
        )
    }
  }

  private def resultCheck: SemanticCheck =
    // CALL of VOID procedure => No need to name arguments, even in query
    // CALL of empty procedure => No need to name arguments, even in query
    if (signature.outputFields.isEmpty && (callResults.nonEmpty || yieldAll)) {
      error("Cannot yield value from void procedure.", position)
    } else if (signature.outputFields.isEmpty) {
      success
    } // CALL ... YIELD ... => Check named outputs
    else if (declaredResults) {
      callResults.foldSemanticCheck(_.semanticCheck(callOutputTypes))
    } // CALL wo YIELD of non-VOID or non-empty procedure in query => Error
    else {
      error(
        s"Procedure call inside a query does not support naming results implicitly (name explicitly using `YIELD` instead)",
        position
      )
    }

  private val callOutputTypes: Map[String, CypherType] =
    signature.outputSignature.map { _.map { field => field.name -> field.typ }.toMap }.getOrElse(Map.empty)

  override def containsNoUpdates: Boolean = signature.accessMode match {
    case ProcedureReadOnlyAccess => true
    case ProcedureDbmsAccess     => true
    case _                       => false
  }

  def asUnresolvedCall: UnresolvedCall = UnresolvedCall(
    procedureNamespace = Namespace(signature.name.namespace.toList)(position),
    procedureName = ProcedureName(signature.name.name)(position),
    declaredArguments = if (declaredArguments) Some(callArguments) else None,
    declaredResult = if (declaredResults) Some(ProcedureResult(callResults)(position)) else None,
    yieldAll
  )(position)
}
