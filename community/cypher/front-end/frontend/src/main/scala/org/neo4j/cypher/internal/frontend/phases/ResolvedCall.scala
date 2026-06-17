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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.ast.AbstractFieldSignature
import org.neo4j.cypher.internal.ast.CallClause
import org.neo4j.cypher.internal.ast.LocalFieldSignature
import org.neo4j.cypher.internal.ast.NonOptional
import org.neo4j.cypher.internal.ast.OptionalState
import org.neo4j.cypher.internal.ast.ProcedureResult
import org.neo4j.cypher.internal.ast.ProcedureResultItem
import org.neo4j.cypher.internal.ast.ReturnItems.ReturnVariables
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.semantics.*
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.success
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.ast.semantics.scoping.LocalProcedureScopeSignature
import org.neo4j.cypher.internal.expressions.CoerceTo
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Expression.SemanticContext
import org.neo4j.cypher.internal.expressions.ImplicitProcedureArgument
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.ProcedureName
import org.neo4j.cypher.internal.util.ZippableUtil.Zippable
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.exceptions.SyntaxException

trait ResolvedCall[IMPL <: ResolvedCall[IMPL]] extends CallClause {
  def procedureName: ProcedureName
  def inputFieldSignatures: Seq[AbstractFieldSignature]
  def outputFieldSignatures: Option[Seq[AbstractFieldSignature]]

  def callArguments: Seq[Expression]
  def callResults: IndexedSeq[ProcedureResultItem]
  def declaredArguments: Boolean
  def declaredResults: Boolean

  def withArguments(arguments: Seq[Expression]): IMPL
  def withFakedFullDeclarations: IMPL

  def asUnresolvedCall: UnresolvedCall

  def coerceArguments: IMPL = {
    val optInputFields = inputFieldSignatures.map(Some(_))
    val coercedArguments =
      callArguments.zipLeft(optInputFields, None)
        .map {
          case (arg, optField) =>
            // If type is CTAny we don't need any coercion
            optField.map { field =>
              if (field.getType == symbols.CTAny) arg
              else CoerceTo(arg, field.getType)
            }.getOrElse(arg)
        }
    withArguments(coercedArguments)
  }

  def callResultIndices: IndexedSeq[(Int, String, String)] = { // pos, newName, oldName
    val outputIndices: Map[String, Int] = outputFieldSignatures.map { outputs =>
      outputs.map(_.name).zip(outputs.indices).toMap
    }.getOrElse(Map.empty)
    callResults.map(result => (outputIndices(result.outputName), result.variable.name, result.outputName))
  }

  def callResultTypes: Seq[(String, CypherType)] = {
    if (outputFieldSignatures.isEmpty && (callResults.nonEmpty || yieldAll)) {
      throw SyntaxException.cannotYieldFromVoidProcedure()
    }
    callResults.map(result => result.variable.name -> callOutputTypes(result.outputName))
  }

  override def returnVariables: ReturnVariables =
    ReturnVariables(includeExisting = false, callResults.map(_.variable).toList)

  def signatureString: String

  def description: String

  override def argumentCheck: SemanticCheck = {
    val totalNumArgs = inputFieldSignatures.length
    val numArgsWithDefaults = inputFieldSignatures.count(_.hasDefault)
    val minNumArgs = totalNumArgs - numArgsWithDefaults
    val givenNumArgs = callArguments.length

    if (declaredArguments) {
      val tooFewArgs = givenNumArgs < minNumArgs
      val tooManyArgs = givenNumArgs > totalNumArgs
      if (!tooFewArgs && !tooManyArgs) {
        // this zip is fine since it will only verify provided args in callArguments
        // default values are checked at load time
        inputFieldSignatures.zip(callArguments).map {
          case (field, arg) =>
            SemanticExpressionCheck.check(SemanticContext.Results, arg) chain
              SemanticExpressionCheck.expectType(field.getType.covariant, arg)
        }.foldLeft(success)(_ chain _)
      } else {
        val argTypes = minNumArgs match {
          case 0 => "no arguments"
          case 1 => s"at least 1 argument of type ${inputFieldSignatures.head.getType.normalizedCypherTypeString()}"
          case _ =>
            s"at least $minNumArgs arguments of types ${inputFieldSignatures.take(minNumArgs).map(_.getType.normalizedCypherTypeString()).mkString(", ")}"
        }
        val sigDesc =
          s"""Procedure ${procedureName.fullName} has signature: $signatureString
             |meaning that it expects $argTypes""".stripMargin

        if (tooFewArgs) {
          error(SemanticError.procedureCallTooFewArguments(
            givenNumArgs,
            minNumArgs,
            totalNumArgs,
            numArgsWithDefaults,
            String.valueOf(procedureName.fullName),
            String.valueOf(signatureString),
            sigDesc,
            description,
            position
          ))
        } else {
          val maxExpectedMsg = totalNumArgs match {
            case 0 => "none"
            case _ => s"no more than $totalNumArgs"
          }
          error(SemanticError.procedureCallTooManyArguments(
            totalNumArgs,
            givenNumArgs,
            String.valueOf(procedureName.fullName),
            String.valueOf(signatureString),
            maxExpectedMsg,
            sigDesc,
            description,
            position
          ))
        }
      }
    } else {
      if (totalNumArgs == 0) {
        error(SemanticError.procedureCallWithoutParentheses(procedureName.fullName, position))
      } else
        error(SemanticError.procedureCallWithParenthesesWithArgs(procedureName.fullName, position))
    }
  }

  override def resultCheck: SemanticCheck = {
    // CALL of VOID procedure => No need to name arguments, even in query
    // CALL of empty procedure => No need to name arguments, even in query
    if ((outputFieldSignatures.isEmpty || outputFieldSignatures.get.isEmpty) && (callResults.nonEmpty || yieldAll)) {
      error(SemanticError.cannotYieldFromVoidProcedure(position))
    } else if (outputFieldSignatures.isEmpty || outputFieldSignatures.get.isEmpty) {
      success
    } // CALL ... YIELD ... => Check named outputs
    else if (declaredResults) {
      callResults.foldSemanticCheck(_.semanticCheck(callOutputTypes))
    } // CALL wo YIELD of non-VOID or non-empty procedure in query => Error
    else {
      error(
        SemanticError.procedureCallWithImplicitNaming(callOutputTypes.keys.toList.sorted, position)
      )
    }
  }

  override def invalidAggregationCheck: SemanticCheck =
    callArguments.foldSemanticCheck(arg => checkArgumentForAggregation(arg))

  val callOutputTypes: Map[String, CypherType] =
    outputFieldSignatures.map { _.map { field => field.name -> field.getType }.toMap }.getOrElse(Map.empty)
}

object ResolvedNonLocalCall {

  def apply(signatureLookup: ProcedureName => ProcedureSignature)(unresolved: UnresolvedCall): ResolvedNonLocalCall = {
    val UnresolvedCall(_, declaredArguments, declaredResult, _, yieldAll, optional) = unresolved
    val position = unresolved.position
    val signature = signatureLookup(unresolved.procedureName)
    def implicitArguments = signature.inputSignature.map(s =>
      s.default.map(d => ImplicitProcedureArgument(s.name, s.typ, d)).getOrElse(
        ExplicitParameter(s.name, s.typ)(position)
      )
    )
    val callArguments = declaredArguments.getOrElse(implicitArguments)
    val sensitiveArguments = signature.inputSignature.take(callArguments.length).map(_.sensitive)
    val callArgumentsWithSensitivityMarkers = callArguments.zipAll(sensitiveArguments, null, false).map {
      case (e: Expression, true) => e.endoRewrite(SensitiveParameterRewriter)
      case (p, _)                => p
    }

    def implicitCallResults = signatureResults(signature, position)
    val callResults = declaredResult.map(_.items).getOrElse(implicitCallResults)

    val callFilter = declaredResult.flatMap(_.where)
    if (callFilter.nonEmpty)
      throw new IllegalArgumentException(s"Expected no unresolved call with WHERE but got: $unresolved")
    else
      ResolvedNonLocalCall(
        signature,
        callArgumentsWithSensitivityMarkers,
        callResults,
        declaredArguments.nonEmpty,
        declaredResult.nonEmpty,
        yieldAll,
        optional
      )(position)
  }

  private def signatureResults(
    signature: ProcedureSignature,
    position: InputPosition
  ): IndexedSeq[ProcedureResultItem] =
    signature.outputSignature.getOrElse(Seq.empty).map {
      field => ProcedureResultItem(Variable(field.name)(position, Variable.isIsolatedDefault))(position)
    }.toIndexedSeq
}

case class ResolvedNonLocalCall(
  signature: ProcedureSignature,
  override val callArguments: Seq[Expression],
  override val callResults: IndexedSeq[ProcedureResultItem],
  // true if given by the user originally
  override val declaredArguments: Boolean = true,
  // true if given by the user originally
  override val declaredResults: Boolean = true,
  // YIELD *
  override val yieldAll: Boolean = false,
  override val optionalState: OptionalState = NonOptional
)(val position: InputPosition) extends ResolvedCall[ResolvedNonLocalCall] {
  override def procedureName: ProcedureName = signature.name

  override def inputFieldSignatures: Seq[AbstractFieldSignature] = signature.inputSignature

  override def outputFieldSignatures: Option[Seq[AbstractFieldSignature]] = signature.outputSignature

  def withArguments(arguments: Seq[Expression]): ResolvedNonLocalCall = copy(callArguments = arguments)(position)

  def withFakedFullDeclarations: ResolvedNonLocalCall =
    // keep old YieldAll value for VOID procedure to be able to throw correct error if true
    copy(
      declaredArguments = true,
      declaredResults = true,
      yieldAll = if (signature.outputSignature.isEmpty) yieldAll else false
    )(position)

  def mapCallArguments(f: Expression => Expression): ResolvedNonLocalCall =
    copy(callArguments = callArguments.map(f))(this.position)

  def signatureString: String = signature.toString

  override val description: String = signature.description.fold("")(d => s"Description: $d")

  override def containsNoUpdates: Boolean = signature.accessMode match {
    case ProcedureReadOnlyAccess => true
    case ProcedureDbmsAccess     => true
    case _                       => false
  }

  override def asUnresolvedCall: UnresolvedCall = UnresolvedCall(
    procedureName = signature.name,
    declaredArguments = if (declaredArguments) Some(callArguments) else None,
    declaredResult = if (declaredResults) Some(ProcedureResult(callResults)(position)) else None,
    yieldAll,
    optional
  )(position)
}

object ResolvedLocalCall {

  def apply(
    unresolved: UnresolvedCall,
    localProcedureScopeSignature: LocalProcedureScopeSignature
  ): ResolvedLocalCall = {
    val UnresolvedCall(procedureName, declaredArguments, declaredResult, _, yieldAll, optional) = unresolved
    val position = unresolved.position
    val inputSignature = localProcedureScopeSignature.inputSignature
    val outputSignature = localProcedureScopeSignature.outputSignature

    val callArguments = declaredArguments.getOrElse(Seq.empty)
    val callArgumentsWithSensitivityMarkers = callArguments.map { (e: Expression) =>
      e.endoRewrite(SensitiveParameterRewriter)
    }

    def implicitCallResults: IndexedSeq[ProcedureResultItem] =
      outputSignature.getOrElse(Seq.empty).map {
        field => ProcedureResultItem(Variable(field.name)(position, Variable.isIsolatedDefault))(position)
      }.toIndexedSeq

    val callResults = declaredResult.map(_.items).getOrElse(implicitCallResults)

    val callFilter = declaredResult.flatMap(_.where)
    if (callFilter.nonEmpty)
      throw new IllegalArgumentException(s"Expected no unresolved call with WHERE but got: $unresolved")
    else
      ResolvedLocalCall(
        procedureName,
        inputSignature,
        outputSignature,
        bodyContainsUpdates = false,
        callArgumentsWithSensitivityMarkers,
        callResults,
        declaredArguments.nonEmpty,
        declaredResult.nonEmpty,
        yieldAll,
        optional
      )(position)
  }
}

case class ResolvedLocalCall(
  override val procedureName: ProcedureName,
  inputSignature: Seq[LocalFieldSignature],
  outputSignature: Option[Seq[LocalFieldSignature]],
  bodyContainsUpdates: Boolean,
  override val callArguments: Seq[Expression],
  override val callResults: IndexedSeq[ProcedureResultItem],
  // true if given by the user originally
  override val declaredArguments: Boolean = true,
  // true if given by the user originally
  override val declaredResults: Boolean = true,
  // YIELD *
  override val yieldAll: Boolean = false,
  override val optionalState: OptionalState = NonOptional
)(val position: InputPosition) extends ResolvedCall[ResolvedLocalCall] {

  override def inputFieldSignatures: Seq[AbstractFieldSignature] = inputSignature

  override def outputFieldSignatures: Option[Seq[AbstractFieldSignature]] = outputSignature

  def fullyDeclared: Boolean = declaredArguments && declaredResults

  def withArguments(arguments: Seq[Expression]): ResolvedLocalCall = copy(callArguments = arguments)(position)

  def withFakedFullDeclarations: ResolvedLocalCall =
    // keep old YieldAll value for VOID procedure to be able to throw correct error if true
    copy(
      declaredArguments = true,
      declaredResults = true,
      yieldAll = if (outputSignature.isEmpty) yieldAll else false
    )(position)

  def mapCallArguments(f: Expression => Expression): ResolvedLocalCall =
    copy(callArguments = callArguments.map(f))(this.position)

  def withBodyContainsUpdates(newValue: Boolean): ResolvedLocalCall =
    copy(bodyContainsUpdates = newValue)(position)

  override def signatureString: String = {
    val sig = inputSignature.mkString(", ")
    val nameAndSig = s"${procedureName.fullName}($sig)"
    outputSignature.map(out => s"$nameAndSig :: ${out.mkString(", ")}").getOrElse(nameAndSig)
  }

  override val description: String = "(local procedure)"

  override def containsNoUpdates: Boolean = !bodyContainsUpdates

  override def asUnresolvedCall: UnresolvedCall = UnresolvedCall(
    procedureName = procedureName,
    declaredArguments = if (declaredArguments) Some(callArguments) else None,
    declaredResult = if (declaredResults) Some(ProcedureResult(callResults)(position)) else None,
    yieldAll,
    optional
  )(position)
}
