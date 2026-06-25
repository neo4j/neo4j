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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.semantics.*
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.success
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.when
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult.error
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckableExpression
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.CoerceTo
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Expression.SemanticContext
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionInvocationLike
import org.neo4j.cypher.internal.expressions.functions.UserDefinedFunctionInvocation
import org.neo4j.cypher.internal.expressions.functions.{Function => BuiltInFunction}
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.ZippableUtil.Zippable
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.gqlstatus.GqlHelper

import java.util.Locale

object ResolvedFunctionInvocation {

  def fromUnresolved(
    signatureLookup: FunctionName => Option[UserFunctionSignature],
    otherVersionLookup: FunctionName => Option[UserFunctionSignature] = _ => None,
    otherVersion: CypherVersion = CypherVersion.Cypher25
  )(unresolved: FunctionInvocation): ResolvedFunctionInvocation = {
    val position = unresolved.position
    val name = unresolved.functionName
    val signature = signatureLookup(name)
    val args = signature.map(obfuscateArgs(_, unresolved.args)).getOrElse(unresolved.args)
    val otherBuiltInFunction: Option[BuiltInFunction] =
      BuiltInFunction.scopedLookup(otherVersion).get(name.fullName.toLowerCase(Locale.ROOT))
    val otherVersionIfExists: Option[CypherVersion] = {
      if (otherBuiltInFunction.isDefined) Some(otherVersion)
      else if (signature.isEmpty) otherVersionLookup(name).map(_ => otherVersion)
      else None
    }

    ResolvedFunctionInvocation(name, signature, args, otherVersionIfExists)(position)
  }

  def obfuscateArgs(signature: UserFunctionSignature, args: IndexedSeq[Expression]): IndexedSeq[Expression] = {
    args.zipLeft(signature.inputSignature, null).map {
      case (exp: Expression, fieldSignature: FieldSignature) if fieldSignature.sensitive =>
        exp.endoRewrite(SensitiveParameterRewriter)
      case (exp, _) => exp
    }
  }
}

/**
 * A ResolvedFunctionInvocation is a user-defined function where the signature
 * has been resolved, i.e. verified that it exists in the database
 *
 * @param functionName The qualified name of the function.
 * @param fcnSignature Either `Some(signature)` if the signature was resolved, or
 *                     `None` if the function didn't exist
 * @param callArguments The argument list to the function
 * @param position The position in the original query string.
 */
case class ResolvedFunctionInvocation(
  override val functionName: FunctionName,
  fcnSignature: Option[UserFunctionSignature],
  override val callArguments: IndexedSeq[Expression],
  otherCypherVersionIfExists: Option[CypherVersion] = None
)(val position: InputPosition)
    extends Expression with FunctionInvocationLike with UserDefinedFunctionInvocation with SemanticCheckableExpression {

  override def isUserDefined: Boolean = fcnSignature.exists(!_.builtIn)

  override def isBuiltIn: Boolean = fcnSignature.exists(_.builtIn)

  def coerceArguments: ResolvedFunctionInvocation = fcnSignature match {
    case Some(signature) =>
      val optInputFields = signature.inputSignature.map(Some(_))
      val coercedArguments =
        callArguments.zipLeft(optInputFields, None)
          .map {
            case (arg, optField) =>
              // If type is CTAny we don't need any coercion
              optField.map { field => if (field.typ == symbols.CTAny) arg else CoerceTo(arg, field.typ) }.getOrElse(arg)
          }
      copy(callArguments = coercedArguments)(position)

    case None => this
  }

  override def semanticCheck(ctx: SemanticContext): SemanticCheck =
    signatureCheck chain illegalAggregationCheck(ctx)

  private def signatureCheck: SemanticCheck = fcnSignature match {
    case None =>
      functionName match {
        case fn: FunctionName if fn.name.equalsIgnoreCase("not") =>
          SemanticError.unknownFunctionNamedNot(position)
        case FunctionName(ns, qn)
          if ns.parts.isEmpty && qn.toLowerCase(Locale.ROOT) == "distance" =>
          SemanticError(
            GqlHelper.getGql42001_42N48("distance", position.offset, position.line, position.column),
            "'distance' has been replaced by 'point.distance'",
            position
          )
        case _ =>
          otherCypherVersionIfExists match {
            case Some(otherVersion) =>
              SemanticError.unknownFunctionWithVersionHint(functionName.fullName, otherVersion, position)
            case None =>
              SemanticError.unknownFunction(functionName.fullName, position)
          }
      }
    case Some(signature) =>
      // `graph.names()` and `graph.propertiesByName()` are only valid when not targeting a constituent graph.
      val fullName = functionName.fullName
      val graphFunctionCheck: SemanticCheck =
        if (
          fullName.equalsIgnoreCase("graph.names") ||
          fullName.equalsIgnoreCase("graph.propertiesByName")
        ) {
          SemanticCheck.fromState { state =>
            if (state.workingGraph.nonEmpty) {
              SemanticError(
                GqlHelper.getGql42001_42N72(position.offset, position.line, position.column),
                "Calling %s() is only supported on composite databases.".formatted(fullName),
                position
              )
            } else success
          }
        } else success

      val expectedNumArgs = signature.inputSignature.length
      val usedDefaultArgs = signature.inputSignature.drop(callArguments.length).flatMap(_.default)
      val actualNumArgs = callArguments.length + usedDefaultArgs.length

      val argCheck: SemanticCheck = if (expectedNumArgs == actualNumArgs) {
        // this zip is fine since it will only verify provided args in callArguments
        // default values are checked at load time
        signature.inputSignature.zip(callArguments).map {
          case (field, arg) =>
            SemanticExpressionCheck.check(SemanticContext.Results, arg) chain
              SemanticExpressionCheck.expectType(field.typ.covariant, arg)
        }.foldLeft(success)(_ chain _) chain
          SemanticExpressionCheck.specifyType(signature.outputType.covariant, this)
      } else {
        val msg =
          (if (signature.inputSignature.isEmpty) "arguments"
           else if (signature.inputSignature.size == 1)
             s"argument of type ${signature.inputSignature.head.typ.normalizedCypherTypeString()}"
           else
             s"arguments of type ${signature.inputSignature.map(_.typ.normalizedCypherTypeString()).mkString(", ")}") +
            signature.description.map(d => s"${System.lineSeparator()}Description: $d").getOrElse("")
        error(
          _: SemanticState,
          SemanticError.functionCallWrongNumberOfArguments(
            expectedNumArgs,
            actualNumArgs,
            signature.name.fullName,
            String.valueOf(signature),
            msg,
            position
          )
        )
      }

      graphFunctionCheck chain argCheck
  }

  private def checkNoNestedAggregateFunctions: SemanticCheck =
    arguments.collectFirst {
      case expr if expr.containsAggregate => expr.findAggregate.get
    } foldSemanticCheck {
      val prettifier = ExpressionStringifier()
      expr =>
        SemanticCheck.error(SemanticError.aggregateExpressionsNotAllowedInAggregationFunctions(
          prettifier(expr),
          expr.position
        ))
    }

  private def illegalAggregationCheck(ctx: SemanticContext): SemanticCheck = {
    when(isAggregate) {
      when(ctx == Expression.SemanticContext.Simple) {
        SemanticCheck.error(
          SemanticError.aggregateExpressionsNotAllowedInSimpleExpressions(
            functionName.fullName,
            functionName.name,
            position
          )
        )
      } chain checkNoNestedAggregateFunctions
    }
  }

  override def isAggregate: Boolean = fcnSignature.exists(_.isAggregate)

  override def asUnresolvedFunction: FunctionInvocation = FunctionInvocation(
    functionName = functionName,
    distinct = false,
    args = arguments.toIndexedSeq
  )(position)

  override def isConstantForQuery: Boolean = false
}
