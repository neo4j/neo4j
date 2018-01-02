/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_4.semantics

import org.neo4j.cypher.internal.util.v3_4.InputPosition
import org.neo4j.cypher.internal.util.v3_4.symbols.{TypeSpec, _}
import org.neo4j.cypher.internal.frontend.v3_4.{SemanticCheck, TypeGenerator}
import org.neo4j.cypher.internal.v3_4.expressions.Expression.{DefaultTypeMismatchMessageGenerator, SemanticContext}
import org.neo4j.cypher.internal.v3_4.expressions._

/**
  * This class holds methods for performing semantic analysis.
  */
trait SemanticAnalysisTooling {

  def semanticCheckFold[A](
                     traversable: Traversable[A]
                   )(
                    f:A => SemanticCheck
  ): SemanticCheck =
    state => traversable.foldLeft(SemanticCheckResult.success(state)){
      (r1:SemanticCheckResult, o:A) => {
        val r2 = f(o)(r1.state)
        SemanticCheckResult(r2.state, r1.errors ++ r2.errors)
      }
    }

  def semanticCheck[A <: SemanticCheckable](traversable: TraversableOnce[A]): SemanticCheck =
    state => traversable.foldLeft(SemanticCheckResult.success(state)){
      (r1:SemanticCheckResult, o:A) => {
        val r2 = o.semanticCheck(r1.state)
        SemanticCheckResult(r2.state, r1.errors ++ r2.errors)
      }
    }

  def specifyType(typeGen: TypeGenerator, expression: Expression): SemanticState => Either[SemanticError, SemanticState] =
    s => specifyType(typeGen(s), expression)(s)

  def specifyType(possibleTypes: => TypeSpec, expression: Expression): SemanticState => Either[SemanticError, SemanticState] =
    _.specifyType(expression, possibleTypes)

  def expectType(typeGen: TypeGenerator, expression: Expression): SemanticCheck =
    s => expectType(typeGen(s), expression)(s)

  def expectType(possibleTypes: TypeSpec, opt: Option[Expression]): SemanticCheck =
    opt.map(expectType(possibleTypes, _)).getOrElse(SemanticCheckResult.success)

  def expectType(typeGen: TypeGenerator, expression: Expression, messageGen: (String, String) => String): SemanticCheck =
    s => expectType(typeGen(s), expression, messageGen)(s)

  def expectType[Exp <: Expression](possibleTypes: TypeSpec, expressions:Traversable[Exp]):SemanticCheck =
    state => expressions.foldLeft(SemanticCheckResult.success(state)){
      (r1:SemanticCheckResult, o:Exp) => {
        val r2 = expectType(possibleTypes, o)(r1.state)
        SemanticCheckResult(r2.state, r1.errors ++ r2.errors)
      }
    }

  def expectType(
                  possibleTypes: => TypeSpec
                )(
                  ctx: SemanticContext,
                  expr:Expression
  ): SemanticCheck = expectType(possibleTypes, expr)

  def expectType(
                  possibleTypes: => TypeSpec,
                  expression: Expression,
                  messageGen: (String, String) => String = DefaultTypeMismatchMessageGenerator
                ): SemanticCheck = s => {
    s.expectType(expression, possibleTypes) match {
      case (ss, TypeSpec.none) =>
        val existingTypesString = ss.expressionType(expression).specified.mkString(", ", " or ")
        val expectedTypesString = possibleTypes.mkString(", ", " or ")
        SemanticCheckResult.error(ss,
          SemanticError("Type mismatch: " + messageGen(expectedTypesString, existingTypesString), expression.position))
      case (ss, _)             =>
        SemanticCheckResult.success(ss)
    }
  }

  def checkTypes(expression: Expression, signatures: Seq[TypeSignature]): SemanticCheck = s => {
    val initSignatures = signatures.filter(_.argumentTypes.length == expression.arguments.length)

    val (remainingSignatures: Seq[TypeSignature], result) =
      expression.arguments.foldLeft((initSignatures, SemanticCheckResult.success(s))) {
        case (accumulator@(Seq(), _), _) =>
          accumulator
        case ((possibilities, r1), arg)  =>
          val argTypes = possibilities.foldLeft(TypeSpec.none) { _ | _.argumentTypes.head.covariant }
          val r2 = expectType(argTypes, arg)(r1.state)

          val actualTypes = types(arg)(r2.state)
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

    specifyType(outputType, expression)(result.state) match {
      case Left(err)    => SemanticCheckResult(result.state, result.errors :+ err)
      case Right(state) => SemanticCheckResult(state, result.errors)
    }
  }

  def when(condition: Boolean)(check: => SemanticCheck): SemanticCheck = state =>
    if (condition)
      check(state)
    else
      SemanticCheckResult.success(state)

  def unless(condition: Boolean)(check: => SemanticCheck): SemanticCheck = state =>
    if (condition)
      SemanticCheckResult.success(state)
    else
      check(state)

  def unionOfTypes(traversable: TraversableOnce[Expression]): TypeGenerator = state =>
    TypeSpec.union(traversable.map(types(_)(state)).toSeq: _*)

  def leastUpperBoundsOfTypes(traversable: TraversableOnce[Expression]): TypeGenerator =
    if (traversable.isEmpty)
      _ => CTAny.invariant
    else
      state => traversable.map { types(_)(state) } reduce { _ leastUpperBounds _ }

  val pushStateScope: SemanticCheck = state => SemanticCheckResult.success(state.newChildScope)
  val popStateScope: SemanticCheck = state => SemanticCheckResult.success(state.popScope)
  def withScopedState(check: => SemanticCheck): SemanticCheck =
    pushStateScope chain check chain popStateScope

  def typeSwitch(expr: Expression)(choice: TypeSpec => SemanticCheck): SemanticCheck =
    (state: SemanticState) => choice(state.expressionType(expr).actual)(state)

  def validNumber(long:IntegerLiteral): Boolean =
    try {
      long.value.isInstanceOf[Long]
    } catch {
      case e:java.lang.NumberFormatException => false
    }

  def validNumber(double:DoubleLiteral): Boolean =
    try {
      double.value.isInstanceOf[Double]
    } catch {
      case e:java.lang.NumberFormatException => false
    }

  def ensureDefined(v:LogicalVariable): (SemanticState) => Either[SemanticError, SemanticState] =
    (_: SemanticState).ensureVariableDefined(v)

  def declareVariable(v:LogicalVariable, possibleTypes: TypeSpec): (SemanticState) => Either[SemanticError, SemanticState] =
    (_: SemanticState).declareVariable(v, possibleTypes)

  def declareVariable(
                       v:LogicalVariable,
                       typeGen: TypeGenerator,
                       positions: Set[InputPosition] = Set.empty
                     ): (SemanticState) => Either[SemanticError, SemanticState] =
    (s: SemanticState) => s.declareVariable(v, typeGen(s), positions)

  def implicitVariable(v:LogicalVariable, possibleType: CypherType): (SemanticState) => Either[SemanticError, SemanticState] =
    (_: SemanticState).implicitVariable(v, possibleType)

  def declareGraph(v:Variable): (SemanticState) => Either[SemanticError, SemanticState] =
    (_: SemanticState).declareGraph(v)

  def declareGraphMarkedAsGenerated(v:Variable): SemanticCheck = {
    val declare = (_: SemanticState).declareGraph(v)
    val mark = (_: SemanticState).localMarkAsGenerated(v)
    declare chain mark
  }

  def implicitGraph(v:Variable): (SemanticState) => Either[SemanticError, SemanticState] =
    (_: SemanticState).implicitGraph(v)

  def ensureGraphDefined(v:Variable): SemanticCheck = {
    val ensured = (_: SemanticState).ensureGraphDefined(v)
    ensured chain expectType(CTGraphRef.covariant, v)
  }

  def requireMultigraphSupport(msg: String, position: InputPosition): SemanticCheck =
    s => {
      if(!s.features(SemanticFeature.MultipleGraphs))
        SemanticCheckResult(s,
          List(FeatureError(s"$msg is not available in this implementation of Cypher " +
                              "due to lack of support for multiple graphs.", position)))
      else
        SemanticCheckResult.success(s)
  }

  def error(msg: String, position: InputPosition)(state: SemanticState): SemanticCheckResult =
    SemanticCheckResult(state, Vector(SemanticError(msg, position)))

  def possibleTypes(expression: Expression) : TypeGenerator =
    types(expression)(_).unwrapLists

  def types(expression:Expression): TypeGenerator = _.expressionType(expression).actual
}

