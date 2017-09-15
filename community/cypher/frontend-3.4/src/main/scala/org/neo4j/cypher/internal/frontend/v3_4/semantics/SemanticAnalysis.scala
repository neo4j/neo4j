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
package org.neo4j.cypher.internal.frontend.v3_4.semantics

import org.neo4j.cypher.internal.apa.v3_4.InputPosition
import org.neo4j.cypher.internal.frontend.v3_4.ast.Expression.{DefaultTypeMismatchMessageGenerator, SemanticContext}
import org.neo4j.cypher.internal.frontend.v3_4.ast.ReduceExpression.AccumulatorExpressionTypeMismatchMessageGenerator
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters.DesugaredMapProjection
import org.neo4j.cypher.internal.frontend.v3_4.symbols._
import org.neo4j.cypher.internal.frontend.v3_4.{SemanticCheck, TypeGenerator, ast}

import scala.util.Try

object SemanticAnalysis {

  val crashOnUnknownExpression: (SemanticContext, Expression) => SemanticCheck =
    (ctx, e) => throw new UnsupportedOperationException(s"Error in semantic analysis: Unknown expression $e")

  /**
    * This fallback allow for a testing backdoor to insert custom Expressions. Do not use in production.
    */
  var semanticCheckFallback: (SemanticContext, Expression) => SemanticCheck = crashOnUnknownExpression

  def semanticCheck(ctx: SemanticContext, expression: Expression): SemanticCheck =
    expression match {

        // ARITHMETICS

      case x:Add =>
        SemanticAnalysis.semanticCheck(ctx, x.lhs) chain
          expectType(TypeSpec.all, x.lhs) chain
          SemanticAnalysis.semanticCheck(ctx, x.rhs) chain
          expectType(infixRhsTypes(x.lhs), x.rhs) chain
          specifyType(infixOutputTypes(x.lhs, x.rhs), x) chain
          checkAddBoundary(x)

      case x:Subtract =>
        semanticCheck(ctx, x.arguments) chain
          checkTypes(x, x.signatures) chain
          checkSubtractBoundary(x)

      case x:UnarySubtract =>
        semanticCheck(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:Multiply =>
        semanticCheck(ctx, x.arguments) chain
          checkTypes(x, x.signatures) chain
          checkMultiplyBoundary(x)

      case x:Divide =>
        semanticCheck(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:Modulo =>
        semanticCheck(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:Pow =>
        semanticCheck(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

        // PREDICATES

      case x:Not =>
        semanticCheck(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:Equals =>
        semanticCheck(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:NotEquals =>
        semanticCheck(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:InvalidNotEquals =>
        SemanticError(
          "Unknown operation '!=' (you probably meant to use '<>', which is the operator for inequality testing)",
          x.position)

      case x:RegexMatch =>
        semanticCheck(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:And =>
        semanticCheck(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:Or =>
        semanticCheck(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:Xor =>
        semanticCheck(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:Ands =>
        semanticCheck(ctx, x.exprs)

      case x:Ors =>
        SemanticCheckResult.success

      case x:In =>
        semanticCheck(ctx, x.lhs) chain
          expectType(CTAny.covariant, x.lhs) chain
          semanticCheck(ctx, x.rhs) chain
          expectType(CTList(CTAny).covariant, x.rhs) chain
          specifyType(CTBoolean, x)

      case x:StartsWith =>
        semanticCheck(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:EndsWith =>
        semanticCheck(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:Contains =>
        semanticCheck(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:IsNull =>
        semanticCheck(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:IsNotNull =>
        semanticCheck(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:LessThan =>
        semanticCheck(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:LessThanOrEqual =>
        semanticCheck(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:GreaterThan =>
        semanticCheck(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:GreaterThanOrEqual =>
        semanticCheck(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:PartialPredicate[_] =>
        semanticCheck(ctx, x.coveredPredicate)

      //

      case x:CaseExpression =>
        val possibleTypes = unionOfTypes(x.possibleExpressions)
        SemanticAnalysis.semanticCheck(ctx, x.expression) chain
          semanticCheck(ctx, x.alternatives.flatMap { a => Seq(a._1, a._2) }) chain
          semanticCheck(ctx, x.default) chain
          when (x.expression.isEmpty) {
            expectType(CTBoolean.covariant, x.alternatives.map(_._1))
          } chain
          specifyType(possibleTypes, x)

      case x:AndedPropertyInequalities =>
        x.inequalities.map(semanticCheck(ctx, _)).reduceLeft(_ chain _)

      case x:CoerceTo =>
        semanticCheck(ctx, x.expr) chain expectType(x.typ.covariant, x.expr)

      case x:Property =>
        semanticCheck(ctx, x.map) chain
          expectType(CTMap.covariant | CTAny.invariant, x.map) chain
          specifyType(CTAny.covariant, x)

      // Check the variable is defined and, if not, define it so that later errors are suppressed
      // This is used in expressions; in graphs we must make sure to sem check variables explicitly (!)
      case x:Variable =>
        s => s.ensureVariableDefined(x) match {
          case Right(ss) => SemanticCheckResult.success(ss)
          case Left(error) => s.declareVariable(x, CTAny.covariant) match {
            // if the variable is a graph, declaring it will fail
            case Right(ss) => SemanticCheckResult.error(ss, error)
            case Left(_error) => SemanticCheckResult.error(s, _error)
          }
        }

      case x:FunctionInvocation =>
        x.function.semanticCheckHook(ctx, x)

      case x:GetDegree =>
        semanticCheck(ctx, x.node) chain
          expectType(CTMap.covariant | CTAny.invariant, x.node) chain
          specifyType(CTAny.covariant, x)

      case x:Parameter =>
        specifyType(x.parameterType.covariant, x)

      case x:HasLabels =>
        semanticCheck(ctx, x.expression) chain
          expectType(CTNode.covariant, x.expression) chain
          specifyType(CTBoolean, x)

        // ITERABLES

      case x:FilterExpression =>
        FilteringExpressions.checkPredicateDefined(x) chain
          FilteringExpressions.semanticCheck(ctx, x) chain
          specifyType(x.expression.types, x)

      case x:ExtractExpression =>
        FilteringExpressions.checkPredicateNotDefined(x) chain
          checkExtractExpressionDefined(x) chain
          FilteringExpressions.semanticCheck(ctx, x) chain
          checkInnerExtractExpression(x) chain
          FilteringExpressions.failIfAggregating(x.extractExpression)

      case x:ListComprehension =>
        FilteringExpressions.semanticCheck(ctx, x) chain
          checkInnerListComprehension(x) chain
          FilteringExpressions.failIfAggregating(x.extractExpression)

      case x:PatternComprehension =>
        SemanticState.recordCurrentScope(x) chain
          withScopedState {
            x.pattern.semanticCheck(Pattern.SemanticContext.Match) chain
              x.namedPath.map(declareVariable(_, CTPath): SemanticCheck).getOrElse(SemanticCheckResult.success) chain
              semanticCheck(SemanticContext.Simple, x.predicate) chain
              semanticCheck(SemanticContext.Simple, x.projection)
          } chain {
            val outerTypes: TypeGenerator = x.projection.types(_).wrapInList
            specifyType(outerTypes, x)
          }

      case _:FilterScope => SemanticCheckResult.success
      case _:ExtractScope => SemanticCheckResult.success
      case _:ReduceScope => SemanticCheckResult.success

      case x:CountStar =>
        specifyType(CTInteger, x)

      case x:PathExpression =>
        specifyType(CTPath, x)

      case x:ShortestPathExpression =>
        x.pattern.declareVariables(Pattern.SemanticContext.Expression) chain
          x.pattern.semanticCheck(Pattern.SemanticContext.Expression) chain
          specifyType(CTList(CTPath), x)

      case x:PatternExpression =>
        x.pattern.semanticCheck(Pattern.SemanticContext.Expression) chain
          specifyType(CTList(CTPath), x)

      case x:IterablePredicateExpression =>
        FilteringExpressions.checkPredicateDefined(x) chain
          FilteringExpressions.semanticCheck(ctx, x) chain
          specifyType(CTBoolean, x)

      case x:ReduceExpression =>
        semanticCheck(ctx, x.init) chain
          semanticCheck(ctx, x.list) chain
          expectType(CTList(CTAny).covariant, x.list) chain
          withScopedState {
            val indexType: TypeGenerator = s =>
              (x.list.types(s) constrain CTList(CTAny)).unwrapLists
            val accType: TypeGenerator = x.init.types

            declareVariable(x.variable, indexType) chain
              declareVariable(x.accumulator, accType) chain
              semanticCheck(SemanticContext.Simple, x.expression)
          } chain
          expectType(x.init.types, x.expression, AccumulatorExpressionTypeMismatchMessageGenerator) chain
          specifyType(s => x.init.types(s) leastUpperBounds x.expression.types(s), x) chain
          FilteringExpressions.failIfAggregating(x.expression)

      case x:ListLiteral =>
        def possibleTypes: TypeGenerator = state => x.expressions match {
          case Seq() => CTList(CTAny).covariant
          case _     => leastUpperBoundsOfTypes(x.expressions)(state).wrapInCovariantList
        }
        semanticCheck(ctx, x.expressions) chain specifyType(possibleTypes, x)

      case x:ListSlice =>
        semanticCheck(ctx, x.list) chain
          expectType(CTList(CTAny).covariant, x.list) chain
          when(x.from.isEmpty && x.to.isEmpty) {
            SemanticError("The start or end (or both) is required for a collection slice", x.position)
          } chain
          semanticCheck(ctx, x.from) chain
          expectType(CTInteger.covariant, x.from) chain
          semanticCheck(ctx, x.to) chain
          expectType(CTInteger.covariant, x.to) chain
          specifyType(x.list.types, x)

      case x:ContainerIndex =>
        semanticCheck(ctx, x.expr) chain
          semanticCheck(ctx, x.idx) chain
          typeSwitch(x.expr) {
            case exprT =>
              typeSwitch(x.idx) {
                case idxT =>
                  val listT = CTList(CTAny).covariant & exprT
                  val mapT = CTMap.covariant & exprT
                  val exprIsList = listT != TypeSpec.none
                  val exprIsMap = mapT != TypeSpec.none
                  val idxIsInteger = (CTInteger.covariant & idxT) != TypeSpec.none
                  val idxIsString = (CTString.covariant & idxT) != TypeSpec.none
                  val listLookup = exprIsList || idxIsInteger
                  val mapLookup = exprIsMap || idxIsString

                  if (listLookup && !mapLookup) {
                    expectType(CTList(CTAny).covariant, x.expr) chain
                      expectType(CTInteger.covariant, x.idx) chain
                      specifyType(x.expr.types(_).unwrapLists, x)
                  }
                  else if (!listLookup && mapLookup) {
                    expectType(CTMap.covariant, x.expr) chain
                      expectType(CTString.covariant, x.idx)
                  } else {
                    SemanticCheckResult.success
                  }
              }
          }

      // MAPS

      case x:MapExpression =>
        semanticCheck(ctx, x.items.map(_._2)) chain
          specifyType(CTMap, x)

      case x:MapProjection =>
        semanticCheck(ctx, x.items) chain
          specifyType(CTMap, x) ifOkChain // We need to remember the scope to later rewrite this ASTNode
          SemanticState.recordCurrentScope(x)

      case x:LiteralEntry =>
        semanticCheck(ctx, x.exp)

      case x:VariableSelector =>
        semanticCheck(ctx, x.id)

      case x:PropertySelector =>
        SemanticCheckResult.success

      case x:AllPropertiesSelector =>
        SemanticCheckResult.success

      case x:DesugaredMapProjection =>
        semanticCheck(ctx, x.items) chain
          ensureDefined(x.name) chain
          specifyType(CTMap, x) ifOkChain // We need to remember the scope to later rewrite this ASTNode
          SemanticState.recordCurrentScope(x)


        // LITERALS

      case x:DecimalIntegerLiteral =>
        when(!validNumber(x)) {
          if (x.stringVal matches "^-?[1-9][0-9]*$")
            SemanticError("integer is too large", x.position)
          else
            SemanticError("invalid literal number", x.position)
        } chain specifyType(CTInteger, x)

      case x:OctalIntegerLiteral =>
        when(!validNumber(x)) {
          if (x.stringVal matches "^-?0[0-7]+$")
            SemanticError("integer is too large", x.position)
          else
            SemanticError("invalid literal number", x.position)
        } chain specifyType(CTInteger, x)

      case x:HexIntegerLiteral =>
        when(!validNumber(x)) {
          if (x.stringVal matches "^-?0x[0-9a-fA-F]+$")
            SemanticError("integer is too large", x.position)
          else
            SemanticError("invalid literal number", x.position)
        } chain specifyType(CTInteger, x)

      case x:DecimalDoubleLiteral =>
        when(!validNumber(x)) {
          SemanticError("invalid literal number", x.position)
        } ifOkChain
        when(x.value.isInfinite) {
          SemanticError("floating point number is too large", x.position)
        } chain specifyType(CTFloat, x)

      case x:StringLiteral =>
        specifyType(CTString, x)

      case x:Null =>
        specifyType(CTAny.covariant, x)

      case x:BooleanLiteral =>
        specifyType(CTBoolean, x)

      case x:SemanticCheckableExpression =>
        x.semanticCheck(ctx)

      case x:Expression => semanticCheckFallback(ctx, x)
    }

  object FilteringExpressions {

    def semanticCheck(ctx: SemanticContext, e: FilteringExpression):SemanticCheck =
      SemanticAnalysis.semanticCheck(ctx, e.expression) chain
        expectType(CTList(CTAny).covariant, e.expression) chain
        checkInnerPredicate(e) chain
        failIfAggregating(e.innerPredicate)

    def failIfAggregating(expression: Option[Expression]): Option[SemanticError] =
      expression.flatMap(failIfAggregating)

    def failIfAggregating(expression: Expression): Option[SemanticError] =
      expression.findAggregate.map(
        aggregate =>
          SemanticError("Can't use aggregating expressions inside of expressions executing over lists", aggregate.position)
      )

    def checkPredicateDefined(e: FilteringExpression) =
      when (e.innerPredicate.isEmpty) {
        SemanticError(s"${e.name}(...) requires a WHERE predicate", e.position)
      }

    def checkPredicateNotDefined(e: FilteringExpression) =
      when (e.innerPredicate.isDefined) {
        SemanticError(s"${e.name}(...) should not contain a WHERE predicate", e.position)
      }

    private def checkInnerPredicate(e: FilteringExpression): SemanticCheck =
      e.innerPredicate match {
      case Some(predicate) => withScopedState {
        declareVariable(e.variable, possibleInnerTypes(e)) chain
        SemanticAnalysis.semanticCheck(SemanticContext.Simple, predicate)
      }
      case None    => SemanticCheckResult.success
    }

    def possibleInnerTypes(e: FilteringExpression): TypeGenerator = s =>
      (e.expression.types(s) constrain CTList(CTAny)).unwrapLists
  }

  def semanticCheck(
                     ctx: SemanticContext,
                     traversable: Traversable[Expression]
                   ): SemanticCheck =
    semanticCheckFold(traversable)(expr => semanticCheck(ctx, expr))

  def semanticCheckFold[Exp <: Expression](
                     traversable: Traversable[Exp]
                   )(
                    f:Exp => SemanticCheck
  ): SemanticCheck =
    state => traversable.foldLeft(SemanticCheckResult.success(state)){
      (r1:SemanticCheckResult, o:Exp) => {
        val r2 = f(o)(r1.state)
        SemanticCheckResult(r2.state, r1.errors ++ r2.errors)
      }
    }

  def semanticCheck(ctx: SemanticContext, option: Option[Expression]): SemanticCheck =
    option.fold(SemanticCheckResult.success) {
      semanticCheck(ctx, _)
    }

  private def checkMultiplyBoundary(multiply: Multiply): SemanticCheck =
    (multiply.lhs, multiply.rhs) match {
      case (l:IntegerLiteral, r:IntegerLiteral) if Try(Math.multiplyExact(l.value, r.value)).isFailure =>
        SemanticError(s"result of ${l.value} * ${r.value} cannot be represented as an integer", multiply.position)
      case _ => SemanticCheckResult.success
    }

  private def checkSubtractBoundary(subtract: Subtract): SemanticCheck =
    (subtract.lhs, subtract.rhs) match {
      case (l:IntegerLiteral, r:IntegerLiteral) if Try(Math.subtractExact(l.value, r.value)).isFailure =>
        SemanticError(s"result of ${l.value} - ${r.value} cannot be represented as an integer", subtract.position)
      case _ => SemanticCheckResult.success
    }

  private def checkAddBoundary(add: Add): SemanticCheck =
    (add.lhs, add.rhs) match {
      case (l:IntegerLiteral, r:IntegerLiteral) if Try(Math.addExact(l.value, r.value)).isFailure =>
        SemanticError(s"result of ${l.value} + ${r.value} cannot be represented as an integer", add.position)
      case _ => SemanticCheckResult.success
    }

  private def infixRhsTypes(lhs: ast.Expression): TypeGenerator = s => {
    val lhsTypes = lhs.types(s)

    // Strings
    // "a" + "b" => "ab"
    // "a" + 1 => "a1"
    // "a" + 1.1 => "a1.1"
    // Numbers
    // 1 + "b" => "1b"
    // 1 + 1 => 2
    // 1 + 1.1 => 2.1
    // 1.1 + "b" => "1.1b"
    // 1.1 + 1 => 2.1
    // 1.1 + 1.1 => 2.2
    val valueTypes =
      if (lhsTypes containsAny (CTInteger.covariant | CTFloat.covariant | CTString.covariant))
        CTString.covariant | CTInteger.covariant | CTFloat.covariant
      else
        TypeSpec.none

    // [a] + [b] => [a, b]
    val listTypes = lhsTypes constrain CTList(CTAny)

    // [a] + b => [a, b]
    val lhsListTypes = listTypes | listTypes.unwrapLists

    // a + [b] => [a, b]
    val rhsListTypes = lhsTypes.wrapInList

    valueTypes | lhsListTypes | rhsListTypes
  }

  private def infixOutputTypes(lhs: ast.Expression, rhs: ast.Expression): TypeGenerator = s => {
    val lhsTypes = lhs.types(s)
    val rhsTypes = rhs.types(s)

    def when(fst: TypeSpec, snd: TypeSpec)(result: CypherType): TypeSpec =
      if (lhsTypes.containsAny(fst) && rhsTypes.containsAny(snd) || lhsTypes.containsAny(snd) && rhsTypes.containsAny(fst))
        result.invariant
      else
        TypeSpec.none

    // "a" + "b" => "ab"
    // "a" + 1 => "a1"
    // "a" + 1.1 => "a1.1"
    // 1 + "b" => "1b"
    // 1.1 + "b" => "1.1b"
    val stringTypes: TypeSpec =
    when(CTString.covariant, CTInteger.covariant | CTFloat.covariant | CTString.covariant)(CTString)

    // 1 + 1 => 2
    // 1 + 1.1 => 2.1
    // 1.1 + 1 => 2.1
    // 1.1 + 1.1 => 2.2
    val numberTypes: TypeSpec =
    when(CTInteger.covariant, CTInteger.covariant)(CTInteger) |
      when(CTFloat.covariant, CTFloat.covariant | CTInteger.covariant)(CTFloat)

    val listTypes = {
      val lhsListTypes = lhsTypes constrain CTList(CTAny)
      val rhsListTypes = rhsTypes constrain CTList(CTAny)
      val lhsListInnerTypes = lhsListTypes.unwrapLists
      val rhsListInnerTypes = rhsListTypes.unwrapLists

      // [a] + [b] => [a, b]
      (lhsListTypes intersect rhsListTypes) |
        // [a] + b => [a, b]
        (rhsTypes intersectOrCoerce lhsListInnerTypes).wrapInList |
        // a + [b] => [a, b]
        (lhsTypes intersectOrCoerce rhsListInnerTypes).wrapInList
    }

    stringTypes | numberTypes | listTypes
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
  def expectType[Exp <: Expression](possibleTypes: TypeSpec, expressions:Traversable[Exp])
  :SemanticCheck =
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

  def checkTypes(expression: Expression, signatures: Seq[ExpressionSignature]): SemanticCheck = s => {
    val initSignatures = signatures.filter(_.argumentTypes.length == expression.arguments.length)

    val (remainingSignatures: Seq[ExpressionSignature], result) =
      expression.arguments.foldLeft((initSignatures, SemanticCheckResult.success(s))) {
        case (accumulator@(Seq(), _), _) =>
          accumulator
        case ((possibilities, r1), arg)  =>
          val argTypes = possibilities.foldLeft(TypeSpec.none) { _ | _.argumentTypes.head.covariant }
          val r2 = expectType(argTypes, arg)(r1.state)

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
    TypeSpec.union(traversable.map(_.types(state)).toSeq: _*)

  def leastUpperBoundsOfTypes(traversable: TraversableOnce[Expression]): TypeGenerator =
    if (traversable.isEmpty)
      _ => CTAny.invariant
    else
      state => traversable.map { _.types(state) } reduce { _ leastUpperBounds _ }

  val pushStateScope: SemanticCheck = state => SemanticCheckResult.success(state.newChildScope)
  val popStateScope: SemanticCheck = state => SemanticCheckResult.success(state.popScope)
  def withScopedState(check: => SemanticCheck): SemanticCheck =
    pushStateScope chain check chain popStateScope

  def typeSwitch(expr: Expression)(choice: TypeSpec => SemanticCheck): SemanticCheck =
    (state: SemanticState) => choice(state.expressionType(expr).actual)(state)

  def validNumber(long:IntegerLiteral) =
    try {
      long.value.isInstanceOf[Long]
    } catch {
      case e:java.lang.NumberFormatException => false
    }
  def validNumber(double:DoubleLiteral) =
    try {
      double.value.isInstanceOf[Double]
    } catch {
      case e:java.lang.NumberFormatException => false
    }

  def ensureDefined(v:Variable): (SemanticState) => Either[SemanticError, SemanticState] =
    (_: SemanticState).ensureVariableDefined(v)

  def ensureGraphDefined(v:Variable): SemanticCheck = {
    val ensured = (_: SemanticState).ensureGraphDefined(v)
    ensured chain expectType(CTGraphRef.covariant, v)
  }

  def declareVariable(v:Variable, possibleTypes: TypeSpec): (SemanticState) => Either[SemanticError, SemanticState] =
    (_: SemanticState).declareVariable(v, possibleTypes)

  def declareVariable(
                       v:Variable,
                       typeGen: TypeGenerator,
                       positions: Set[InputPosition] = Set.empty
                     ): (SemanticState) => Either[SemanticError, SemanticState] =
    (s: SemanticState) => s.declareVariable(v, typeGen(s), positions)

  def checkExtractExpressionDefined(x: ExtractExpression): SemanticCheck =
    when (x.scope.extractExpression.isEmpty) {
      SemanticError(s"${x.name}(...) requires '| expression' (an extract expression)", x.position)
    }

  def checkInnerExtractExpression(x: ExtractExpression): SemanticCheck =
    x.scope.extractExpression.fold(SemanticCheckResult.success) {
      e => withScopedState {
        declareVariable(x.variable, FilteringExpressions.possibleInnerTypes(x)) chain
          semanticCheck(SemanticContext.Simple, e)
      } chain {
        val outerTypes: TypeGenerator = e.types(_).wrapInList
        specifyType(outerTypes, x)
      }
    }

  def checkInnerListComprehension(x: ListComprehension): SemanticCheck =
    x.extractExpression match {
      case Some(e) =>
        withScopedState {
          declareVariable(x.variable, FilteringExpressions.possibleInnerTypes(x)) chain
            semanticCheck(SemanticContext.Simple, e)
        } chain {
          val outerTypes: TypeGenerator = e.types(_).wrapInList
          specifyType(outerTypes, x)
        }
      case None =>
        specifyType(x.expression.types, x)
    }
}

