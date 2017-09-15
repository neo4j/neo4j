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

import org.neo4j.cypher.internal.frontend.v3_4.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_4.ast.ReduceExpression.AccumulatorExpressionTypeMismatchMessageGenerator
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters.DesugaredMapProjection
import org.neo4j.cypher.internal.frontend.v3_4.symbols._
import org.neo4j.cypher.internal.frontend.v3_4.{SemanticCheck, TypeGenerator, ast}

import scala.util.Try

object SemanticExpressionCheck extends SemanticAnalysisTooling {

  val crashOnUnknownExpression: (SemanticContext, Expression) => SemanticCheck =
    (ctx, e) => throw new UnsupportedOperationException(s"Error in semantic analysis: Unknown expression $e")

  /**
    * This fallback allow for a testing backdoor to insert custom Expressions. Do not use in production.
    */
  var semanticCheckFallback: (SemanticContext, Expression) => SemanticCheck = crashOnUnknownExpression

  /**
    * Build a semantic check for the given expression using the simple expression context.
    */
  def simple(expression: Expression): SemanticCheck = check(SemanticContext.Simple, expression)

  /**
    * Build a semantic check for the given expression and context.
    */
  def check(ctx: SemanticContext, expression: Expression): SemanticCheck =
    expression match {

        // ARITHMETICS

      case x:Add =>
        check(ctx, x.lhs) chain
          expectType(TypeSpec.all, x.lhs) chain
          check(ctx, x.rhs) chain
          expectType(infixAddRhsTypes(x.lhs), x.rhs) chain
          specifyType(infixAddOutputTypes(x.lhs, x.rhs), x) chain
          checkAddBoundary(x)

      case x:Subtract =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures) chain
          checkSubtractBoundary(x)

      case x:UnarySubtract =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:Multiply =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures) chain
          checkMultiplyBoundary(x)

      case x:Divide =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:Modulo =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:Pow =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

        // PREDICATES

      case x:Not =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:Equals =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:NotEquals =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:InvalidNotEquals =>
        SemanticError(
          "Unknown operation '!=' (you probably meant to use '<>', which is the operator for inequality testing)",
          x.position)

      case x:RegexMatch =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:And =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:Or =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:Xor =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:Ands =>
        check(ctx, x.exprs)

      case x:Ors =>
        SemanticCheckResult.success

      case x:In =>
        check(ctx, x.lhs) chain
          expectType(CTAny.covariant, x.lhs) chain
          check(ctx, x.rhs) chain
          expectType(CTList(CTAny).covariant, x.rhs) chain
          specifyType(CTBoolean, x)

      case x:StartsWith =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:EndsWith =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:Contains =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:IsNull =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:IsNotNull =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:LessThan =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:LessThanOrEqual =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:GreaterThan =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:GreaterThanOrEqual =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x:PartialPredicate[_] =>
        check(ctx, x.coveredPredicate)

      //

      case x:CaseExpression =>
        val possibleTypes = unionOfTypes(x.possibleExpressions)
        SemanticExpressionCheck.check(ctx, x.expression) chain
          check(ctx, x.alternatives.flatMap { a => Seq(a._1, a._2) }) chain
          check(ctx, x.default) chain
          when (x.expression.isEmpty) {
            expectType(CTBoolean.covariant, x.alternatives.map(_._1))
          } chain
          specifyType(possibleTypes, x)

      case x:AndedPropertyInequalities =>
        x.inequalities.map(check(ctx, _)).reduceLeft(_ chain _)

      case x:CoerceTo =>
        check(ctx, x.expr) chain expectType(x.typ.covariant, x.expr)

      case x:Property =>
        check(ctx, x.map) chain
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
        SemanticFunctionCheck.check(ctx, x)

      case x:GetDegree =>
        check(ctx, x.node) chain
          expectType(CTMap.covariant | CTAny.invariant, x.node) chain
          specifyType(CTAny.covariant, x)

      case x:Parameter =>
        specifyType(x.parameterType.covariant, x)

      case x:HasLabels =>
        check(ctx, x.expression) chain
          expectType(CTNode.covariant, x.expression) chain
          specifyType(CTBoolean, x)

        // ITERABLES

      case x:FilterExpression =>
        FilteringExpressions.checkPredicateDefined(x) chain
          FilteringExpressions.semanticCheck(ctx, x) chain
          specifyType(types(x.expression), x)

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
              check(SemanticContext.Simple, x.predicate) chain
              check(SemanticContext.Simple, x.projection)
          } chain {
            val outerTypes: TypeGenerator = types(x.projection)(_).wrapInList
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
        check(ctx, x.init) chain
          check(ctx, x.list) chain
          expectType(CTList(CTAny).covariant, x.list) chain
          withScopedState {
            val indexType: TypeGenerator = s =>
              (types(x.list)(s) constrain CTList(CTAny)).unwrapLists
            val accType: TypeGenerator = types(x.init)

            declareVariable(x.variable, indexType) chain
              declareVariable(x.accumulator, accType) chain
              check(SemanticContext.Simple, x.expression)
          } chain
          expectType(types(x.init), x.expression, AccumulatorExpressionTypeMismatchMessageGenerator) chain
          specifyType(s => types(x.init)(s) leastUpperBounds types(x.expression)(s), x) chain
          FilteringExpressions.failIfAggregating(x.expression)

      case x:ListLiteral =>
        def possibleTypes: TypeGenerator = state => x.expressions match {
          case Seq() => CTList(CTAny).covariant
          case _     => leastUpperBoundsOfTypes(x.expressions)(state).wrapInCovariantList
        }
        check(ctx, x.expressions) chain specifyType(possibleTypes, x)

      case x:ListSlice =>
        check(ctx, x.list) chain
          expectType(CTList(CTAny).covariant, x.list) chain
          when(x.from.isEmpty && x.to.isEmpty) {
            SemanticError("The start or end (or both) is required for a collection slice", x.position)
          } chain
          check(ctx, x.from) chain
          expectType(CTInteger.covariant, x.from) chain
          check(ctx, x.to) chain
          expectType(CTInteger.covariant, x.to) chain
          specifyType(types(x.list), x)

      case x:ContainerIndex =>
        check(ctx, x.expr) chain
          check(ctx, x.idx) chain
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
                      specifyType(types(x.expr)(_).unwrapLists, x)
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
        check(ctx, x.items.map(_._2)) chain
          specifyType(CTMap, x)

      case x:MapProjection =>
        check(ctx, x.items) chain
          specifyType(CTMap, x) ifOkChain // We need to remember the scope to later rewrite this ASTNode
          SemanticState.recordCurrentScope(x)

      case x:LiteralEntry =>
        check(ctx, x.exp)

      case x:VariableSelector =>
        check(ctx, x.id)

      case x:PropertySelector =>
        SemanticCheckResult.success

      case x:AllPropertiesSelector =>
        SemanticCheckResult.success

      case x:DesugaredMapProjection =>
        check(ctx, x.items) chain
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

  /**
    * Build a semantic check over a traversable of expressions.
    */
  def simple(traversable: Traversable[Expression]): SemanticCheck = check(SemanticContext.Simple, traversable)

  def check(
             ctx: SemanticContext,
             traversable: Traversable[Expression]
           ): SemanticCheck =
    semanticCheckFold(traversable)(expr => check(ctx, expr))

  /**
    * Build a semantic check over an optional expression.
    */
  def simple(option: Option[Expression]): SemanticCheck = check(SemanticContext.Simple, option)

  def check(ctx: SemanticContext, option: Option[Expression]): SemanticCheck =
    option.fold(SemanticCheckResult.success) {
      check(ctx, _)
    }

  object FilteringExpressions {

    def semanticCheck(ctx: SemanticContext, e: FilteringExpression):SemanticCheck =
      SemanticExpressionCheck.check(ctx, e.expression) chain
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

    def checkPredicateDefined(e: FilteringExpression): SemanticCheck =
      when (e.innerPredicate.isEmpty) {
        SemanticError(s"${e.name}(...) requires a WHERE predicate", e.position)
      }

    def checkPredicateNotDefined(e: FilteringExpression): SemanticCheck =
      when (e.innerPredicate.isDefined) {
        SemanticError(s"${e.name}(...) should not contain a WHERE predicate", e.position)
      }

    private def checkInnerPredicate(e: FilteringExpression): SemanticCheck =
      e.innerPredicate match {
      case Some(predicate) => withScopedState {
        declareVariable(e.variable, possibleInnerTypes(e)) chain
        SemanticExpressionCheck.check(SemanticContext.Simple, predicate)
      }
      case None    => SemanticCheckResult.success
    }

    def possibleInnerTypes(e: FilteringExpression): TypeGenerator = s =>
      (types(e.expression)(s) constrain CTList(CTAny)).unwrapLists
  }

  private def checkAddBoundary(add: Add): SemanticCheck =
    (add.lhs, add.rhs) match {
      case (l:IntegerLiteral, r:IntegerLiteral) if Try(Math.addExact(l.value, r.value)).isFailure =>
        SemanticError(s"result of ${l.value} + ${r.value} cannot be represented as an integer", add.position)
      case _ => SemanticCheckResult.success
    }

  private def checkSubtractBoundary(subtract: Subtract): SemanticCheck =
    (subtract.lhs, subtract.rhs) match {
      case (l:IntegerLiteral, r:IntegerLiteral) if Try(Math.subtractExact(l.value, r.value)).isFailure =>
        SemanticError(s"result of ${l.value} - ${r.value} cannot be represented as an integer", subtract.position)
      case _ => SemanticCheckResult.success
    }

  private def checkMultiplyBoundary(multiply: Multiply): SemanticCheck =
    (multiply.lhs, multiply.rhs) match {
      case (l:IntegerLiteral, r:IntegerLiteral) if Try(Math.multiplyExact(l.value, r.value)).isFailure =>
        SemanticError(s"result of ${l.value} * ${r.value} cannot be represented as an integer", multiply.position)
      case _ => SemanticCheckResult.success
    }

  private def infixAddRhsTypes(lhs: ast.Expression): TypeGenerator = s => {
    val lhsTypes = types(lhs)(s)

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

  private def infixAddOutputTypes(lhs: ast.Expression, rhs: ast.Expression): TypeGenerator = s => {
    val lhsTypes = types(lhs)(s)
    val rhsTypes = types(rhs)(s)

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

  private def checkExtractExpressionDefined(x: ExtractExpression): SemanticCheck =
    when (x.scope.extractExpression.isEmpty) {
      SemanticError(s"${x.name}(...) requires '| expression' (an extract expression)", x.position)
    }

  private def checkInnerExtractExpression(x: ExtractExpression): SemanticCheck =
    x.scope.extractExpression.fold(SemanticCheckResult.success) {
      e => withScopedState {
        declareVariable(x.variable, FilteringExpressions.possibleInnerTypes(x)) chain
          check(SemanticContext.Simple, e)
      } chain {
        val outerTypes: TypeGenerator = types(e)(_).wrapInList
        specifyType(outerTypes, x)
      }
    }

  private def checkInnerListComprehension(x: ListComprehension): SemanticCheck =
    x.extractExpression match {
      case Some(e) =>
        withScopedState {
          declareVariable(x.variable, FilteringExpressions.possibleInnerTypes(x)) chain
            check(SemanticContext.Simple, e)
        } chain {
          val outerTypes: TypeGenerator = types(e)(_).wrapInList
          specifyType(outerTypes, x)
        }
      case None =>
        specifyType(types(x.expression), x)
    }
}

