/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.ast.semantics

import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.AllPropertiesSelector
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.AndedPropertyInequalities
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.BooleanLiteral
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.CaseExpression
import org.neo4j.cypher.internal.expressions.CoerceTo
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.Contains
import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.DecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.DesugaredMapProjection
import org.neo4j.cypher.internal.expressions.Divide
import org.neo4j.cypher.internal.expressions.EndsWith
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.ExistsSubClause
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Expression.SemanticContext
import org.neo4j.cypher.internal.expressions.ExtractExpression
import org.neo4j.cypher.internal.expressions.ExtractScope
import org.neo4j.cypher.internal.expressions.FilterExpression
import org.neo4j.cypher.internal.expressions.FilteringExpression
import org.neo4j.cypher.internal.expressions.FilterScope
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.GetDegree
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.HasLabelsOrTypes
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.HexIntegerLiteral
import org.neo4j.cypher.internal.expressions.ImplicitProcedureArgument
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.IntegerLiteral
import org.neo4j.cypher.internal.expressions.InvalidNotEquals
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.IsNull
import org.neo4j.cypher.internal.expressions.IterablePredicateExpression
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.ListComprehension
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.ListSlice
import org.neo4j.cypher.internal.expressions.LiteralEntry
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.MapProjection
import org.neo4j.cypher.internal.expressions.Modulo
import org.neo4j.cypher.internal.expressions.Multiply
import org.neo4j.cypher.internal.expressions.MultiRelationshipPathStep
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.NotEquals
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.OctalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.ParameterWithOldSyntax
import org.neo4j.cypher.internal.expressions.PartialPredicate
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.Pow
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertySelector
import org.neo4j.cypher.internal.expressions.ReduceExpression
import org.neo4j.cypher.internal.expressions.ReduceExpression.AccumulatorExpressionTypeMismatchMessageGenerator
import org.neo4j.cypher.internal.expressions.ReduceScope
import org.neo4j.cypher.internal.expressions.RegexMatch
import org.neo4j.cypher.internal.expressions.ShortestPathExpression
import org.neo4j.cypher.internal.expressions.SingleRelationshipPathStep
import org.neo4j.cypher.internal.expressions.StartsWith
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Subtract
import org.neo4j.cypher.internal.expressions.UnaryAdd
import org.neo4j.cypher.internal.expressions.UnarySubtract
import org.neo4j.cypher.internal.expressions.VarLengthBound
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.VariableSelector
import org.neo4j.cypher.internal.expressions.Xor
import org.neo4j.cypher.internal.expressions.functions.Length3_5
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTDate
import org.neo4j.cypher.internal.util.symbols.CTDateTime
import org.neo4j.cypher.internal.util.symbols.CTDuration
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTLocalDateTime
import org.neo4j.cypher.internal.util.symbols.CTLocalTime
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTPath
import org.neo4j.cypher.internal.util.symbols.CTPoint
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CTTime
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.StorableType.storableType
import org.neo4j.cypher.internal.util.symbols.TypeSpec

import scala.annotation.tailrec
import scala.util.Try

object SemanticExpressionCheck extends SemanticAnalysisTooling {

  val crashOnUnknownExpression: (SemanticContext, Expression) => SemanticCheck =
    (_, e) => throw new UnsupportedOperationException(s"Error in semantic analysis: Unknown expression $e")

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
  def check(ctx: SemanticContext, expression: Expression, parents: Seq[Expression] = Seq()): SemanticCheck =
    expression match {

        // ARITHMETICS

      case x:Add =>
        check(ctx, x.lhs, x +: parents) chain
          expectType(TypeSpec.all, x.lhs) chain
          check(ctx, x.rhs, x +: parents) chain
          expectType(infixAddRhsTypes(x.lhs), x.rhs) chain
          specifyType(infixAddOutputTypes(x.lhs, x.rhs), x) chain
          checkAddBoundary(x)

      case x:Subtract =>
        check(ctx, x.arguments, x +: parents) chain
          checkTypes(x, x.signatures) chain
          checkSubtractBoundary(x)

      case x:UnarySubtract =>
        check(ctx, x.arguments, x +: parents) chain
          checkTypes(x, x.signatures) chain
          checkUnarySubtractBoundary(x)

      case x:UnaryAdd =>
        check(ctx, x.arguments, x +: parents) chain
          checkTypes(x, x.signatures)

      case x:Multiply =>
        check(ctx, x.arguments, x +: parents) chain
          checkTypes(x, x.signatures) chain
          checkMultiplyBoundary(x)

      case x:Divide =>
        check(ctx, x.arguments, x +: parents) chain
          checkTypes(x, x.signatures)

      case x:Modulo =>
        check(ctx, x.arguments, x +: parents) chain
          checkTypes(x, x.signatures)

      case x:Pow =>
        check(ctx, x.arguments, x +: parents) chain
          checkTypes(x, x.signatures)

        // PREDICATES

      case x:Not =>
        check(ctx, x.arguments, x +: parents) chain
          checkTypes(x, x.signatures)

      case x:Equals =>
        check(ctx, x.arguments, x +: parents) chain checkTypes(x, x.signatures)

      case x:NotEquals =>
        check(ctx, x.arguments, x +: parents) chain checkTypes(x, x.signatures)

      case x:InvalidNotEquals =>
        SemanticError(
          "Unknown operation '!=' (you probably meant to use '<>', which is the operator for inequality testing)",
          x.position)

      case x:RegexMatch =>
        check(ctx, x.arguments, x +: parents) chain
          checkTypes(x, x.signatures)

      case x:And =>
        check(ctx, x.arguments, x +: parents) chain
          checkTypes(x, x.signatures)

      case x:Or =>
        check(ctx, x.arguments, x +: parents) chain
          checkTypes(x, x.signatures)

      case x:Xor =>
        check(ctx, x.arguments, x +: parents) chain
          checkTypes(x, x.signatures)

      case x:Ands =>
        check(ctx, x.exprs, x +: parents) chain
          checkTypes(x, x.signatures)

      case x:Ors =>
        check(ctx, x.exprs, x +: parents) chain
          checkTypes(x, x.signatures)

      case x:In =>
        check(ctx, x.lhs, x +: parents) chain
          expectType(CTAny.covariant, x.lhs) chain
          check(ctx, x.rhs, x +: parents) chain
          expectType(CTList(CTAny).covariant, x.rhs) chain
          specifyType(CTBoolean, x)

      case x:StartsWith =>
        check(ctx, x.arguments, x +: parents) chain
          checkTypes(x, x.signatures)

      case x:EndsWith =>
        check(ctx, x.arguments, x +: parents) chain
          checkTypes(x, x.signatures)

      case x:Contains =>
        check(ctx, x.arguments, x +: parents) chain
          checkTypes(x, x.signatures)

      case x:IsNull =>
        check(ctx, x.arguments, x +: parents) chain
          checkTypes(x, x.signatures)

      case x:IsNotNull =>
        check(ctx, x.arguments, x +: parents) chain
          checkTypes(x, x.signatures)

      case x:LessThan =>
        check(ctx, x.arguments, x +: parents) chain checkTypes(x, x.signatures)

      case x:LessThanOrEqual =>
        check(ctx, x.arguments, x +: parents) chain checkTypes(x, x.signatures)

      case x:GreaterThan =>
        check(ctx, x.arguments, x +: parents) chain checkTypes(x, x.signatures)

      case x:GreaterThanOrEqual =>
        check(ctx, x.arguments, x +: parents) chain checkTypes(x, x.signatures)

      case x: VarLengthBound =>
        check(ctx, x.arguments, x +: parents) chain
          expectType(CTList(CTRelationship).covariant, x.relName) chain
          specifyType(CTBoolean, x)

      case x:PartialPredicate[_] =>
        check(ctx, x.coveredPredicate, x +: parents)

      case x:CaseExpression =>
        val possibleTypes = unionOfTypes(x.possibleExpressions)
        SemanticExpressionCheck.check(ctx, x.expression, x +: parents) chain
          check(ctx, x.alternatives.flatMap { a => Seq(a._1, a._2) }, x +: parents) chain
          check(ctx, x.default, x +: parents) chain
          when (x.expression.isEmpty) {
            expectType(CTBoolean.covariant, x.alternatives.map(_._1))
          } chain
          specifyType(possibleTypes, x)

      case x:AndedPropertyInequalities =>
        x.inequalities.map(check(ctx, _, x +: parents)).reduceLeft(_ chain _)

      case x:CoerceTo =>
        check(ctx, x.expr, x +: parents) chain expectType(x.typ.covariant, x.expr)

      case x: Property =>
        val allowedTypes = CTNode.covariant | CTRelationship.covariant | CTMap.covariant | CTPoint.covariant | CTDate.covariant | CTTime.covariant |
          CTLocalTime.covariant | CTLocalDateTime.covariant | CTDateTime.covariant | CTDuration.covariant

        check(ctx, x.map, x +: parents) chain
          expectType(allowedTypes, x.map) chain
          typeSwitch(x.map) {
            // Maybe we can do even more here - Point / Dates probably have type implications too
            case CTNode.invariant | CTRelationship.invariant => specifyType(storableType, x)
            case _ => specifyType(CTAny.covariant, x)
          }

      case x:CachedProperty =>
        specifyType(CTAny.covariant, x)

      // Check the variable is defined and, if not, define it so that later errors are suppressed
      // This is used in expressions; in graphs we must make sure to sem check variables explicitly (!)
      case x:Variable =>
        s => s.ensureVariableDefined(x) match {
          case Right(ss) => SemanticCheckResult.success(ss)
          case Left(error) =>
            if (s.declareVariablesToSuppressDuplicateErrors) {
              // Most of the time we want to suppress if this error occurs again, by declaring the missing variable now
              s.declareVariable(x, CTAny.covariant) match {
                // if the variable is a graph, declaring it will fail
                case Right(ss) => SemanticCheckResult.error(ss, error)
                case Left(_error) => SemanticCheckResult.error(s, _error)
              }
            } else {
              // If we are ignoring errors anyway, the fake declaration might mess up the scope
              SemanticCheckResult.error(s, error)
            }
        }

      case x:FunctionInvocation =>
        SemanticFunctionCheck.check(ctx, x, x +: parents)

      case x:GetDegree =>
        check(ctx, x.node, x +: parents) chain
          expectType(CTMap.covariant | CTAny.invariant, x.node) chain
          specifyType(CTAny.covariant, x)

      case x:ParameterWithOldSyntax =>
        SemanticError("The old parameter syntax `{param}` is no longer supported. Please use `$param` instead", x.position)

      case x:Parameter =>
        specifyType(x.parameterType.covariant, x)

      case x:ImplicitProcedureArgument =>
        specifyType(x.parameterType.covariant, x)

      case x:HasLabelsOrTypes =>
        check(ctx, x.expression, x +: parents) chain
          expectType(CTNode.covariant | CTRelationship.covariant, x.expression) chain
          specifyType(CTBoolean, x)

      case x:HasLabels =>
        check(ctx, x.expression, x +: parents) chain
          expectType(CTNode.covariant, x.expression) chain
          specifyType(CTBoolean, x)

      case x:HasTypes =>
        check(ctx, x.expression, x +: parents) chain
          expectType(CTRelationship.covariant, x.expression) chain
          specifyType(CTBoolean, x)

        // ITERABLES

      case x:FilterExpression =>
        SemanticError("Filter is no longer supported. Please use list comprehension instead", x.position)

      case x:ExtractExpression =>
        SemanticError("Extract is no longer supported. Please use list comprehension instead", x.position)

      case x:ListComprehension =>
        FilteringExpressions.semanticCheck(ctx, x, parents) chain
          checkInnerListComprehension(x, parents) chain
          FilteringExpressions.failIfAggregating(x.extractExpression)

      case x:PatternComprehension =>
        SemanticState.recordCurrentScope(x) chain
          withScopedState {
            SemanticPatternCheck.check(Pattern.SemanticContext.Match, x.pattern) chain
              x.namedPath.foldSemanticCheck(declareVariable(_, CTPath)) chain
              x.predicate.foldSemanticCheck(Where.checkExpression) chain
              simple(x.projection)
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
        specifyType(CTPath, x) chain
          check(ctx, x.step)

      case x: NodePathStep =>
        check(ctx, x.node) chain
          check(ctx, x.next)

      case x: SingleRelationshipPathStep =>
        check(ctx, x.rel) chain
          x.toNode.map(check(ctx, _)).getOrElse(SemanticCheckResult.success) chain
          check(ctx, x.next)

      case x: MultiRelationshipPathStep =>
        check(ctx, x.rel) chain
          x.toNode.map(check(ctx, _)).getOrElse(SemanticCheckResult.success) chain
          check(ctx, x.next)

      case _: NilPathStep =>
        SemanticCheckResult.success

      case x:ShortestPathExpression =>
        SemanticPatternCheck.checkElementPredicates(Pattern.SemanticContext.Expression)(x.pattern) chain
          SemanticPatternCheck.declareVariables(Pattern.SemanticContext.Expression)(x.pattern) chain
          SemanticPatternCheck.check(Pattern.SemanticContext.Expression)(x.pattern) chain
          specifyType(if (x.pattern.single) CTPath else CTList(CTPath), x)

      case x:PatternExpression =>
        SemanticState.recordCurrentScope(x) chain
          withScopedState {
            SemanticPatternCheck.check(Pattern.SemanticContext.Match, x.pattern) chain {
              (state: SemanticState) => {
                val errors = x.pattern.element.allVariables.toSeq.collect {
                  case v if state.recordedScopes(x).symbol(v.name).isEmpty && !SemanticPatternCheck.variableIsGenerated(v) =>
                    SemanticError(s"PatternExpressions are not allowed to introduce new variables: '${v.name}'.", v.position)
                }
                SemanticCheckResult(state, errors)
              }
            }
          } chain
          specifyType(CTList(CTPath), x) chain
          SemanticPatternCheck.checkElementPredicates(Pattern.SemanticContext.Expression, x.pattern.element)

      case x:IterablePredicateExpression =>
        FilteringExpressions.checkPredicateDefined(x) chain
          FilteringExpressions.semanticCheck(ctx, x, parents) chain
          specifyType(CTBoolean, x)

      case x:ReduceExpression =>
        check(ctx, x.init, x +: parents) chain
          check(ctx, x.list, x +: parents) chain
          expectType(CTList(CTAny).covariant, x.list) chain
          withScopedState {
            val indexType: TypeGenerator = s =>
              (types(x.list)(s) constrain CTList(CTAny)).unwrapLists
            val accType: TypeGenerator = types(x.init)

            declareVariable(x.variable, indexType) chain
              declareVariable(x.accumulator, accType) chain
              check(SemanticContext.Simple, x.expression, x +: parents)
          } chain
          expectType(types(x.init), x.expression, AccumulatorExpressionTypeMismatchMessageGenerator) chain
          specifyType(s => types(x.init)(s) leastUpperBounds types(x.expression)(s), x) chain
          FilteringExpressions.failIfAggregating(x.expression)

      case x:ListLiteral =>
        def possibleTypes: TypeGenerator = state => x.expressions match {
          case Seq() => CTList(CTAny).covariant
          case _     => leastUpperBoundsOfTypes(x.expressions)(state).wrapInCovariantList
        }
        check(ctx, x.expressions, x +: parents) chain specifyType(possibleTypes, x)

      case x:ListSlice =>
        check(ctx, x.list, x +: parents) chain
          expectType(CTList(CTAny).covariant, x.list) chain
          when(x.from.isEmpty && x.to.isEmpty) {
            SemanticError("The start or end (or both) is required for a collection slice", x.position)
          } chain
          check(ctx, x.from, x +: parents) chain
          expectType(CTInteger.covariant, x.from) chain
          check(ctx, x.to, x +: parents) chain
          expectType(CTInteger.covariant, x.to) chain
          specifyType(types(x.list), x)

      case x:ContainerIndex =>
        check(ctx, x.expr, x +: parents) chain
          check(ctx, x.idx, x +: parents) chain
          typeSwitch(x.expr) {
            exprT =>
              typeSwitch(x.idx) {
                idxT =>
                  val listT = CTList(CTAny).covariant & exprT
                  val mapT = CTMap.covariant & exprT
                  val exprIsList = listT != TypeSpec.none
                  val exprIsMap = mapT != TypeSpec.none
                  val idxIsInteger = (CTInteger.covariant & idxT) != TypeSpec.none
                  val idxIsString = (CTString.covariant & idxT) != TypeSpec.none
                  val listLookup = exprIsList || idxIsInteger
                  val mapLookup = exprIsMap || idxIsString

                  if (listLookup && !mapLookup) {
                    expectType(CTList(CTAny).covariant, x.expr) ifOkChain
                      specifyType(types(x.expr)(_).unwrapLists, x) chain
                      expectType(CTInteger.covariant, x.idx)
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
        check(ctx, x.items.map(_._2), x +: parents) chain
          specifyType(CTMap, x)

      case x:MapProjection =>
        check(ctx, x.items, x +: parents) chain
          ensureDefined(x.name) chain
          expectType(CTMap.covariant, x.name) chain
          specifyType(CTMap, x) ifOkChain // We need to remember the scope to later rewrite this ASTNode
          SemanticState.recordCurrentScope(x)

      case x:LiteralEntry =>
        check(ctx, x.exp, x +: parents)

      case x:VariableSelector =>
        check(ctx, x.id, x +: parents)

      case _:PropertySelector =>
        SemanticCheckResult.success

      case _:AllPropertiesSelector =>
        SemanticCheckResult.success

      case x:DesugaredMapProjection =>
        check(ctx, x.items, x +: parents) chain
          ensureDefined(x.variable) chain
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
          if (x.stringVal matches "^-?0o?[0-7]+$")
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

      // EXISTS
      case x:ExistsSubClause =>
        @tailrec
        def existsIsValidHere(p: Seq[Expression]): SemanticCheck = p match {
          case Nil => None
          case (And(_, _) | Or(_, _) | Ands(_) | Ors(_) | Xor(_, _) | Not(_) | ExistsSubClause(_,_)) :: ps  => existsIsValidHere(ps)
          case _ => SemanticError("EXISTS is only valid in a WHERE clause as a standalone predicate or as part of a boolean expression (AND / OR / XOR / NOT)", x.position)
        }
        existsIsValidHere(parents) chain
          SemanticState.recordCurrentScope(x) chain
            withScopedState { // saves us from leaking to the outside
              SemanticPatternCheck.check(Pattern.SemanticContext.Match, x.pattern) chain
                when(x.optionalWhereExpression.isDefined) {
                  val whereExpression = x.optionalWhereExpression.get
                  check(ctx, whereExpression, x +: parents) chain
                    expectType(CTBoolean.covariant, whereExpression)
                }
            }

      case x: Length3_5 =>
        check(ctx, x.argument, x +: parents) chain
          checkTypes(x, x.signatures)

      case x:Expression => semanticCheckFallback(ctx, x)
    }

  /**
   * Build a semantic check over a traversable of expressions.
   */
  def simple(traversable: Traversable[Expression]): SemanticCheck = check(SemanticContext.Simple, traversable, Seq())

  def check(
             ctx: SemanticContext,
             traversable: Traversable[Expression],
             parents: Seq[Expression]
           ): SemanticCheck =
    semanticCheckFold(traversable)(expr => check(ctx, expr, parents))

  /**
   * Build a semantic check over an optional expression.
   */
  def simple(option: Option[Expression]): SemanticCheck = check(SemanticContext.Simple, option, Seq())

  def check(ctx: SemanticContext, option: Option[Expression], parents: Seq[Expression]): SemanticCheck =
    option.foldSemanticCheck {
      check(ctx, _, parents)
    }

  object FilteringExpressions {

    def semanticCheck(ctx: SemanticContext, e: FilteringExpression, parents: Seq[Expression]):SemanticCheck =
      SemanticExpressionCheck.check(ctx, e.expression, e +: parents) chain
        expectType(CTList(CTAny).covariant, e.expression) chain
        checkInnerPredicate(e, parents) chain
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

    private def checkInnerPredicate(e: FilteringExpression, parents: Seq[Expression]): SemanticCheck =
      e.innerPredicate match {
      case Some(predicate) => withScopedState {
        declareVariable(e.variable, possibleInnerTypes(e)) chain
          SemanticExpressionCheck.check(SemanticContext.Simple, predicate, e +: parents) chain
          SemanticExpressionCheck.expectType(CTBoolean.covariant, predicate)
      }
      case None    => SemanticCheckResult.success
    }

    def possibleInnerTypes(e: FilteringExpression): TypeGenerator = s =>
      (types(e.expression)(s) constrain CTList(CTAny)).unwrapLists
  }

  private def checkAddBoundary(add: Add): SemanticCheck =
    (add.lhs, add.rhs) match {
      case (l:IntegerLiteral, r:IntegerLiteral) if Try(Math.addExact(l.value, r.value)).isFailure =>
        SemanticError(s"result of ${l.stringVal} + ${r.stringVal} cannot be represented as an integer", add.position)
      case _ => SemanticCheckResult.success
    }

  private def checkSubtractBoundary(subtract: Subtract): SemanticCheck =
    (subtract.lhs, subtract.rhs) match {
      case (l:IntegerLiteral, r:IntegerLiteral) if Try(Math.subtractExact(l.value, r.value)).isFailure =>
        SemanticError(s"result of ${l.stringVal} - ${r.stringVal} cannot be represented as an integer", subtract.position)
      case _ => SemanticCheckResult.success
    }

  private def checkUnarySubtractBoundary(subtract: UnarySubtract): SemanticCheck =
    subtract.rhs match {
      case r:IntegerLiteral if Try(Math.subtractExact(0, r.value)).isFailure =>
        SemanticError(s"result of -${r.stringVal} cannot be represented as an integer", subtract.position)
      case _ => SemanticCheckResult.success
    }

  private def checkMultiplyBoundary(multiply: Multiply): SemanticCheck =
    (multiply.lhs, multiply.rhs) match {
      case (l:IntegerLiteral, r:IntegerLiteral) if Try(Math.multiplyExact(l.value, r.value)).isFailure =>
        SemanticError(s"result of ${l.stringVal} * ${r.stringVal} cannot be represented as an integer", multiply.position)
      case _ => SemanticCheckResult.success
    }

  private def infixAddRhsTypes(lhs: Expression): TypeGenerator = s => {
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
    // Temporals
    // T + Duration => T
    // Duration + T => T
    // Duration + Duration => Duration
    val valueTypes =
      if (lhsTypes containsAny (CTInteger.covariant | CTFloat.covariant | CTString.covariant))
        CTString.covariant | CTInteger.covariant | CTFloat.covariant
      else
        TypeSpec.none

    val temporalTypes =
      if (lhsTypes containsAny (CTDate.covariant | CTTime.covariant | CTLocalTime.covariant |
        CTDateTime.covariant | CTLocalDateTime.covariant | CTDuration.covariant))
        CTDuration.covariant
      else
        TypeSpec.none

    val durationTypes =
      if (lhsTypes containsAny CTDuration.covariant)
        CTDate.covariant | CTTime.covariant | CTLocalTime.covariant |
          CTDateTime.covariant | CTLocalDateTime.covariant | CTDuration.covariant
      else
        TypeSpec.none

    // [a] + [b] => [a, b]
    val listTypes = (lhsTypes leastUpperBounds CTList(CTAny) constrain CTList(CTAny)).covariant

    // [a] + b => [a, b]
    val lhsListTypes = listTypes | listTypes.unwrapLists

    // a + [b] => [a, b]
    val rhsListTypes = CTList(CTAny).covariant

    valueTypes | lhsListTypes | rhsListTypes | temporalTypes | durationTypes
  }

  private def infixAddOutputTypes(lhs: Expression, rhs: Expression): TypeGenerator = s => {
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

    val temporalTypes: TypeSpec =
      when(CTDuration.covariant, CTDuration.covariant)(CTDuration) |
      when(CTDate.covariant, CTDuration.covariant)(CTDate) |
      when(CTDuration.covariant, CTDate.covariant)(CTDate) |
      when(CTTime.covariant, CTDuration.covariant)(CTTime) |
      when(CTDuration.covariant, CTTime.covariant)(CTTime) |
      when(CTLocalTime.covariant, CTDuration.covariant)(CTLocalTime) |
      when(CTDuration.covariant, CTLocalTime.covariant)(CTLocalTime) |
      when(CTLocalDateTime.covariant, CTDuration.covariant)(CTLocalDateTime) |
      when(CTDuration.covariant, CTLocalDateTime.covariant)(CTLocalDateTime) |
      when(CTDateTime.covariant, CTDuration.covariant)(CTDateTime) |
      when(CTDuration.covariant, CTDateTime.covariant)(CTDateTime)

    val listTypes = {
      val lhsListTypes = lhsTypes constrain CTList(CTAny)
      val rhsListTypes = rhsTypes constrain CTList(CTAny)
      val lhsListInnerTypes = lhsListTypes.unwrapLists
      val rhsListInnerTypes = rhsListTypes.unwrapLists
      val lhsScalarTypes = lhsTypes without CTList(CTAny)
      val rhsScalarTypes = rhsTypes without CTList(CTAny)

      val bothListMergedLhTypes = (lhsListInnerTypes coerceOrLeastUpperBound rhsListInnerTypes).wrapInList
      val bothListMergedRhTypes = (rhsListInnerTypes coerceOrLeastUpperBound lhsListInnerTypes).wrapInList

      val lhListMergedTypes = (rhsScalarTypes coerceOrLeastUpperBound lhsListInnerTypes).wrapInList
      val rhListMergedTypes = (lhsScalarTypes coerceOrLeastUpperBound rhsListInnerTypes).wrapInList

      bothListMergedLhTypes | bothListMergedRhTypes | lhListMergedTypes | rhListMergedTypes
    }

    stringTypes | numberTypes | listTypes | temporalTypes
  }

  private def checkInnerListComprehension(x: ListComprehension, parents: Seq[Expression]): SemanticCheck =
    x.extractExpression match {
      case Some(e) =>
        withScopedState {
          declareVariable(x.variable, FilteringExpressions.possibleInnerTypes(x)) chain
            check(SemanticContext.Simple, e, x +: parents)
        } chain {
          val outerTypes: TypeGenerator = types(e)(_).wrapInList
          specifyType(outerTypes, x)
        }
      case None => withScopedState {
        // Even if there is no usage of that variable, we need to declare it, to not confuse the Namespacer
        declareVariable(x.variable, FilteringExpressions.possibleInnerTypes(x))
      } chain {
        specifyType(types(x.expression), x)
      }
    }
}
