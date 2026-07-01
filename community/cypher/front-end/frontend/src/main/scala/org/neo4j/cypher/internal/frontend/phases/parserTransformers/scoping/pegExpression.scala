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
package org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping

import org.neo4j.cypher.internal.ast.FullSubqueryExpression
import org.neo4j.cypher.internal.ast.semantics.scoping.Declarations
import org.neo4j.cypher.internal.ast.semantics.scoping.ExpressionScope
import org.neo4j.cypher.internal.ast.semantics.scoping.References
import org.neo4j.cypher.internal.ast.semantics.scoping.RegularContext
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingScope
import org.neo4j.cypher.internal.expressions.AllReducePredicate
import org.neo4j.cypher.internal.expressions.AllReducePredicate.AllReduceScope
import org.neo4j.cypher.internal.expressions.AllReducePredicate.ReductionStepVariableScope
import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.ExtractScope
import org.neo4j.cypher.internal.expressions.FilterScope
import org.neo4j.cypher.internal.expressions.FunctionInvocationLike
import org.neo4j.cypher.internal.expressions.IterablePredicateExpression
import org.neo4j.cypher.internal.expressions.ListComprehension
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.ReduceExpression
import org.neo4j.cypher.internal.expressions.ReduceScope
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.label_expressions.LabelExpression.DynamicLeaf
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.Foldable.FoldingBehavior
import org.neo4j.cypher.internal.util.Foldable.SkipChildren

object pegExpression {

  def apply(labelExpression: LabelExpression, incoming: RegularContext)(implicit c: PegContext): WorkingScope = {
    c.getRecordScopeOrElse[LabelExpression](
      labelExpression,
      incoming,
      inImportingWith = false,
      foreachIterVar = None,
      applyUncached(_, _)
    )
  }

  private def applyUncached(labelExpression: LabelExpression, incoming: RegularContext)(implicit
    c: PegContext): WorkingScope = {
    def collect(scope: WorkingScope): Seq[WorkingScope] => FoldingBehavior[Seq[WorkingScope]] =
      acc => SkipChildren(acc :+ scope)

    val children = labelExpression.folder.treeFold(Seq[WorkingScope]()) {
      case DynamicLeaf(leafExpression, _) => collect(apply(leafExpression.expression, incoming))
    }
    onlyChildIfSelfOrElse(
      children,
      labelExpression,
      () => incoming.expressionResultScope(labelExpression, children)
    )
  }

  def apply(expression: Expression, incoming: RegularContext)(implicit c: PegContext): WorkingScope = {
    c.getRecordScopeOrElse[Expression](
      expression,
      incoming,
      inImportingWith = false,
      foreachIterVar = None,
      applyUncached(_, _)
    )
  }

  private def applyUncached(expression: Expression, incoming: RegularContext)(implicit c: PegContext): WorkingScope = {
    val children = scopeExpression(expression, incoming)
    onlyChildIfSelfOrElse(children, expression, () => incoming.expressionResultScope(expression, children))
  }

  private def scopeExpression(
    expression: Expression,
    incoming: RegularContext
  )(implicit c: PegContext): Seq[ExpressionScope] = {
    def collect(scope: ExpressionScope)
      : Seq[ExpressionScope] => FoldingBehavior[Seq[ExpressionScope]] =
      acc => SkipChildren(acc :+ scope)

    expression.folder.treeFold(Seq[ExpressionScope]()) {

      /**
       *  Recognize expression in projection context
       */
      case expr: Expression if incoming.recognizeExpression(expr, isSubExpression = true).isDefined =>
        val recognizedItem = incoming.recognizeExpression(expr, isSubExpression = true).get
        collect(incoming.recognizedLeafScope(expr, recognizedItem))

      /**
       * Variable
       */
      case variable: Variable =>
        val children = WorkingScope.noChildren
        val referenced: Option[References] = Some(References.connect(variable, incoming.allSymbolsAndKeys))
        collect(incoming.expressionResultScope(variable, children, referenced))

      /**
       * Regular function
       */
      case fi: FunctionInvocationLike if !fi.isAggregate =>
        val children = fi.callArguments.map(arg => apply(arg, incoming))
        collect(incoming.expressionResultScope(fi, children))

      /**
       * Aggregation function
       */
      case cntStar: CountStar =>
        collect(incoming.expressionResultScope(cntStar, Seq.empty))
      case fi: FunctionInvocationLike if fi.isAggregate =>
        val argIncoming = incoming.aggregatingConstantChildContext
        val children = fi.callArguments.map(arg => apply(arg, argIncoming))
        collect(incoming.expressionResultScope(fi, children))

      /**
       * Scalar subqueries
       */
      case fse: FullSubqueryExpression =>
        val child = ScopeSurveyor.scope(fse.query, incoming.constantChildContext(), c)
        val children = Seq(child)
        collect(incoming.expressionResultScope(fse, children))

      /**
       * Scope expressions
       */
      case lc @ ListComprehension(es @ ExtractScope(variable, innerPredicate, extractExpression), expression) =>
        val innerIncoming = incoming.amendedWithShadowingConstant(Set(variable))
        val innerResult = Seq(innerPredicate, extractExpression).flatten.map(apply(_, innerIncoming))
        val extractReferenced = WorkingScope.referencedInChildren(innerResult) diff variable
        val extractScope =
          innerIncoming.expressionResultScope(
            es,
            innerResult,
            Some(extractReferenced),
            Declarations(Seq(variable), Seq.empty)
          )
        val expressionResult = apply(expression, incoming)
        collect(incoming.expressionResultScope(lc, Seq(expressionResult, extractScope)))

      case pe @ PatternExpression(pattern) =>
        val patternResult =
          pegPattern(pattern.element, incoming.constantChildContext(), foreachIterVar = None)
        collect(incoming.expressionResultScope(
          pe,
          Seq(patternResult),
          Some(patternResult.referenced),
          patternResult.declared.withoutAnonymousDeclaration
        ))

      case pc @ PatternComprehension(optVar, pattern, innerPredicate, projection) =>
        val patternResult = pegPattern(pattern.element, incoming, foreachIterVar = None)
        val variables = optVar match {
          case Some(value) => Seq(value) ++ patternResult.declared.variables
          case None        => patternResult.declared.variables
        }
        val innerIncoming = incoming.amendedWithShadowingConstant(variables.toSet)
        val innerResult = Seq(innerPredicate, Some(projection)).flatMap {
          case Some(ex) => Some(apply(ex, innerIncoming))
          case None     => None
        }
        val children = patternResult +: innerResult
        val referenced = {
          val innerReferenced = WorkingScope.referencedInChildren(innerResult) diff variables.toSet
          val patternReferenced = patternResult.referenced
          Some(innerReferenced union patternReferenced)
        }
        val declared = Declarations(variables, Seq.empty)
        collect(incoming.expressionResultScope(pc, children, referenced, declared))

      case iter: IterablePredicateExpression =>
        val FilterScope(variable, innerPredicate) = iter.scope
        val innerIncoming = incoming.amendedWithShadowingConstant(Set(variable))
        val innerResult = innerPredicate.fold(Seq.empty[WorkingScope]) { ex => Seq(apply(ex, innerIncoming)) }
        val filterReferenced = WorkingScope.referencedInChildren(innerResult) diff variable
        val filterScope =
          innerIncoming.expressionResultScope(
            iter.scope,
            innerResult,
            Some(filterReferenced),
            Declarations(Seq(variable), Seq.empty)
          )
        val expressionResult = apply(iter.expression, incoming)
        collect(incoming.expressionResultScope(iter, Seq(filterScope, expressionResult)))

      case r @ ReduceExpression(rs @ ReduceScope(accumulator, variable, expression), init, list) =>
        val innerIncoming = incoming.amendedWithShadowingConstant(Set(accumulator, variable))
        val innerResult = apply(expression, innerIncoming)
        val reduceReferenced = innerResult.referenced diff accumulator diff variable
        val reduceScope =
          innerIncoming.expressionResultScope(
            rs,
            Seq(innerResult),
            Some(reduceReferenced),
            Declarations(Seq(accumulator, variable), Seq.empty)
          )
        val initResult = apply(init, incoming)
        val listResult = apply(list, incoming)
        collect(incoming.expressionResultScope(r, Seq(initResult, listResult, reduceScope)))

      case r @ AllReducePredicate(
          ars @ AllReduceScope(
            accumulator,
            rsvs @ ReductionStepVariableScope(reductionStepVariable, reductionStep, predicate)
          ),
          init,
          list
        ) =>
        val bothShadowed = incoming.amendedWithShadowingConstant(Set(accumulator, reductionStepVariable))
        val reductionStepResult = apply(reductionStep, bothShadowed)
        val predicateResult = apply(predicate, bothShadowed)
        val rsvReferenced =
          WorkingScope.referencedInChildren(Seq(reductionStepResult, predicateResult)) diff reductionStepVariable
        val reductionStepScope =
          bothShadowed.expressionResultScope(
            rsvs,
            Seq(reductionStepResult, predicateResult),
            Some(rsvReferenced),
            Declarations(Seq(reductionStepVariable), Seq.empty)
          )
        val accumulatorScope =
          incoming.amendedWithShadowingConstant(Set(accumulator)).expressionResultScope(
            ars,
            Seq(reductionStepScope),
            Some(rsvReferenced diff accumulator),
            Declarations(Seq(accumulator), Seq.empty)
          )
        val initResult = apply(init, incoming)
        val listResult = apply(list, incoming)
        collect(incoming.expressionResultScope(r, Seq(initResult, listResult, accumulatorScope)))
    }
  }

  @inline private def onlyChildIsSelf(children: Seq[WorkingScope], self: ASTNode): Boolean =
    children.size == 1 && children.head.astNode == self

  @inline private def onlyChildIfSelfOrElse(
    children: Seq[WorkingScope],
    self: ASTNode,
    orElse: () => WorkingScope
  ): WorkingScope = {
    if (onlyChildIsSelf(children, self)) {
      children.head
    } else {
      orElse()
    }
  }
}
