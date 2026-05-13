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
import org.neo4j.cypher.internal.ast.ProjectionClause
import org.neo4j.cypher.internal.ast.ProjectionClause.Elements
import org.neo4j.cypher.internal.ast.ScopeClauseSubqueryCall
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.scoping.AggregatingPart
import org.neo4j.cypher.internal.ast.semantics.scoping.AggregatingSubclausePart
import org.neo4j.cypher.internal.ast.semantics.scoping.ExpressionScope
import org.neo4j.cypher.internal.ast.semantics.scoping.NonAggregatingPart
import org.neo4j.cypher.internal.ast.semantics.scoping.NonAggregatingSubclausePart
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionExpressionContext
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionPart
import org.neo4j.cypher.internal.ast.semantics.scoping.StatementScope
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingScope
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.util.Ref

/**
 * Runs all checks requiring the resolution of callables to be complete.
 *
 * Checked error codes:
 *   - 42I18,
 *   - 42N44,
 *   - 42N23
 */

case object AggregationChecker extends VariableCheckerUtil {

  // 42N23
  private def invalidUseOfAggregation(expr: Expression): Option[SemanticError] =
    Option.when(expr.containsAggregate)(
      SemanticError.aggregateExpressionsInOrderBy(Seq(ExpressionStringifier().apply(expr)), expr.position)
    )

  // 42N44
  private def inaccessibleVariable(clauseName: String): SimpleVariableCheck = {
    case ExpressionScope(lv: LogicalVariable, ctx, _, _, _)
      if !ctx.isConstantForPart(lv, NonAggregatingSubclausePart) =>
      Set(SemanticError.inaccessibleVariable(lv.name, clauseName, lv.position))
    case StatementScope(scs: ScopeClauseSubqueryCall, ctx, _, _, _, _, _, _)
      if !scs.importedVariables.forall(lv => ctx.isConstantForPart(lv, NonAggregatingSubclausePart)) =>
      scs.importedVariables.filterNot(lv => ctx.isConstantForPart(lv, NonAggregatingSubclausePart))
        .map(lv => SemanticError.inaccessibleVariable(lv.name, clauseName, lv.position)).toSet
  }

  // 42I18
  private def findAllInvalidReferences(
    scope: WorkingScope,
    part: ProjectionPart,
    inSubExpression: Boolean
  ): Set[LogicalVariable] = scope match {
    // A SubqueryExpression can only reference simple variable references - no recognition
    case ExpressionScope(_: FullSubqueryExpression, ctx: ProjectionExpressionContext, referenced, _, _) =>
      referenced.filterTargets(t => !ctx.isConstantForPart(t, part)).getVariables.toSet
    // If an expression is recognized we skip the children
    case ExpressionScope(expr: Expression, ctx: ProjectionExpressionContext, _, _, _)
      if ctx.recognizeExpression(expr, inSubExpression).isDefined => Set.empty
    // If a variable is not constant for the projection it is an invalid reference
    case ExpressionScope(lv: LogicalVariable, ctx, _, _, _) if !ctx.isConstantForPart(lv, part) => Set(lv)
    // Traverse the tree
    case ExpressionScope(_, _: ProjectionExpressionContext, _, _, children) =>
      children.foldLeft(Set.empty[LogicalVariable]) {
        case (acc, c) => acc ++ findAllInvalidReferences(c, part, inSubExpression = true)
      }
    case _ => Set.empty
  }

  private def checkScope(scope: WorkingScope, check: SimpleVariableCheck): Set[SemanticError] = {
    scope match {
      case ExpressionScope(expr: Expression, ProjectionExpressionContext(_, _, _, spec, _), _, _, _)
        if spec.isSubclauseRecognizable(expr) => Set.empty
      case _ =>
        check.applyOrElse(scope, (_: WorkingScope) => Set.empty) ++
          scope.children.flatMap(ws => checkScope(ws, check))
    }
  }

  private def traverseScope(clauseName: String, scope: WorkingScope): Set[SemanticError] = {

    val groups = scope.children.groupBy(_.incoming match {
      case ProjectionExpressionContext(_, _, _, _, part) => part
      case _                                             => NonAggregatingPart
    })

    val invalidReferencesInAggregationItems =
      groups.getOrElse(AggregatingPart, Set.empty).flatMap(s =>
        findAllInvalidReferences(s, AggregatingPart, inSubExpression = false).toSeq
      ).toSeq

    val ambiguousReferences =
      Option.when(invalidReferencesInAggregationItems.nonEmpty) {
        SemanticError.invalidReferenceToNonGroupingExpression(
          invalidReferencesInAggregationItems.sortBy(_.position).map(_.name).distinct,
          invalidReferencesInAggregationItems.head.position
        )
      }

    val subclausesScopes =
      groups.getOrElse(NonAggregatingSubclausePart, Seq.empty) ++
        groups.getOrElse(AggregatingSubclausePart, Seq.empty)
    val subclauseInaccessibleVariable = subclausesScopes.flatMap(s => checkScope(s, inaccessibleVariable(clauseName)))

    (ambiguousReferences ++ subclauseInaccessibleVariable).toSet

  }

  def legacyIllegalAggregationCheck(clause: ProjectionClause): Set[SemanticError] = {
    clause.orderBy.toSeq.flatMap(_.checkIllegalOrdering(clause.returnItems)).toSet
  }

  def checkAggregatingClause(from: BaseState, clause: ProjectionClause): Set[SemanticError] = {
    val scopeOpt = from.scopeState().recordedScopes.get(Ref(clause))
    scopeOpt.fold(Set.empty[SemanticError]) { s => traverseScope(clause.name, s) } ++
      legacyIllegalAggregationCheck(clause)
  }

  def checkNonAggregatingClause(clause: ProjectionClause): Set[SemanticError] =
    Elements(clause).subclauses.sortAndPredicateExpressions.flatMap(invalidUseOfAggregation).toSet ++
      legacyIllegalAggregationCheck(clause)

}
