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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.ExplicitGroupingElements
import org.neo4j.cypher.internal.ast.FullSubqueryExpression
import org.neo4j.cypher.internal.ast.GroupBy
import org.neo4j.cypher.internal.ast.ProjectionClause
import org.neo4j.cypher.internal.ast.ProjectionClause.Elements
import org.neo4j.cypher.internal.ast.ScopeClauseSubqueryCall
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.scoping.AggregatingPart
import org.neo4j.cypher.internal.ast.semantics.scoping.AggregatingSubclausePart
import org.neo4j.cypher.internal.ast.semantics.scoping.ExpressionScope
import org.neo4j.cypher.internal.ast.semantics.scoping.GroupByPart
import org.neo4j.cypher.internal.ast.semantics.scoping.NonAggregatingPart
import org.neo4j.cypher.internal.ast.semantics.scoping.NonAggregatingSubclausePart
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionExpressionContext
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionPart
import org.neo4j.cypher.internal.ast.semantics.scoping.StatementScope
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingScope
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.Ref

/**
 * Runs all checks requiring the resolution of callables to be complete.
 *
 * Checked error codes:
 *   - 42I18,
 *   - 42I80,
 *   - 42N44,
 *   - 42N23
 */

case object AggregationChecker extends VariableCheckerUtil {

  private val expressionStringifier = ExpressionStringifier.apply()

  // 42N23
  private def invalidUseOfAggregation(expr: Expression): Option[SemanticError] =
    Option.when(expr.containsAggregate)(
      SemanticError.aggregateExpressionsInOrderBy(Seq(expressionStringifier(expr)), expr.position)
    )

  // 42N44
  private def inaccessibleVariable(
    clauseName: String,
    from: BaseState,
    groupBySupported: Boolean
  ): SimpleVariableCheck = {
    case ExpressionScope(lv: LogicalVariable, ctx, _, _, _)
      if !ctx.isConstantForPart(lv, NonAggregatingSubclausePart) =>
      Set(SemanticError.inaccessibleVariable(lv.name, clauseName, lv.position, groupBySupported))
    case StatementScope(scs: ScopeClauseSubqueryCall, ctx, _, _, _, _, _, _)
      if !scs.importedVariables.forall(lv => ctx.isConstantForPart(lv, NonAggregatingSubclausePart)) =>
      scs.importedVariables.filterNot(lv => ctx.isConstantForPart(lv, NonAggregatingSubclausePart))
        .map(lv => SemanticError.inaccessibleVariable(lv.name, clauseName, lv.position, groupBySupported)).toSet
    // Subquery expression matched as a sub-expression of a larger sort/WHERE:
    // recognition wraps it in a `recognizedLeafScope` with empty children, so the
    // inner Query's `a`-references aren't reachable via the leaf's own scope.
    // Look up the matching projection item via the spec and read its recorded
    // scope's references (the item's own scope, built without recognition, has
    // the inner refs).
    case ExpressionScope(fse: FullSubqueryExpression, ctx: ProjectionExpressionContext, _, _, _) =>
      ctx.projectionSpecification.allItems.find(_.expression == fse)
        .map(item =>
          from.scopeState().getReferenced(item.expression)
            .filterNot(lv => ctx.isConstantForPart(lv, NonAggregatingSubclausePart))
            .map(lv => SemanticError.inaccessibleVariable(lv.name, clauseName, lv.position, groupBySupported))
        )
        .getOrElse(Set.empty)
    // Recognized-leaf scope (non-variable expression matched via sub-expression).
    // Per CIP-248 Rule 2 the variables the user actually wrote live in
    // `hiddenReferences`; the public `referenced` only carries the resolved
    // alias. Flag any caller whose target isn't constant in this subclause part.
    case scope @ ExpressionScope(_, ctx, _, _, _) if scope.hiddenReferences.getVariables.nonEmpty =>
      scope.hiddenReferences.getVariables
        .filterNot(lv => ctx.isConstantForPart(lv, NonAggregatingSubclausePart))
        .map(lv => SemanticError.inaccessibleVariable(lv.name, clauseName, lv.position, groupBySupported))
        .toSet
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

  private case class InvalidGroupingElement(error: SemanticError, exemptItemExprs: Set[Ref[ASTNode]])

  // 42I80
  private def invalidGroupingElement(scope: WorkingScope): Option[InvalidGroupingElement] = scope match {
    case ExpressionScope(e: Expression, ctx: ProjectionExpressionContext, referenced, _, _) =>
      val spec = ctx.projectionSpecification
      val aggregatingVar =
        referenced.filterTargets(spec.aggregatingItems.flatMap(_.alias)).getVariables.minByOption(_.name)
      val groupingVars =
        referenced.filterTargets(spec.nonAggregatingItems.flatMap(_.alias)).getVariables
      val exemptItemExprs: Set[Ref[ASTNode]] =
        if (groupingVars.isEmpty) Set.empty
        else {
          val referencedNames = groupingVars.iterator.map(_.name).toSet
          spec.nonAggregatingItems
            .filter(_.alias.exists(a => referencedNames(a.name)))
            .map(item => Ref[ASTNode](item.expression))
        }
      def element = expressionStringifier(e)
      def result(referencedName: String, referencesAggregation: Boolean) =
        InvalidGroupingElement(
          SemanticError.invalidGroupingElement(element, referencedName, referencesAggregation, e.position),
          exemptItemExprs
        )
      aggregatingVar
        .map(v => result(v.name, referencesAggregation = true))
        .orElse(
          Option.when(!e.isInstanceOf[LogicalVariable])(groupingVars.minByOption(_.name)).flatten
            .map(v => result(v.name, referencesAggregation = false))
        )
    case _ => None
  }

  private def isExplicitGroupBy(scope: WorkingScope): Boolean = scope.astNode match {
    case GroupBy(_: ExplicitGroupingElements) => true
    case _                                    => false
  }

  private def checkScope(
    scope: WorkingScope,
    check: SimpleVariableCheck,
    isTopLevel: Boolean = true
  ): Set[SemanticError] = {
    scope match {
      // Top-level subclause expression that the user wrote identically to a
      // projection item (alias / exact full match / recognizable form) is
      // allowed by CIP-248 Rule 2 — no traversal, no check. Sub-level recognized
      // leaves below the top are NOT skipped: they represent sub-expression
      // matches and their inner variables may still be inaccessible.
      case ExpressionScope(expr: Expression, ProjectionExpressionContext(_, _, _, spec, _), _, _, _)
        if isTopLevel && (spec.isSubclauseRecognizable(expr) || spec.allItems.exists(_.expression == expr)) =>
        Set.empty
      case _ =>
        check.applyOrElse(scope, (_: WorkingScope) => Set.empty) ++
          scope.children.flatMap(ws => checkScope(ws, check, isTopLevel = false))
    }
  }

  private def traverseScope(
    from: BaseState,
    clauseName: String,
    scope: WorkingScope,
    groupBySupported: Boolean
  ): Set[SemanticError] = {

    val groups = scope.children.groupBy(_.incoming match {
      case ProjectionExpressionContext(_, _, _, _, part) => part
      case _                                             => NonAggregatingPart
    })

    def scopesFor(part: ProjectionPart): Seq[WorkingScope] = groups.getOrElse(part, Seq.empty)
    def invalidRefsIn(scopes: Seq[WorkingScope], part: ProjectionPart): Seq[LogicalVariable] =
      scopes.flatMap(s => findAllInvalidReferences(s, part, inSubExpression = false))

    // 42I80: only explicit grouping elements can be invalid grouping elements.
    val invalidGroupingElementResults =
      scopesFor(GroupByPart).filter(isExplicitGroupBy).flatMap(_.children).flatMap(invalidGroupingElement)

    // Items referenced by an invalid grouping element are the user's grouping keys, suppress their cascading 42I18.
    val cascadeExemptItemExprs: Set[Ref[ASTNode]] = invalidGroupingElementResults.flatMap(_.exemptItemExprs).toSet

    // 42I18: with GROUP BY, every non-aggregating item must derive from the grouping keys.
    val nonAggregatingItemScopes =
      scopesFor(NonAggregatingPart).filter(_.incoming match {
        case ctx: ProjectionExpressionContext => ctx.projectionSpecification.hasGroupBy
        case _                                => false
      })
    val nonAggregatingToCheck =
      if (cascadeExemptItemExprs.isEmpty) nonAggregatingItemScopes
      else nonAggregatingItemScopes.filterNot(s => cascadeExemptItemExprs(Ref(s.astNode)))

    val invalidReferences =
      invalidRefsIn(scopesFor(AggregatingPart), AggregatingPart) ++
        invalidRefsIn(scopesFor(AggregatingSubclausePart), AggregatingSubclausePart) ++
        invalidRefsIn(nonAggregatingToCheck, NonAggregatingPart)

    val ambiguousReferences =
      Option.when(invalidReferences.nonEmpty) {
        SemanticError.invalidReferenceToNonGroupingExpression(
          invalidReferences.sortBy(_.position).map(_.name).distinct,
          invalidReferences.head.position
        )
      }

    // 42N44: variables referenced in sort/WHERE subclauses must be accessible in this part.
    val subclauseInaccessibleVariable =
      scopesFor(NonAggregatingSubclausePart)
        .flatMap(s => checkScope(s, inaccessibleVariable(clauseName, from, groupBySupported)))

    (ambiguousReferences ++ subclauseInaccessibleVariable ++ invalidGroupingElementResults.map(_.error)).toSet
  }

  def legacyIllegalAggregationCheck(clause: ProjectionClause): Set[SemanticError] = {
    clause.orderBy.toSeq.flatMap(_.checkIllegalOrdering(clause.returnItems)).toSet
  }

  def checkClause(
    from: BaseState,
    clause: ProjectionClause,
    scope: WorkingScope,
    version: CypherVersion
  ): Set[SemanticError] =
    if (clause.isAggregating) checkAggregatingClause(from, clause, scope, version)
    else checkNonAggregatingClause(clause)

  def checkAggregatingClause(
    from: BaseState,
    clause: ProjectionClause,
    scope: WorkingScope,
    version: CypherVersion
  ): Set[SemanticError] =
    traverseScope(from, clause.name, scope, groupBySupported = version != CypherVersion.Cypher5) ++
      (if (version == CypherVersion.Cypher5) legacyIllegalAggregationCheck(clause) else Set.empty)

  def checkNonAggregatingClause(clause: ProjectionClause): Set[SemanticError] =
    Elements(clause).subclauses.sortAndPredicateExpressions.flatMap(invalidUseOfAggregation).toSet ++
      legacyIllegalAggregationCheck(clause)

}
