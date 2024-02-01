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
package org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery

import org.neo4j.cypher.internal.ast.CollectExpression
import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.logical.steps.QuerySolvableByGetDegree.SetExtractor
import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.PathStep
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.functions.Exists
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.flattenBooleanOperators
import org.neo4j.cypher.internal.ir.AggregatingQueryProjection
import org.neo4j.cypher.internal.ir.CallSubqueryHorizon
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryHorizon
import org.neo4j.cypher.internal.ir.QueryPagination
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.ast.CountIRExpression
import org.neo4j.cypher.internal.ir.ast.ExistsIRExpression
import org.neo4j.cypher.internal.ir.ast.ListIRExpression
import org.neo4j.cypher.internal.ir.converters.SimplePatternConverters
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.rewriting.rewriters.AddUniquenessPredicates
import org.neo4j.cypher.internal.rewriting.rewriters.AddVarLengthPredicates
import org.neo4j.cypher.internal.rewriting.rewriters.PredicateNormalizer
import org.neo4j.cypher.internal.rewriting.rewriters.projectNamedPaths
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.topDown

case class CreateIrExpressions(
  anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
  semanticTable: SemanticTable
) extends Rewriter {
  private val pathStepBuilder: PathPatternPart => PathStep = projectNamedPaths.patternPartPathExpression
  private val stringifier = ExpressionStringifier(_.asCanonicalStringVal)
  private val inlinedWhereClausesNormalizer = PredicateNormalizer.normalizeInlinedWhereClauses

  private val LabelAndPropertyNormalizer =
    PredicateNormalizer.normalizeLabelAndPropertyPredicates(anonymousVariableNameGenerator)
  private val addUniquenessPredicates = AddUniquenessPredicates
  private val addVarLengthPredicates = AddVarLengthPredicates

  /**
   * MatchPredicateNormalizer invalidates some conditions that are usually fixed by later rewriters.
   * The only one that is crucial to fix is that And => Ands and Or => Ors because that is assumed in IR creation.
   */
  private val fixExtractedPredicatesRewriter =
    flattenBooleanOperators

  private def createPathExpression(pattern: PatternExpression): PathExpression = {
    val pos = pattern.position
    val path = PatternPart(pattern.pattern.element)
    val step: PathStep = pathStepBuilder(path)
    PathExpression(step)(pos)
  }

  /**
   * Get the [[PlannerQuery]] for a pattern from a [[PatternExpression]] or [[ExistsExpression]] or [[CountExpression]].
   *
   * @param pattern        the pattern
   * @param dependencies   the dependencies or the expression
   * @param maybePredicate a WHERE clause predicate
   * @param horizon        the horizon to put into the query.
   */
  private def getPlannerQuery(
    pattern: ASTNode,
    dependencies: Set[LogicalVariable],
    maybePredicate: Option[Expression],
    horizon: QueryHorizon
  ): PlannerQuery = {
    val pathPattern = pattern match {
      case relationshipsPattern: RelationshipsPattern =>
        SimplePatternConverters.convertSimplePattern(relationshipsPattern.element)
      case _ =>
        throw new IllegalArgumentException(s"Cannot get planner query, unexpected pattern: $pattern")
    }

    // Create predicates for relationship uniqueness
    val uniquePredicates = addUniquenessPredicates.createPredicatesFor(pattern)

    val varLengthPredicates = addVarLengthPredicates.createPredicatesFor(pattern)

    // Extract inlined predicates
    val extractedPredicates: Seq[Expression] = {
      (inlinedWhereClausesNormalizer.extractAllFrom(pattern) ++
        LabelAndPropertyNormalizer.extractAllFrom(pattern)).endoRewrite(fixExtractedPredicatesRewriter)
    }

    val qg = QueryGraph(
      argumentIds = dependencies,
      selections = Selections.from(uniquePredicates ++ varLengthPredicates ++ extractedPredicates ++ maybePredicate)
    ).addPathPattern(pathPattern)

    val query = RegularSinglePlannerQuery(
      queryGraph = qg,
      horizon = horizon
    )
    PlannerQueryBuilder.finalizeQuery(query)
  }

  private val instance: Rewriter = topDown(Rewriter.lift {
    /**
     * Rewrites exists( (n)-[anon_0]->(anon_1:M) ) into
     * IR for MATCH (n)-[anon_0]->(anon_1:M)
     */
    case exists @ Exists(pe @ PatternExpression(pattern)) =>
      val existsVariable = varFor(anonymousVariableNameGenerator.nextName)
      val query = getPlannerQuery(pattern, pe.dependencies, None, RegularQueryProjection())
      ExistsIRExpression(query, existsVariable, stringifier(exists))(
        exists.position,
        pe.computedIntroducedVariables,
        pe.computedScopeDependencies
      )

    /**
     * Rewrites exists{ MATCH (n)-[anon_0]->(anon_1:M) RETURN n} into
     * IR for MATCH (n)-[anon_0]->(anon_1:M) RETURN n
     *
     */
    case existsExpression @ ExistsExpression(q) =>
      val existsVariable = varFor(anonymousVariableNameGenerator.nextName)
      val plannerQuery = StatementConverters.convertToPlannerQuery(
        q,
        semanticTable,
        anonymousVariableNameGenerator,
        CancellationChecker.NeverCancelled,
        existsExpression.scopeDependencies
      )
      ExistsIRExpression(plannerQuery, existsVariable, stringifier(existsExpression))(
        existsExpression.position,
        existsExpression.computedIntroducedVariables,
        existsExpression.computedScopeDependencies
      )

    /**
     * Rewrites (n)-[anon_0]->(anon_1:M) into
     * IR for MATCH (n)-[anon_0]->(anon_1:M) RETURN PathExpression(NodePathStep(n, RelationshipPathStep(anon_0, NodePathStep(anon_1, NilPathStep))))
     */
    case pe @ PatternExpression(pattern) =>
      val variableToCollect = varFor(anonymousVariableNameGenerator.nextName)
      val collection = varFor(anonymousVariableNameGenerator.nextName)

      val pathExpression = createPathExpression(pe)
      val query = getPlannerQuery(
        pattern,
        pe.dependencies,
        None,
        RegularQueryProjection(Map(variableToCollect -> pathExpression))
      )
      ListIRExpression(query, variableToCollect, collection, stringifier(pe))(
        pe.position,
        pe.computedIntroducedVariables,
        pe.computedScopeDependencies
      )

    /**
     * Rewrites COUNT { (n)-[anon_0]->(anon_1:M) } into
     * IR for MATCH (n)-[anon_0]->(anon_1:M) RETURN count(*)
     */
    case countExpression @ CountExpression(q) =>
      val countVariable = varFor(anonymousVariableNameGenerator.nextName)
      val arguments = countExpression.dependencies
      val plannerQuery = StatementConverters.convertToPlannerQuery(
        q,
        semanticTable,
        anonymousVariableNameGenerator,
        CancellationChecker.NeverCancelled,
        arguments
      )

      /**
       * For single queries not containing SKIP, LIMIT, WHERE, DISTINCT, aggregation, etc.,
       * it is fine to just override the horizon with an aggregating projection and rewrite away the InterestingOrder.
       * For other single queries we can add the aggregating projection in an additional tail.
       * This cannot be done for Union queries as it cannot be cast as a single planner query and it is the
       * result of the union that should be aggregated.
       * For these cases we instead add the query as a CallSubqueryHorizon
       * and then set the tail as the aggregating query projection.
       */
      val finalizedQuery = plannerQuery match {
        case query: RegularSinglePlannerQuery =>
          query.tailOrSelf.horizon match {
            // Simply some Return items but no SKIP, LIMIT, WHERE, DISTINCT, aggregation, etc.
            case RegularQueryProjection(_, QueryPagination(None, None), Selections(SetExtractor()), _) =>
              // We can simply override the final horizon with our aggregation.
              plannerQuery.asSinglePlannerQuery
                .updateTailOrSelf(_.withHorizon(
                  AggregatingQueryProjection(aggregationExpressions =
                    Map(countVariable -> CountStar()(q.position))
                  )
                ))
                // And also remove any ORDER BY since that won't have any impact in a COUNT subquery anyway.
                .updateTailOrSelf(_.withInterestingOrder(InterestingOrder.empty))
            case _ =>
              // In any other case we add another tail with our aggregation.
              plannerQuery.asSinglePlannerQuery
                .updateTailOrSelf(_.withTail(RegularSinglePlannerQuery(
                  horizon = AggregatingQueryProjection(aggregationExpressions =
                    Map(countVariable -> CountStar()(q.position))
                  )
                )))
          }
        case _ => RegularSinglePlannerQuery(
            queryGraph = QueryGraph(
              argumentIds = arguments
            ),
            horizon = CallSubqueryHorizon(
              callSubquery = plannerQuery,
              correlated = true,
              yielding = true,
              inTransactionsParameters = None
            ),
            tail = Some(
              RegularSinglePlannerQuery(
                horizon = AggregatingQueryProjection(aggregationExpressions =
                  Map(countVariable -> CountStar()(countExpression.position))
                )
              )
            )
          )
      }

      CountIRExpression(finalizedQuery, countVariable, stringifier(countExpression))(
        countExpression.position,
        countExpression.computedIntroducedVariables,
        countExpression.computedScopeDependencies
      )

    /**
     * Rewrites COLLECT { (n)-[anon_0]->(anon_1:M) RETURN n } into
     * IR for MATCH (n)-[anon_0]->(anon_1:M) RETURN n
     */
    case collectExpression @ CollectExpression(q) =>
      val collectVariable = varFor(anonymousVariableNameGenerator.nextName)
      val arguments = collectExpression.dependencies
      val plannerQuery = StatementConverters.convertToPlannerQuery(
        q,
        semanticTable,
        anonymousVariableNameGenerator,
        CancellationChecker.NeverCancelled,
        arguments
      )

      /**
       * Collect Subqueries may only return one item, we also know that all branches of Union
       * clauses will have the same variable name. This is all checked earlier in semantic checking.
       */
      val toCollectVar = collectExpression.query.returnVariables.explicitVariables.head

      ListIRExpression(plannerQuery, toCollectVar, collectVariable, stringifier(collectExpression))(
        collectExpression.position,
        collectExpression.computedIntroducedVariables,
        collectExpression.computedScopeDependencies
      )
  })

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
