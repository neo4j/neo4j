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
package org.neo4j.cypher.internal.frontend.phases.parserTransformers

import org.neo4j.cypher.internal.ast.AlterAuthRule
import org.neo4j.cypher.internal.ast.AlterCurrentGraphType
import org.neo4j.cypher.internal.ast.AlterUser
import org.neo4j.cypher.internal.ast.AlterUsers
import org.neo4j.cypher.internal.ast.CollectExpression
import org.neo4j.cypher.internal.ast.ConditionalQueryWhen
import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.CreateAuthRule
import org.neo4j.cypher.internal.ast.CreateUser
import org.neo4j.cypher.internal.ast.DropAuthRule
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.ExplicitGroupingElements
import org.neo4j.cypher.internal.ast.GroupBy
import org.neo4j.cypher.internal.ast.GroupingAll
import org.neo4j.cypher.internal.ast.GroupingNone
import org.neo4j.cypher.internal.ast.ImportingWithSubqueryCall
import org.neo4j.cypher.internal.ast.LoadCSV
import org.neo4j.cypher.internal.ast.NextStatement
import org.neo4j.cypher.internal.ast.ParsedAsFilter
import org.neo4j.cypher.internal.ast.ParsedAsLet
import org.neo4j.cypher.internal.ast.RenameAuthRule
import org.neo4j.cypher.internal.ast.ScopeClauseSubqueryCall
import org.neo4j.cypher.internal.ast.Search
import org.neo4j.cypher.internal.ast.ShowAuthRules
import org.neo4j.cypher.internal.ast.ShowCurrentGraphTypeClause
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.expressions.MatchMode.RepeatableElements
import org.neo4j.cypher.internal.expressions.PathMode.Acyclic
import org.neo4j.cypher.internal.expressions.PatternPart.SelectiveSelector
import org.neo4j.cypher.internal.expressions.PrefixedPatternPart
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.expressions.ShortestPathsPatternPart
import org.neo4j.cypher.internal.frontend.phases.BaseContains
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.METADATA_COLLECTION
import org.neo4j.cypher.internal.frontend.phases.SyntaxUsageMetricKey
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.VisitorPhase
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.factories.ParsingConfig
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.IsolateSubqueriesInMutatingPatterns.SubqueriesInMutatingPatternsIsolated
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Condition
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition

/**
 * Collects usage statistics about several syntactical CYPHER features
 * and reports them as a metric.
 */
case object CollectSyntaxUsageMetrics
    extends VisitorPhase[BaseContext, BaseState]
    with StepSequencer.Step
    with ParsePipelineTransformerFactory
    with DefaultPostCondition {

  override def visit(state: BaseState, context: BaseContext): Unit = {
    def increaseMetric(key: SyntaxUsageMetricKey): Unit = {
      context.internalUsageStats.incrementSyntaxUsageCount(key)
    }

    var isLoadCsvQuery = false
    var isCallInTxQuery = false
    var isCallInTxConcurrentQuery = false

    state.statement().folder.treeForeach {
      case ppp: PrefixedPatternPart if !ppp.pathMode.implicitlyCreated && ppp.isSelective =>
        // We found a path pattern with a selective selector and explicit path mode.
        // Do not also increase, for example, GPM_SHORTEST. We will traverse further down the tree and reach that case later.
        increaseMetric(SyntaxUsageMetricKey.GPM_SHORTEST_WITH_EXPLICIT_PATH_MODE)
      case _: SelectiveSelector =>
        increaseMetric(SyntaxUsageMetricKey.GPM_SHORTEST)
      case _: ShortestPathsPatternPart =>
        increaseMetric(SyntaxUsageMetricKey.LEGACY_SHORTEST)
      case _: QuantifiedPath =>
        increaseMetric(SyntaxUsageMetricKey.QUANTIFIED_PATH_PATTERN)
      case _: ExistsExpression =>
        increaseMetric(SyntaxUsageMetricKey.EXISTS_SUBQUERY)
      case _: CountExpression =>
        increaseMetric(SyntaxUsageMetricKey.COUNT_SUBQUERY)
      case _: CollectExpression =>
        increaseMetric(SyntaxUsageMetricKey.COLLECT_SUBQUERY)
      case _: RepeatableElements =>
        increaseMetric(SyntaxUsageMetricKey.REPEATABLE_ELEMENTS)
      case _: Acyclic =>
        increaseMetric(SyntaxUsageMetricKey.ACYCLIC_PATH_MODE)
      case ParsedAsLet =>
        increaseMetric(SyntaxUsageMetricKey.LET_CLAUSE)
      case ParsedAsFilter =>
        increaseMetric(SyntaxUsageMetricKey.FILTER_CLAUSE)
      case _: LoadCSV =>
        isLoadCsvQuery = true
      case sq: ImportingWithSubqueryCall =>
        increaseMetric(SyntaxUsageMetricKey.IMPORTING_WITH_SUBQUERY)
        if (sq.isCorrelated) {
          increaseMetric(SyntaxUsageMetricKey.IMPORTING_WITH_SUBQUERY_CORRELATED)
        } else {
          increaseMetric(SyntaxUsageMetricKey.IMPORTING_WITH_SUBQUERY_UNCORRELATED)
        }
        if (sq.inTransactionsParameters.isDefined) {
          isCallInTxQuery = true
          if (sq.inTransactionsParameters.get.concurrencyParams.isDefined) {
            isCallInTxConcurrentQuery = true
          }
        }
      case sq: ScopeClauseSubqueryCall =>
        increaseMetric(SyntaxUsageMetricKey.SCOPE_CLAUSE_SUBQUERY)
        if (sq.inTransactionsParameters.isDefined) {
          isCallInTxQuery = true
          if (sq.inTransactionsParameters.get.concurrencyParams.isDefined) {
            isCallInTxConcurrentQuery = true
          }
        }
      case gb: GroupBy =>
        increaseMetric(SyntaxUsageMetricKey.GROUP_BY)
        gb.groupingElements match {
          case _: ExplicitGroupingElements => increaseMetric(SyntaxUsageMetricKey.GROUP_BY_EXPLICIT)
          case _: GroupingAll              => increaseMetric(SyntaxUsageMetricKey.GROUP_BY_ALL)
          case _: GroupingNone             => increaseMetric(SyntaxUsageMetricKey.GROUP_BY_NONE)
        }
      case _: ConditionalQueryWhen =>
        increaseMetric(SyntaxUsageMetricKey.CONDITIONAL_QUERY)
      case _: NextStatement =>
        increaseMetric(SyntaxUsageMetricKey.NEXT_STATEMENT)
      case Search(_, _, _, _, None, _) =>
        increaseMetric(SyntaxUsageMetricKey.SEARCH_WITHOUT_FILTERS)
      case Search(_, _, _, _, Some(_), _) =>
        increaseMetric(SyntaxUsageMetricKey.SEARCH_WITH_FILTERS)
      case _: AlterCurrentGraphType =>
        increaseMetric(SyntaxUsageMetricKey.ALTER_CURRENT_GRAPH_TYPE)
      case _: ShowCurrentGraphTypeClause =>
        increaseMetric(SyntaxUsageMetricKey.SHOW_CURRENT_GRAPH_TYPE)
      case _: CreateAuthRule =>
        increaseMetric(SyntaxUsageMetricKey.CREATE_AUTH_RULE)
      case _: RenameAuthRule =>
        increaseMetric(SyntaxUsageMetricKey.RENAME_AUTH_RULE)
      case _: AlterAuthRule =>
        increaseMetric(SyntaxUsageMetricKey.ALTER_AUTH_RULE)
      case _: DropAuthRule =>
        increaseMetric(SyntaxUsageMetricKey.DROP_AUTH_RULE)
      case _: ShowAuthRules =>
        increaseMetric(SyntaxUsageMetricKey.SHOW_AUTH_RULES)
      case c: CreateUser if c.tags.isDefined =>
        increaseMetric(SyntaxUsageMetricKey.CREATE_USER_TAGS)
      case a: AlterUser if a.tags.nonEmpty =>
        increaseMetric(SyntaxUsageMetricKey.ALTER_USER_TAGS)
      case a: AlterUsers if a.tags.nonEmpty =>
        increaseMetric(SyntaxUsageMetricKey.ALTER_USERS_TAGS)
    }

    if (isLoadCsvQuery) {
      increaseMetric(SyntaxUsageMetricKey.LOAD_CSV)
      if (isCallInTxQuery) {
        increaseMetric(SyntaxUsageMetricKey.LOAD_CSV_CALL_IN_TX)
        if (isCallInTxConcurrentQuery) {
          increaseMetric(SyntaxUsageMetricKey.LOAD_CSV_CALL_IN_TX_CONCURRENT)
        }
      }
    }
  }

  override def postConditions: Set[Condition] = super[DefaultPostCondition].postConditions

  override def preConditions: Set[Condition] = Set(
    BaseContains[Statement](),
    // No rewriting should have happened. These are all postConditions from rewriting steps in
    // FrontEndCompilationPhases.parsingBase
    !DeprecatedSyntaxReplaced,
    !DeprecatedSemanticsReplaced,
    !SubqueriesInMutatingPatternsIsolated
  ) ++ PreparatoryRewriting.postConditions.map(!_)

  override def invalidatedConditions: Set[Condition] = Set.empty

  override def getTransformer(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] = this

  override def phase = METADATA_COLLECTION
}
