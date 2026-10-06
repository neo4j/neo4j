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

import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Search
import org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery.SearchWithComplexPatternConverter.renameVariables
import org.neo4j.cypher.internal.expressions.CompileEntityFilter
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.UnPositionedVariable
import org.neo4j.cypher.internal.expressions.functions.Collect
import org.neo4j.cypher.internal.expressions.functions.Id
import org.neo4j.cypher.internal.ir.AggregatingQueryProjection
import org.neo4j.cypher.internal.ir.CallSubqueryHorizon
import org.neo4j.cypher.internal.ir.FulltextSearchClause
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryProjection
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.SearchClause
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.UnwindProjection
import org.neo4j.cypher.internal.ir.VectorSearchClause
import org.neo4j.cypher.internal.ir.VectorSearchClause.MatchFilterConfig
import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters.PredicateConverter
import org.neo4j.cypher.internal.macros.AssertMacros3.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.Rewritable.RewritableAny
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.helpers.NameDeduplicator
import org.neo4j.cypher.internal.util.topDown
import org.neo4j.exceptions.InternalException

import scala.util.chaining.scalaUtilChainingOps

/**
 * Given a MATCH-SEARCH with complex pattern:
 *
 * MATCH (movie:Movie)<-[rel:DIRECTED]-(director:Person)
 *   SEARCH movie IN (
 *     VECTOR INDEX moviePlots
 *     FOR $param
 *     LIMIT 5
 *   )
 * RETURN movie.title AS title
 *
 * Convert it to a subquery which solves the pattern first, then performs SEARCH using matched elements as entity filter.
 * Equivalent of (almost real Cypher):
 *
 * CALL (*) {
 *   MATCH (inner_movie:Movie)<-[inner_rel:DIRECTED]-(inner_director:Person)
 *   WITH
 *     inner_movie,
 *     collect({inner_movie: inner_movie, inner_rel: inner_rel, inner_director: inner_director}) AS rows
 *   WITH
 *     compile_match_filter(id(inner_movie)) AS candidateIds,
 *     collect({inner_movie: inner_movie, rows: rows}) AS groups
 *   UNWIND groups AS group
 *   MATCH (movie)
 *   SEARCH movie IN (
 *     VECTOR INDEX moviePlots
 *     FOR $param
 *     LIMIT 5
 *     entityFilter = candidateIds
 *   )
 *   WHERE group.inner_movie = movie
 *   UNWIND group.rows AS record
 *   RETURN
 *     record.inner_director AS director,
 *     record.inner_movie AS movie,
 *     record.inner_rel AS rel
 * }
 */
class SearchWithComplexPatternConverter(
  statementConverters: StatementConverters,
  anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
  cancellationChecker: CancellationChecker
) {

  def convertIfExists(acc: PlannerQueryBuilder, matchClause: Match): Option[PlannerQueryBuilder] = {
    for {
      search <- matchClause.search
      if !search.patternComplexity.isSimple && search.indexType == Search.Vector
    } yield {
      val importedVariables = acc.currentlyAvailableVariables // CALL (*) { ... }
      val isolatedMatchClause = matchClause.copy(search = None, optional = false)(matchClause.position)
      val accForSubquery = acc.copy(q = acc.emptySinglePlannerQuery)
      val subquery = buildSubquery(accForSubquery, isolatedMatchClause, search, importedVariables)
      acc.withCallSubquery(
        subquery = subquery,
        correlated = importedVariables.nonEmpty,
        yielding = true,
        inTransactionsParameters = None,
        optional = matchClause.optional,
        importedVariables = importedVariables,
        importedSymbolsFromLastCallSubquery = acc.importedVariables
      )
    }
  }

  private def buildSubquery(
    acc: PlannerQueryBuilder,
    matchAst: Match,
    searchAst: Search,
    importedVars: Set[LogicalVariable]
  ): PlannerQuery = {
    val exportedVars =
      matchAst
        .allExportedVariables
        .removedAll(importedVars)
        .filter(v => AnonymousVariableNameGenerator.isNamed(v.name))
        .toVector.sortBy(_.name)

    // Generate "inner" variable names to solve the pattern with. Original names will be projected at the end to make results available to the rest of the query.
    val originalToInnerVarRenamings = exportedVars.map(v => v -> genVar(innerNameForVar(v))).toMap
    val innerExportedVars = originalToInnerVarRenamings.values.toSet

    val rewrittenMatchAst = renameVariables(matchAst, originalToInnerVarRenamings, cancellationChecker)
    val rewrittenSearchAst = renameVariables(searchAst, originalToInnerVarRenamings, cancellationChecker)
    val innerBindingVar = rewrittenSearchAst.bindingVariable
    val newBindingVar = genVar(searchAst.bindingVariable.name) // generate fresh name for the SEARCH query graph

    val matchFilterConfig = buildMatchFilterConfig(innerBindingVar = innerBindingVar, newBindingVar = newBindingVar)
    val searchClause = convertSearchClause(rewrittenSearchAst, matchFilterConfig, newBindingVar)

    acc
      .pipe(addInnerPattern(_, rewrittenMatchAst, importedVars))
      .pipe { acc =>
        val innerQueryGraph = acc.currentQueryGraph

        // WHERE sub-clause might contain predicates depending on the score variable, which are not solvable at this point
        val (matchSelections, innerScoreVarSelections) = innerQueryGraph.selections.partition(p =>
          p.dependencies.intersect(searchClause.scoreVariable.toSet).isEmpty
        )

        val searchPatternDependencies = innerExportedVars.intersect(searchClause.dependencies)

        // collect({inner_movie: inner_movie ...}) AS rows
        val rowsVar = genVar("rows")

        acc
          .amendQueryGraph(_.withSelections(matchSelections))
          .pipe {
            addCollectRowsAggregation(
              _,
              rowsVar = rowsVar,
              innerExportedVars = innerExportedVars,
              innerBindingVar = innerBindingVar,
              searchPatternDependencies = searchPatternDependencies,
              importedVars = importedVars
            )
          }
          .pipe {
            addCollectCandidateIdsAggregation(
              _,
              candidateIdsVar = matchFilterConfig.candidateIds,
              groupsCollectionVar = matchFilterConfig.groupsCollection,
              rowsVar = rowsVar,
              innerBindingVar = innerBindingVar,
              searchPatternDependencies = searchPatternDependencies,
              indexName = searchClause.indexName,
              importedVars = importedVars
            )
          }
          .pipe {
            addSearchFilter(
              _,
              searchClause = searchClause,
              innerQueryGraph = innerQueryGraph,
              innerBindingVar = innerBindingVar
            )
          }
          .pipe {
            returnOriginalVariables(
              _,
              groupVar = matchFilterConfig.group,
              rowsVar = rowsVar,
              maybeScoreVar = searchClause.scoreVariable,
              innerScoreVarSelections = innerScoreVarSelections,
              originalToInnerVarRenamings = originalToInnerVarRenamings,
              importedVars = importedVars
            )
          }
          .build()
      }
  }

  private def buildMatchFilterConfig(
    innerBindingVar: LogicalVariable,
    newBindingVar: LogicalVariable
  ): MatchFilterConfig = {
    // compile_match_filter(id(inner_movie)) AS candidateIds
    val candidateIdsVar = genVar("candidateIds")
    // collect({inner_movie: inner_movie, rows: rows}) AS groups
    val groupsCollectionVar = genVar("groups")
    // UNWIND groups AS group
    val groupVar = genVar("group")

    // WHERE group.inner_movie = movie
    val joinPredicate = Equals(
      property(map = groupVar, propertyName = innerBindingVar.name),
      newBindingVar
    )(pos)

    VectorSearchClause.MatchFilterConfig(
      candidateIds = candidateIdsVar,
      groupsCollection = groupsCollectionVar,
      group = groupVar,
      joinPredicate = joinPredicate
    )
  }

  private def convertSearchClause(
    rewrittenSearchAst: Search,
    matchFilterConfig: MatchFilterConfig,
    newBindingVar: LogicalVariable
  ): VectorSearchClause = {
    val withRewrittenBindingVar =
      renameVariables(rewrittenSearchAst, Map(rewrittenSearchAst.bindingVariable -> newBindingVar), cancellationChecker)
    SearchClause.fromAst(withRewrittenBindingVar) match {
      case vsc: VectorSearchClause =>
        vsc.withMatchFilterConfig(matchFilterConfig)
      case ftsc: FulltextSearchClause =>
        throw InternalException.internalError(
          "SearchWithComplexPatternConverter",
          s"Expected ${classOf[VectorSearchClause].getSimpleName}, got: $ftsc"
        )
    }
  }

  /**
   * MATCH (inner_movie:Movie)<-[inner_rel:DIRECTED]-(inner_director:Person)
   */
  private def addInnerPattern(
    acc: PlannerQueryBuilder,
    matchClause: Match,
    importedVars: Set[LogicalVariable]
  ): PlannerQueryBuilder = {
    statementConverters
      .addClausesToPlannerQueryBuilder(
        Seq(matchClause),
        acc,
        anonymousVariableNameGenerator,
        cancellationChecker,
        position = QueryProjection.Position.Intermediate
      )
      .amendQueryGraph(_.addArgumentIds(importedVars))
  }

  /**
   * WITH
   *   inner_movie,
   *   collect({inner_movie: inner_movie, inner_rel: inner_rel, inner_director: inner_director}) AS rows
   */
  private def addCollectRowsAggregation(
    acc: PlannerQueryBuilder,
    rowsVar: LogicalVariable,
    innerExportedVars: Set[LogicalVariable],
    innerBindingVar: LogicalVariable,
    searchPatternDependencies: Set[LogicalVariable],
    importedVars: Set[LogicalVariable]
  ): PlannerQueryBuilder = {
    val groupingVariables = searchPatternDependencies + innerBindingVar

    val rowAsMapExpr = MapExpression(
      innerExportedVars.toSeq.sortBy(_.name).map(v => propertyKeyName(v.name) -> v)
    )(pos)

    val aggregation = AggregatingQueryProjection(
      groupingExpressions = groupingVariables.map(v => v -> v).toMap,
      aggregationExpressions = Map(rowsVar -> Collect(rowAsMapExpr)(pos)),
      importedExposedSymbols = importedVars
    )
    acc
      .withHorizon(aggregation)
      .withTail(acc.emptySinglePlannerQuery)
  }

  /**
   * WITH
   *   compile_match_filter(id(inner_movie)) AS candidateIds,
   *   collect({inner_movie: inner_movie, rows: rows}) AS groups
   */
  private def addCollectCandidateIdsAggregation(
    acc: PlannerQueryBuilder,
    candidateIdsVar: LogicalVariable,
    groupsCollectionVar: LogicalVariable,
    rowsVar: LogicalVariable,
    innerBindingVar: LogicalVariable,
    searchPatternDependencies: Set[LogicalVariable],
    indexName: String,
    importedVars: Set[LogicalVariable]
  ): PlannerQueryBuilder = {
    val groupAsMapExpr = MapExpression(Seq(
      propertyKeyName(innerBindingVar.name) -> innerBindingVar,
      propertyKeyName(rowsVar.name) -> rowsVar
    ))(pos)
    val aggregation = AggregatingQueryProjection(
      groupingExpressions = searchPatternDependencies.map(v => v -> v).toMap,
      aggregationExpressions = Map(
        candidateIdsVar -> CompileEntityFilter(Id(innerBindingVar)(pos), indexName)(pos),
        groupsCollectionVar -> Collect(groupAsMapExpr)(pos)
      ),
      importedExposedSymbols = importedVars
    )
    acc
      .withHorizon(aggregation)
      .withTail(acc.emptySinglePlannerQuery)
  }

  /**
   * SEARCH movie IN (
   *   VECTOR INDEX moviePlots
   *   FOR $param
   *   LIMIT 5
   *   entityFilter = candidateIds
   * )
   * WHERE group.inner_movie = movie
   */
  private def addSearchFilter(
    acc: PlannerQueryBuilder,
    searchClause: SearchClause,
    innerQueryGraph: QueryGraph,
    innerBindingVar: LogicalVariable
  ): PlannerQueryBuilder = {

    // SEARCH planner requires binding variable to be present in the query graph.
    // Lookup binding variable in the "inner" pattern query graph, add it with a new name to the SEARCH query graph.

    val bindingNodeVar = innerQueryGraph.patternNodes.collect {
      case node if node == innerBindingVar =>
        searchClause.resultVariable
    }
    val bindingRelVar = innerQueryGraph.patternRelationships.collect {
      case rel if rel.variable == innerBindingVar =>
        rel.copy(
          variable = searchClause.resultVariable,
          boundaryNodes = (genVar(rel.left.name), genVar(rel.right.name))
        )
    }

    checkOnlyWhenAssertionsAreEnabled(
      bindingNodeVar.size + bindingRelVar.size == 1,
      s"Expected binding variable to be either a single node or a single relationship, but got $bindingNodeVar, $bindingRelVar"
    )

    acc.amendQueryGraph(qg =>
      qg
        .addPatternNodes(bindingNodeVar.toSeq*)
        .addPatternRelationships(bindingRelVar)
        .addSearchClause(Some(searchClause))
    )
  }

  /**
   * UNWIND group.rows AS record
   * RETURN
   *   record.inner_director AS director,
   *   record.inner_movie AS movie,
   *   record.inner_rel AS rel
   */
  private def returnOriginalVariables(
    acc: PlannerQueryBuilder,
    groupVar: LogicalVariable,
    rowsVar: LogicalVariable,
    maybeScoreVar: Option[LogicalVariable],
    innerScoreVarSelections: Selections,
    originalToInnerVarRenamings: Map[LogicalVariable, LogicalVariable],
    importedVars: Set[LogicalVariable]
  ): PlannerQueryBuilder = {

    val recordVar = genVar("record")

    // return original variables from the subquery
    val finalProjectionMap =
      originalToInnerVarRenamings.map((originalVar, innerVar) =>
        originalVar -> property(map = recordVar, propertyName = innerVar.name)
      ) ++
        maybeScoreVar.map(v => v -> v)

    val unwindAsRecord = UnwindProjection(
      variable = recordVar,
      exp = property(map = groupVar, propertyName = rowsVar.name),
      importedSymbolsFromLastCallSubquery = importedVars
    )

    val innerToOriginalVarRenamings = originalToInnerVarRenamings.map(_.swap)
    val scoreVarSelections = Selections(
      innerScoreVarSelections
        .flatPredicatesSet
        .flatMap(renameVariables(_, innerToOriginalVarRenamings, cancellationChecker).asPredicates)
    )

    val projectOriginalNames = RegularQueryProjection(
      projections = finalProjectionMap,
      position = QueryProjection.Position.Intermediate,
      importedExposedSymbols = importedVars,
      selections = scoreVarSelections
    )
    acc
      .withHorizon(unwindAsRecord)
      .withTail(acc.emptySinglePlannerQuery)
      .withHorizon(projectOriginalNames)
  }

  private def genVar(name: String): LogicalVariable = {
    UnPositionedVariable.varFor(AnonymousVariableNameGenerator.genName(anonymousVariableNameGenerator, name))
  }

  private def innerNameForVar(v: LogicalVariable): String = {
    "inner_" + NameDeduplicator.removeGeneratedNamesAndParams(v.name)
  }

  private def pos: InputPosition = InputPosition.NONE

  private def propertyKeyName(propertyName: String): PropertyKeyName = {
    PropertyKeyName(propertyName)(pos)
  }

  private def property(map: LogicalVariable, propertyName: String): Property = {
    Property(
      map = map,
      propertyKey = propertyKeyName(propertyName)
    )(pos)
  }
}

object SearchWithComplexPatternConverter {

  private def renameVariables[T <: AnyRef](
    x: T,
    renameFromTo: Map[LogicalVariable, LogicalVariable],
    cancellationChecker: CancellationChecker
  ): T = {
    val topLevelExpressions = x.folder(cancellationChecker).treeFold(Set.empty[Ref[Expression]]) {
      case e: Expression => acc => SkipChildren(acc + Ref(e))
    }

    val rewriter = topDown(
      rewriter = Rewriter.lift {
        case expr: Expression =>
          renameFromTo.foldLeft(expr) {
            case (expr, (fromVar, toVar)) => expr.replaceAllOccurrencesBy(fromVar, toVar.copyId)
          }
      },
      stopper = {
        // only rewrite top-level expressions, rely on `replaceAllOccurrencesBy` to handle inner expressions and scoping
        case e: Expression => !topLevelExpressions.contains(Ref(e))
        case _             => false
      },
      cancellation = cancellationChecker
    )

    x.endoRewrite(rewriter)
  }

  /**
   * Recognizes a CALL subquery built by [[SearchWithComplexPatternConverter.buildSubquery]].
   * Returns query graph containing MATCH pattern with SEARCH clause included.
   */
  object MatchFilterSubquery {

    def unapply(callSubqueryHorizon: CallSubqueryHorizon): Option[QueryGraph] =
      callSubqueryHorizon.callSubquery match {
        case RegularSinglePlannerQuery(
            matchPatternQueryGraph,
            _,
            _: AggregatingQueryProjection,
            Some(RegularSinglePlannerQuery(
              _,
              _,
              _: AggregatingQueryProjection,
              Some(RegularSinglePlannerQuery(
                searchFilterQueryGraph @ QueryGraph(
                  _,
                  _,
                  _,
                  _,
                  _,
                  _,
                  _,
                  _,
                  _,
                  _,
                  Some(vsc @ VectorSearchClause(_, _, _, _, _, _, Some(matchFilterConfig: MatchFilterConfig)))
                ),
                _,
                _: UnwindProjection,
                Some(RegularSinglePlannerQuery(_, _, _: RegularQueryProjection, None, _)),
                _
              )),
              _
            )),
            _
          ) =>
          // recreate inner binding variable from `group.<innerBindingVarName> = <newBindingVar>` join predicate
          val innerBindingVarName: Option[String] = matchFilterConfig.joinPredicate.lhs match {
            case Property(_, PropertyKeyName(name)) => Some(name)
            case _                                  => None
          }

          val possibleBindingVars =
            matchPatternQueryGraph.patternNodes ++ matchPatternQueryGraph.patternRelationships.map(_.variable)

          for {
            name <- innerBindingVarName
            innerBindingVar <- possibleBindingVars.find(_.name == name)
          } yield {
            val innerVsc = renameVariables(
              vsc.copy(matchFilterConfig = None),
              Map(vsc.resultVariable -> innerBindingVar),
              CancellationChecker.NeverCancelled
            )
            matchPatternQueryGraph.addSearchClause(Some(innerVsc))
          }

        case _ => None
      }
  }
}
