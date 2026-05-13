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
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence

import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults
import org.neo4j.cypher.internal.compiler.planner.logical.VectorSearchExceptionHandler
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VectorSearchClause
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.Selectivity

object SearchClauseCardinalityModel {

  /**
   * The SEARCH clause limits the cardinality of its bound variable to be at most x, where x is
   * specified by the LIMIT within the SEARCH clause.
   * When the limit is less than the index cardinality:
   *  - the selectivity is: limit / indexCardinality
   *  Otherwise:
   *  - the selectivity is 1 (the SEARCH clause does not restrict the cardinality of the QueryGraph).
   * @param indexLabelsCardinality The cardinality of disjunctive labels from the index
   * @param searchLimit The value in the LIMIT within the SEARCH clause
   * @return The selectivity of the SEARCH clause
   */
  private def getSearchSelectivity(indexLabelsCardinality: Double, searchLimit: Expression): Selectivity = {
    val vectorSearchLimit = searchLimit match {
      case SignedDecimalIntegerLiteral(v: String) => Math.max(0.0, v.toDouble)
      case _                                      => PlannerDefaults.DEFAULT_LIMIT_ROW_COUNT.toDouble
    }

    if (vectorSearchLimit >= indexLabelsCardinality)
      Selectivity.ONE
    else
      Selectivity(vectorSearchLimit / indexLabelsCardinality)
  }

  /**
   * Get a cardinality estimate of (:L1|L2|L3) where types L1, L2 and L3 are the labels specified by the search index
   * @param indexSearchVariable The bound node variable of the search clause
   * @param searchIndexLabelPredicate The predicate representing a disjunction of labels on the `indexSearchVariable`
   * @param context The QueryGraphCardinalityContext
   * @return A cardinality estimate for the number of nodes that have at least one of the index labels
   */
  private def indexDisjunctiveLabelsCardinality(
    indexSearchVariable: LogicalVariable,
    searchIndexLabelPredicate: Predicate,
    context: QueryGraphCardinalityContext
  ): Cardinality = {
    val searchIndexLabelQueryGraph = QueryGraph(
      patternNodes = Set(indexSearchVariable),
      selections = Selections(Set(searchIndexLabelPredicate))
    )
    context.cardinalityModel.apply(
      RegularSinglePlannerQuery(queryGraph = searchIndexLabelQueryGraph),
      LabelInfo.empty,
      context.relTypeInfo,
      context.semanticTable,
      context.indexPredicateProviderContext,
      context.cardinalityModel,
      context.graphSchemaOptimizations
    )
  }

  private def nodeVectorSearchClauseSelectivity(
    queryGraph: QueryGraph,
    context: QueryGraphCardinalityContext,
    planContext: PlanContext,
    vectorSearch: VectorSearchClause
  ): (Selectivity, QueryGraph, QueryGraphCardinalityContext) = {
    // Get the labels of the node vector index
    val disjunctiveIndexLabelIds = planContext.nodeVectorIndexByName(vectorSearch.indexName) match {
      case Right(descriptor) => descriptor.labelIds
      case Left(vectorIndexError) =>
        VectorSearchExceptionHandler.handleErrors(
          vectorIndexError,
          vectorSearch.indexName,
          vectorSearch.resultVariable.name
        )
    }
    assert(
      disjunctiveIndexLabelIds.nonEmpty,
      "Vector Index must have at least one label"
    )
    val indexLabelIdToNameMap = disjunctiveIndexLabelIds
      .map(labelId => (labelId, planContext.getLabelName(labelId)))
      .toMap

    // Predicate for a disjunction of labels on the bound node of the search clause
    val searchIndexLabelPredicate =
      Predicate(
        Set(vectorSearch.resultVariable),
        Ors.create(
          indexLabelIdToNameMap
            .map {
              case (_, labelName) =>
                HasLabels(
                  vectorSearch.resultVariable,
                  Seq(LabelName(labelName)(InputPosition.NONE))
                )(InputPosition.NONE)
            }
        )
      )

    val updatedQueryGraph =
      queryGraph.withSelections(Selections(queryGraph.selections.predicates + searchIndexLabelPredicate))

    val updatedContext = context.copy(
      semanticTable = context.semanticTable.addResolvedLabelNames(indexLabelIdToNameMap.map(_.swap))
    )

    val searchIndexLabelsCardinality =
      indexDisjunctiveLabelsCardinality(vectorSearch.resultVariable, searchIndexLabelPredicate, updatedContext)

    (getSearchSelectivity(searchIndexLabelsCardinality.amount, vectorSearch.limit), updatedQueryGraph, updatedContext)
  }

  private def addTypesToQueryGraphAndSemanticTable(
    indexTypeIdToNameMap: Map[RelTypeId, String],
    queryGraph: QueryGraph,
    context: QueryGraphCardinalityContext,
    vectorSearch: VectorSearchClause
  ): (QueryGraph, QueryGraphCardinalityContext) = {
    val typeNames =
      indexTypeIdToNameMap.map(indexTypeIdToName => RelTypeName(indexTypeIdToName._2)(InputPosition.NONE)).toSeq

    val maybePatternRelationship = queryGraph.patternRelationships.find(_.variable == vectorSearch.resultVariable)
    val updatedQueryGraph =
      maybePatternRelationship match {
        case Some(patternRelationship) =>
          // Take the intersection of the disjunctive types on the relationships and the disjunctive types on the index
          val intersectionOfDisjunctiveTypes = patternRelationship.types.intersect(typeNames)
          // Update the types on the relationship
          val patternRelationshipWithIndexTypes =
            patternRelationship.copy(
              types =
                if (patternRelationship.types.isEmpty) typeNames
                else intersectionOfDisjunctiveTypes
            )
          // Add the type to the pattern relationship
          queryGraph.withPatternRelationships(
            queryGraph.patternRelationships
              .filterNot(_.variable == vectorSearch.resultVariable)
              + patternRelationshipWithIndexTypes
          )
        case _ =>
          queryGraph
      }

    val updatedContext = context.copy(
      semanticTable = indexTypeIdToNameMap.foldLeft(context.semanticTable) {
        (acc, indexTypeIdToName) => acc.addResolvedRelTypeName(indexTypeIdToName._2, indexTypeIdToName._1)
      }
    )
    (updatedQueryGraph, updatedContext)
  }

  /**
   * Get a cardinality estimate of ()-[:R1|R2|R3]->() where types R1, R2 and R3 are the types specified by the search index
   * @param indexSearchVariable The bound relationship variable of the search clause
   * @param indexTypeIdToNameMap The types of the search index together with the type names
   * @return A cardinality estimate for the number of relationships that have at least one of the index types
   */
  private def indexDisjunctiveTypesCardinality(
    indexSearchVariable: LogicalVariable,
    indexTypeIdToNameMap: Map[RelTypeId, String],
    queryGraph: QueryGraph,
    context: QueryGraphCardinalityContext
  ): Cardinality = {
    val maybePatternRelationship =
      queryGraph.patternRelationships.find(_.variable == indexSearchVariable)
    val indexTypesQueryGraph = {
      maybePatternRelationship match {
        case Some(rel) =>
          QueryGraph(
            patternRelationships = Set(PatternRelationship(
              indexSearchVariable,
              (rel.left, rel.right),
              rel.dir,
              indexTypeIdToNameMap.map(relIdToName => RelTypeName(relIdToName._2)(InputPosition.NONE)).toSeq,
              SimplePatternLength
            )),
            patternNodes = rel.nodes
          )
        case None => QueryGraph()
      }
    }
    context.cardinalityModel.apply(
      RegularSinglePlannerQuery(queryGraph = indexTypesQueryGraph),
      LabelInfo.empty,
      context.relTypeInfo,
      context.semanticTable,
      context.indexPredicateProviderContext,
      context.cardinalityModel,
      context.graphSchemaOptimizations
    )
  }

  private def relationshipVectorSearchClauseSelectivity(
    queryGraph: QueryGraph,
    context: QueryGraphCardinalityContext,
    planContext: PlanContext,
    vectorSearch: VectorSearchClause
  ): (Selectivity, QueryGraph, QueryGraphCardinalityContext) = {
    // Get the type of the relationship vector index
    val indexTypeIds = planContext.relationshipVectorIndexByName(vectorSearch.indexName) match {
      case Right(descriptor) => descriptor.relTypeIds
      case Left(vectorIndexError) =>
        VectorSearchExceptionHandler.handleErrors(
          vectorIndexError,
          vectorSearch.indexName,
          vectorSearch.resultVariable.name
        )
    }
    assert(
      indexTypeIds.nonEmpty,
      "Vector Index must have at least one type"
    )

    val indexTypeIdToNameMap = indexTypeIds.map(typeId => (typeId, planContext.getRelTypeName(typeId))).toMap

    val maybePatternRelationship = queryGraph.patternRelationships.find(_.variable == vectorSearch.resultVariable)

    maybePatternRelationship match {
      case Some(pr) if pr.types.nonEmpty && pr.types.map(_.name).intersect(indexTypeIdToNameMap.values.toSeq).isEmpty =>
        // The relationship had at least one type specified, but none of the index types.
        // This means that the relationship cannot be fulfilled.
        (Selectivity.ZERO, queryGraph, context)
      case _ =>
        // Add the index' types to the semantic table and to the queryGraph's patternRelationship
        val (updatedQueryGraph, updatedContext) =
          addTypesToQueryGraphAndSemanticTable(indexTypeIdToNameMap, queryGraph, context, vectorSearch)

        val searchIndexTypesCardinality = indexDisjunctiveTypesCardinality(
          vectorSearch.resultVariable,
          indexTypeIdToNameMap,
          updatedQueryGraph,
          updatedContext
        )

        (
          getSearchSelectivity(searchIndexTypesCardinality.amount, vectorSearch.limit),
          updatedQueryGraph,
          updatedContext
        )
    }
  }

  /**
   * Estimate the selectivity of the SEARCH clause, and update the queryGraph and the queryGraphCardinalityContext with
   * the labels/types that are implied by the search clause.
   */
  def searchClauseSelectivity(
    queryGraph: QueryGraph,
    context: QueryGraphCardinalityContext,
    planContext: PlanContext
  ): (Selectivity, QueryGraph, QueryGraphCardinalityContext) = {
    queryGraph.searchClause.fold((Selectivity.ONE, queryGraph, context)) {
      case vs: VectorSearchClause =>
        if (queryGraph.patternNodes.contains(vs.resultVariable))
          nodeVectorSearchClauseSelectivity(queryGraph, context, planContext, vs)
        else
          relationshipVectorSearchClauseSelectivity(queryGraph, context, planContext, vs)
    }
  }
}
