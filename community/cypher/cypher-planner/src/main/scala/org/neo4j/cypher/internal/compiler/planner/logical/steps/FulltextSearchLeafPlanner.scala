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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.SearchExceptionHandler
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.expressions.EntityType
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.ir.FulltextSearchClause
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.FulltextIndexDescriptor
import org.neo4j.exceptions.InternalException

/**
 * Plans fulltext search leaf plans for nodes or relationships in SEARCH sub-clauses.
 *
 * Unlike vector search, a fulltext SEARCH sub-clause has no WHERE/filter machinery (CIP-225
 * forbids WHERE together with FULLTEXT INDEX), so there is no query-expression-from-WHERE step.
 */
case object FulltextSearchLeafPlanner extends LeafPlanner {

  override def apply(
    queryGraph: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Set[LogicalPlan] = {
    queryGraph.searchClause match {
      case Some(search @ FulltextSearchClause(
          resultVariable,
          indexName,
          queryString,
          analyzer,
          skip,
          limit,
          scoreVariable
        ))
        if search.isSolvableGivenSymbols(queryGraph.argumentIds) =>

        if (queryGraph.patternNodes.contains(resultVariable)) {
          context.staticComponents.planContext.nodeFulltextIndexByName(indexName) match {
            case Right(descriptor) =>
              val labelTokens =
                descriptor.labelIds.map(labelId =>
                  LabelToken(context.staticComponents.planContext.getLabelName(labelId), labelId)
                )

              val propertyKeyTokens = getPropertyKeyTokens(context, descriptor)

              val implicitlySolvedPredicates =
                SearchImplicitPredicates.solvedNodePredicates(
                  resultVariable,
                  solvesIsNotNullOnIndex(resultVariable, propertyKeyTokens),
                  labelTokens,
                  queryGraph.selections.flatPredicatesSet
                )

              val indexedProperties =
                getIndexedProperties(propertyKeyTokens, NODE_TYPE)

              val nodeFulltextIndexSearch =
                context.staticComponents.logicalPlanProducer.planNodeFulltextIndexSearch(
                  context = context,
                  resultVariable = resultVariable,
                  labels = labelTokens,
                  indexedProperties = indexedProperties,
                  indexName = indexName,
                  queryString = queryString,
                  analyzer = analyzer,
                  skip = skip,
                  limit = limit,
                  scoreVariable = scoreVariable,
                  argumentIds = queryGraph.argumentIds,
                  implicitlySolvedPredicates = implicitlySolvedPredicates
                )
              Set(nodeFulltextIndexSearch)

            case Left(fulltextIndexError) => SearchExceptionHandler.handleErrors(
                fulltextIndexError,
                indexName,
                resultVariable.name
              )
          }
        } else {
          val patternRelationship =
            queryGraph.patternRelationships.find(_.variable == resultVariable).getOrElse(
              throw InternalException.internalError(
                "FulltextSearchLeafPlanner",
                "The binding variable of the fulltext search is not a node or relationship in the query graph"
              )
            )

          context.staticComponents.planContext.relationshipFulltextIndexByName(indexName) match {
            case Right(descriptor) =>
              val indexedTypes = descriptor.relTypeIds.map(relTypeId =>
                RelationshipTypeToken(context.staticComponents.planContext.getRelTypeName(relTypeId), relTypeId)
              )

              val propertyKeyTokens = getPropertyKeyTokens(context, descriptor)

              val implicitlySolvedPredicates =
                SearchImplicitPredicates.solvedRelationshipPredicates(
                  resultVariable,
                  solvesIsNotNullOnIndex(resultVariable, propertyKeyTokens),
                  indexedTypes,
                  queryGraph.selections.flatPredicatesSet
                )

              val indexedProperties =
                getIndexedProperties(propertyKeyTokens, RELATIONSHIP_TYPE)

              val relationshipFulltextIndexSearch =
                context.staticComponents.logicalPlanProducer.planRelationshipFulltextIndexSearch(
                  context = context,
                  patternRelationship = patternRelationship,
                  indexedTypes = indexedTypes,
                  indexedProperties = indexedProperties,
                  indexName = indexName,
                  queryString = queryString,
                  analyzer = analyzer,
                  skip = skip,
                  limit = limit,
                  scoreVariable = scoreVariable,
                  argumentIds = queryGraph.argumentIds,
                  implicitlySolvedPredicates = implicitlySolvedPredicates
                )
              Set(relationshipFulltextIndexSearch)

            case Left(fulltextIndexError) => SearchExceptionHandler.handleErrors(
                fulltextIndexError,
                indexName,
                resultVariable.name
              )
          }
        }

      case _ => Set.empty
    }
  }

  private def getPropertyKeyTokens(
    context: LogicalPlanningContext,
    fulltextIndexDescriptor: FulltextIndexDescriptor
  ): Seq[PropertyKeyToken] =
    fulltextIndexDescriptor.properties.map { nameId =>
      PropertyKeyToken(
        name = context.staticComponents.planContext.getPropertyKeyName(nameId.id),
        nameId = nameId
      )
    }

  private def getIndexedProperties(
    propertyKeyTokens: Seq[PropertyKeyToken],
    entityType: EntityType
  ): Seq[IndexedProperty] =
    propertyKeyTokens.map { propertyKeyToken =>
      IndexedProperty(
        propertyKeyToken,
        // A fulltext index categorically cannot return property values
        // (FulltextIndexCapability.supportsReturningValues() == false), so the search leaf must never
        // claim to provide them. Downstream property reads are served from the store instead.
        getValueFromIndex = DoNotGetValue,
        entityType = entityType
      )
    }

  /**
   * IS NOT NULL is only implicitly solved for a single-property index: a match from a multi-property
   * fulltext index does not imply that any one particular property is non-null, since the match could
   * have come from a different indexed property.
   */
  private def solvesIsNotNullOnIndex(
    fulltextSearchVariable: LogicalVariable,
    propertyKeyTokens: Seq[PropertyKeyToken]
  ): Expression => Boolean =
    propertyKeyTokens match {
      case Seq(singleProperty) =>
        SearchImplicitPredicates.solvesIsNotNull(fulltextSearchVariable, singleProperty, _)
      case _ => _ => false
    }
}
