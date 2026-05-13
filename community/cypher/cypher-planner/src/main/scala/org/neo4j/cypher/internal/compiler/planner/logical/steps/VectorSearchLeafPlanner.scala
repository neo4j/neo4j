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

import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.VectorSearchExceptionHandler
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.EntityType
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasLabel
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.expressions.VectorFilterExpression
import org.neo4j.cypher.internal.expressions.VectorFilterExpression.LowerBoundVectorFilterExpression
import org.neo4j.cypher.internal.expressions.VectorFilterExpression.SeekRangeVectorFilterExpression
import org.neo4j.cypher.internal.expressions.VectorFilterExpression.UpperBoundVectorFilterExpression
import org.neo4j.cypher.internal.expressions.VectorFilterExpression.VectorFilterExpressionRange
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.VectorSearchClause
import org.neo4j.cypher.internal.logical.plans.AllQueryExpression
import org.neo4j.cypher.internal.logical.plans.CompositeQueryExpression
import org.neo4j.cypher.internal.logical.plans.ExclusiveBound
import org.neo4j.cypher.internal.logical.plans.ExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.HalfOpenSeekRange
import org.neo4j.cypher.internal.logical.plans.InclusiveBound
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.InequalitySeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ManyQueryExpression
import org.neo4j.cypher.internal.logical.plans.NonExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.logical.plans.RangeBetween
import org.neo4j.cypher.internal.logical.plans.RangeGreaterThan
import org.neo4j.cypher.internal.logical.plans.RangeLessThan
import org.neo4j.cypher.internal.logical.plans.RangeQueryExpression
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.planner.spi.VectorIndexDescriptor
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.exceptions.InternalException
import org.neo4j.exceptions.VectorIndexSearchException

/**
 * Plans ANN vector search leaf plans for nodes or relationships in SEARCH sub-clauses.
 */
case object VectorSearchLeafPlanner extends LeafPlanner {

  override def apply(
    queryGraph: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Set[LogicalPlan] = {
    queryGraph.searchClause match {
      case Some(search @ VectorSearchClause(resultVariable, indexName, embedding, where, limit, scoreVariable))
        if solvableGivenSymbols(search, queryGraph.argumentIds) =>

        if (queryGraph.patternNodes.contains(resultVariable)) {
          context.staticComponents.planContext.nodeVectorIndexByName(indexName) match {
            case Right(descriptor) =>
              val labelTokens =
                descriptor.labelIds.map(labelId =>
                  LabelToken(context.staticComponents.planContext.getLabelName(labelId), labelId)
                )

              val propertyKeyToken =
                PropertyKeyToken(
                  name = context.staticComponents.planContext.getPropertyKeyName(descriptor.property.id),
                  nameId = descriptor.property
                )
              val implicitlySolvedPredicates =
                solvedNodePredicates(
                  resultVariable,
                  propertyKeyToken,
                  labelTokens,
                  queryGraph.selections.flatPredicatesSet
                )

              val indexedProperties =
                getIndexedProperties(
                  context,
                  resultVariable,
                  descriptor,
                  queryGraph.selections.flatPredicatesSet -- implicitlySolvedPredicates,
                  NODE_TYPE
                )

              val maybeFilter =
                queryExpressionFromWhereClause(
                  where,
                  descriptor.additionalProperties.map(propKeyId =>
                    context.staticComponents.planContext.getPropertyKeyName(propKeyId.id)
                  ),
                  indexName
                )

              val nodeVectorIndexSearch =
                context.staticComponents.logicalPlanProducer.planNodeVectorIndexSearch(
                  context = context,
                  resultVariable = resultVariable,
                  labels = labelTokens,
                  indexedProperties = indexedProperties,
                  indexName = indexName,
                  embedding = embedding,
                  where = where,
                  maybeFilter = maybeFilter,
                  limit = limit,
                  scoreVariable = scoreVariable,
                  argumentIds = queryGraph.argumentIds,
                  implicitlySolvedPredicates = implicitlySolvedPredicates
                )
              Set(nodeVectorIndexSearch)

            case Left(vectorIndexError) => VectorSearchExceptionHandler.handleErrors(
                vectorIndexError,
                indexName,
                resultVariable.name
              )
          }
        } else {
          val patternRelationship =
            queryGraph.patternRelationships.find(_.variable == resultVariable).getOrElse(
              throw InternalException.internalError(
                "VectorSearchLeafPlanner",
                "The binding variable of the vector search is not a node or relationship in the query graph"
              )
            )

          context.staticComponents.planContext.relationshipVectorIndexByName(indexName) match {
            case Right(descriptor) =>
              val propertyKeyToken = PropertyKeyToken(
                name = context.staticComponents.planContext.getPropertyKeyName(descriptor.property.id),
                nameId = descriptor.property
              )

              val indexedTypes = descriptor.relTypeIds.map(relTypeId =>
                RelationshipTypeToken(context.staticComponents.planContext.getRelTypeName(relTypeId), relTypeId)
              )

              val implicitlySolvedPredicates =
                solvedRelationshipPredicates(
                  resultVariable,
                  propertyKeyToken,
                  indexedTypes,
                  queryGraph.selections.flatPredicatesSet
                )

              val indexedProperties =
                getIndexedProperties(
                  context,
                  resultVariable,
                  descriptor,
                  queryGraph.selections.flatPredicatesSet -- implicitlySolvedPredicates,
                  RELATIONSHIP_TYPE
                )

              val maybeFilter =
                queryExpressionFromWhereClause(
                  where,
                  descriptor.additionalProperties.map(propKeyId =>
                    context.staticComponents.planContext.getPropertyKeyName(propKeyId.id)
                  ),
                  indexName
                )

              val relationshipVectorIndexSearch =
                context.staticComponents.logicalPlanProducer.planRelationshipVectorIndexSearch(
                  context = context,
                  patternRelationship = patternRelationship,
                  indexedTypes = indexedTypes,
                  indexedProperties = indexedProperties,
                  indexName = indexName,
                  embedding = embedding,
                  where = where,
                  maybeFilter = maybeFilter,
                  limit = limit,
                  scoreVariable = scoreVariable,
                  argumentIds = queryGraph.argumentIds,
                  implicitlySolvedPredicates = implicitlySolvedPredicates
                )
              Set(relationshipVectorIndexSearch)

            case Left(vectorIndexError) => VectorSearchExceptionHandler.handleErrors(
                vectorIndexError,
                indexName,
                resultVariable.name
              )
          }
        }

      case _ => Set.empty
    }
  }

  private def getIndexedProperties(
    context: LogicalPlanningContext,
    resultVariable: LogicalVariable,
    vectorIndexDescriptor: VectorIndexDescriptor,
    selections: Set[Expression],
    entityType: EntityType
  ) = {
    val propertyKeyTokens =
      vectorIndexDescriptor.properties
        .map { nameId =>
          PropertyKeyToken(
            name = context.staticComponents.planContext.getPropertyKeyName(nameId.id),
            nameId = nameId
          )
        }

    propertyKeyTokens.map { propertyKeyToken =>
      IndexedProperty(
        propertyKeyToken,
        getValueFromIndex =
          context.settings.remoteBatchPropertiesStrategy.getValueFromIndexBehavior(
            resultVariable,
            propertyKeyToken.name,
            selections,
            context.plannerState.contextualPropertyAccess
          ),
        entityType = entityType
      )
    }
  }

  private def solvedNodePredicates(
    vectorSearchVariable: LogicalVariable,
    propertyKeyToken: PropertyKeyToken,
    labelTokens: Seq[LabelToken],
    predicates: Set[Expression]
  ): Set[Expression] = {
    def isSolved: Expression => Boolean =
      labelTokens match {
        case Seq(labelToken) =>
          expression =>
            solvesIsNotNull(vectorSearchVariable, propertyKeyToken, expression) ||
              solvesHasLabel(vectorSearchVariable, labelToken, expression)
        case _ =>
          // If the index is defined for more than one label, the logic needs to be more complicated.
          // For example, if it is defined for (n:Foo|Bar), it implicitly solves x:Foo|Bar or any superset like
          // x:Foo|Bar|Zor but not x:Foo or x:Bar|Zor.
          // We decided not to handle it for now.
          solvesIsNotNull(vectorSearchVariable, propertyKeyToken, _)
      }

    predicates.filter(expandToOr(isSolved))
  }

  private def solvedRelationshipPredicates(
    vectorSearchVariable: LogicalVariable,
    propertyKeyToken: PropertyKeyToken,
    relationshipTokens: Seq[RelationshipTypeToken],
    predicates: Set[Expression]
  ): Set[Expression] = {
    def isSolved: Expression => Boolean =
      if (relationshipTokens.size == 1) {
        val typeToken = relationshipTokens.head
        expression =>
          solvesIsNotNull(vectorSearchVariable, propertyKeyToken, expression) ||
            solvesHasTypes(vectorSearchVariable, typeToken, expression)
      } else {
        // If the index is defined for more than one relationship type, the logic needs to be more complicated.
        // For example, if it is defined for ()-[r:R|S]-(), it implicitly solves x:R|S or any superset like
        // x:R|S|T but not x:R or x:S|T.
        // We decided not to handle it for now.
        solvesIsNotNull(vectorSearchVariable, propertyKeyToken, _)
      }

    predicates.filter(expandToOr(isSolved))
  }

  private def expandToOr(predicate: Expression => Boolean): Expression => Boolean = {
    case Ors(nestedExpressions) => nestedExpressions.exists(predicate)
    case otherExpression        => predicate(otherExpression)
  }

  private def solvesIsNotNull(
    vectorSearchVariable: LogicalVariable,
    propertyKeyToken: PropertyKeyToken,
    expression: Expression
  ): Boolean =
    expression match {
      case IsNotNull(Property(`vectorSearchVariable`, propertyKey)) if propertyKey.name == propertyKeyToken.name => true
      case _ => false
    }

  private def solvesHasLabel(
    vectorSearchVariable: LogicalVariable,
    labelToken: LabelToken,
    expression: Expression
  ): Boolean =
    expression match {
      case HasLabel(`vectorSearchVariable`, lbl) if lbl.name == labelToken.name => true
      case _                                                                    => false
    }

  private def solvesHasTypes(
    vectorSearchVariable: LogicalVariable,
    typeToken: RelationshipTypeToken,
    expression: Expression
  ): Boolean =
    expression match {
      case HasTypes(`vectorSearchVariable`, relTypes)
        if relTypes.size == 1 && relTypes.exists(_.name == typeToken.name) => true
      case _ => false
    }

  def solvableGivenSymbols(search: VectorSearchClause, availableSymbols: Set[LogicalVariable]): Boolean = {
    search.dependencies.subsetOf(availableSymbols)
  }

  def queryExpressionFromWhereClause(
    maybeWhere: Option[Where],
    additionalProperties: Seq[String],
    indexName: String
  ): Option[QueryExpression[Expression]] = {
    maybeWhere.map { where =>
      val expressionsByProperty = asFilterExpressions(where.expression).groupBy(_.propertyName)
      checkPropertiesAgainstIndex(additionalProperties, indexName, expressionsByProperty.keys)
      val queryExpressions =
        additionalProperties
          .map(expressionsByProperty.getOrElse(_, Seq.empty))
          .map(queryExpressionForOneProperty)
      compose(queryExpressions)
    }
  }

  private def queryExpressionForOneProperty(expressionsByProperty: Seq[VectorFilterExpression]) =
    expressionsByProperty match {
      case Seq() =>
        AllQueryExpression
      case Seq(singleton) =>
        queryExpressionFromFilterExpression(singleton)
      case VectorFilterExpressionRange(lowerBound, upperBound) =>
        queryExpressionFromBounds(lowerBound, upperBound)
      case expr =>
        throw InternalException.internalError(
          "Unsupported filter expression in SEARCH WHERE",
          s"$expr is not supported to appear in the WHERE part of a SEARCH sub-clause"
        )
    }

  private def checkPropertiesAgainstIndex(
    additionalProperties: Seq[String],
    indexName: String,
    usedProperties: Iterable[String]
  ): Unit = {
    val unknownProperties = usedProperties.filterNot(additionalProperties.contains)
    if (unknownProperties.nonEmpty) {
      throw VectorIndexSearchException.propertyNotFound(unknownProperties.mkString(", "), indexName)
    }
  }

  private def asFilterExpressions(expression: Expression): Seq[VectorFilterExpression] =
    Ands.unwrap(expression).toSeq
      .map(asFilterExpression)

  private def asFilterExpression(expression: Expression): VectorFilterExpression = expression match {
    case VectorFilterExpression(_, _, operator: VectorFilterExpression) => operator
    case expr =>
      throw InternalException.internalError(
        "Unsupported filter expression in SEARCH WHERE",
        s"$expr is not supported to appear in the WHERE part of a SEARCH sub-clause"
      )
  }

  private val pos = InputPosition.NONE

  private def compose(queryExpressions: Seq[QueryExpression[Expression]]): QueryExpression[Expression] =
    queryExpressions match {
      case Seq(singleton) => singleton
      case multiple       => CompositeQueryExpression(multiple)
    }

  private def queryExpressionFromBounds(
    lowerBound: LowerBoundVectorFilterExpression,
    upperBound: UpperBoundVectorFilterExpression
  ) =
    RangeQueryExpression(InequalitySeekRangeWrapper(RangeBetween(
      seekRangeFromFilterExpression(lowerBound),
      seekRangeFromFilterExpression(upperBound)
    ))(pos))

  private def queryExpressionFromFilterExpression(
    filterExpression: VectorFilterExpression
  ): QueryExpression[Expression] = filterExpression match {
    case VectorFilterExpression.Equality(_, expression) => SingleQueryExpression(expression)
    case VectorFilterExpression.Exists(_)               => ExistenceQueryExpression
    case VectorFilterExpression.NotExists(_)            => NonExistenceQueryExpression
    case VectorFilterExpression.InSet(_, expression)    => ManyQueryExpression(expression)
    case expr: SeekRangeVectorFilterExpression =>
      RangeQueryExpression(InequalitySeekRangeWrapper(
        seekRangeFromFilterExpression(expr)
      )(pos))
  }

  private def seekRangeFromFilterExpression(
    filterExpression: SeekRangeVectorFilterExpression
  ): HalfOpenSeekRange[Expression] = filterExpression match {
    case expr: LowerBoundVectorFilterExpression => seekRangeFromFilterExpression(expr)
    case expr: UpperBoundVectorFilterExpression => seekRangeFromFilterExpression(expr)
  }

  private def seekRangeFromFilterExpression(
    filterExpression: LowerBoundVectorFilterExpression
  ): RangeGreaterThan[Expression] = filterExpression match {
    case VectorFilterExpression.VectorFilterGreaterThan(_, expression) =>
      RangeGreaterThan(NonEmptyList(ExclusiveBound(expression)))
    case VectorFilterExpression.VectorFilterGreaterThanOrEqual(_, expression) =>
      RangeGreaterThan(NonEmptyList(InclusiveBound(expression)))
  }

  private def seekRangeFromFilterExpression(
    filterExpression: UpperBoundVectorFilterExpression
  ): RangeLessThan[Expression] = filterExpression match {
    case VectorFilterExpression.VectorFilterLessThan(_, expression) =>
      RangeLessThan(NonEmptyList(ExclusiveBound(expression)))
    case VectorFilterExpression.VectorFilterLessThanOrEqual(_, expression) =>
      RangeLessThan(NonEmptyList(InclusiveBound(expression)))
  }
}
