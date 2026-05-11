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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.configuration.GraphDatabaseInternalSettings.RemoteBatchPropertiesImplementation
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.Union.UnionMapping
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.helpers.EntityAliases
import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper
import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper.ContextualPropertyAccess
import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper.PropertyAccess
import org.neo4j.cypher.internal.compiler.helpers.RenameChain
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.planner.logical.idp.ExtraRequirement
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.SelectionCandidate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.IndexCompatiblePredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexCompatiblePredicateWithValueBehavior
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasMappableExpressions
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.ir.CallSubqueryHorizon
import org.neo4j.cypher.internal.ir.MutatingPattern
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryHorizon
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.UnionQuery
import org.neo4j.cypher.internal.logical.plans.CachedProperties
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.ExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.GetValueFromIndexBehavior
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.logical.plans.RewrittenExpressions
import org.neo4j.cypher.internal.logical.plans.RewrittenSubQueryPredicates
import org.neo4j.cypher.internal.logical.plans.RewrittenSubQueryPredicates.RewrittenSubQueryPredicatesMap
import org.neo4j.cypher.internal.logical.plans.RewrittenSubQueryPredicates.withNoRewrittenExprs
import org.neo4j.cypher.internal.planner.spi.DatabaseMode.SHARDED
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewritable.RewritableAny
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.topDown

import scala.annotation.tailrec

sealed trait RemoteBatchingStrategy {

  def getValueFromIndexBehaviors(
    indexDescriptor: IndexDescriptor,
    propertyPredicates: Seq[IndexCompatiblePredicate],
    exactPredicatesCanGetValue: Boolean,
    context: LogicalPlanningContext,
    queryGraph: QueryGraph
  ): Seq[IndexCompatiblePredicateWithValueBehavior]

  def getValueFromIndexBehavior(
    indexEntityVariable: LogicalVariable,
    propertyKey: String,
    queryGraphPredicates: Set[Expression],
    contextualPropertyAccess: ContextualPropertyAccess
  ): GetValueFromIndexBehavior

  def planBatchPropertiesForSelections(
    queryGraph: QueryGraph,
    input: LogicalPlan,
    context: LogicalPlanningContext,
    predicatesToSolve: RewrittenSubQueryPredicatesMap
  ): RemoteBatchingSubQueryResult

  def planRemoteBatchProperties(
    inputPlan: LogicalPlan,
    context: LogicalPlanningContext,
    expressions: Iterable[Expression]
  ): RemoteBatchingResult

  def planRemoteBatchPropertiesForMutatingPattern[T <: MutatingPattern with HasMappableExpressions[T]](
    source: LogicalPlan,
    context: LogicalPlanningContext,
    pattern: T
  ): (LogicalPlan, T)

  def planBatchPropertiesForExpressionsWithLookahead(
    queryGraph: QueryGraph,
    input: LogicalPlan,
    context: LogicalPlanningContext,
    expressions: Iterable[Expression]
  ): RemoteBatchingResult

  def interestingPropertiesAsIDPExtraRequirement(
    queryGraph: QueryGraph,
    context: LogicalPlanningContext
  ): ExtraRequirement[LogicalPlan]

  def planPrefetchRemoteBatchPropertiesIfRequired(
    queryGraph: QueryGraph,
    plans: Iterable[LogicalPlan],
    context: LogicalPlanningContext
  ): Iterable[LogicalPlan]

  def planRemotePropertiesBeforeCall(
    query: SinglePlannerQuery,
    plan: LogicalPlan,
    context: LogicalPlanningContext
  ): LogicalPlan

  def planRemotePropertiesBeforeApplyOptional(
    optionalQg: QueryGraph,
    lhsPlan: LogicalPlan,
    context: LogicalPlanningContext
  ): LogicalPlan

  def planBatchPropertiesForHorizonSelections(
    queryGraph: QueryGraph,
    input: LogicalPlan,
    context: LogicalPlanningContext,
    predicatesToSolve: Set[Expression],
    interestingOrderConfig: InterestingOrderConfig
  ): RemoteBatchingSubQueryResult

  def findGlobalPropertyAccessesWithContext(
    query: SinglePlannerQuery
  ): ContextualPropertyAccess

  /**
   * Properties of external available symbols that are accessed by predicates inside the QPP.
   */
  def propertiesToFetchBeforeQpp(
    predicates: Iterable[Expression],
    availableSymbols: Set[LogicalVariable],
    semanticTable: SemanticTable
  ): CachedProperties

  def usePreviouslyCachedProperty(
    inputPlan: LogicalPlan,
    context: LogicalPlanningContext
  ): LogicalPlan
}

case class RemoteBatchingResult(
  rewrittenExpressionsWithCachedProperties: RewrittenExpressions,
  plan: LogicalPlan
)

case class RemoteBatchingSubQueryResult(
  rewrittenExpressionsWithCachedProperties: RewrittenSubQueryPredicatesMap,
  plan: LogicalPlan
)

object RemoteBatchingStrategy {

  def fromConfig(context: PlannerContext): RemoteBatchingStrategy = {
    if (
      context.planContext.databaseMode == SHARDED
      && context.config.remoteBatchPropertiesImplementation() == RemoteBatchPropertiesImplementation.PLANNER
    ) {
      InPlannerRemoteBatching
    } else
      SkipRemoteBatching
  }

  def defaultValue(): RemoteBatchingStrategy = SkipRemoteBatching

  case object InPlannerRemoteBatching extends RemoteBatchingStrategy {

    override def planRemoteBatchProperties(
      inputPlan: LogicalPlan,
      context: LogicalPlanningContext,
      expressions: Iterable[Expression]
    ): RemoteBatchingResult = {
      val accessedProperties = PropertyAccessHelper.findPropertyAccesses(expressions)
      val rewriter = cachedPropertiesRewriter(inputPlan, context)
      val rewrittenExpressions =
        RewrittenExpressions.forMap(expressions.map(expr => expr -> expr.endoRewrite(rewriter)).toMap)
      RemoteBatchingResult(
        rewrittenExpressions,
        planBatchProperties(inputPlan, context, accessedProperties, rewrittenExpressions.allRewrittenExpressions)
      )
    }

    override def planBatchPropertiesForSelections(
      queryGraph: QueryGraph,
      input: LogicalPlan,
      context: LogicalPlanningContext,
      predicatesToSolve: RewrittenSubQueryPredicatesMap
    ): RemoteBatchingSubQueryResult = {
      context.settings.shardOperatorPushdownStrategy.selections(input, queryGraph, context, predicatesToSolve) match {
        case Some(SelectionCandidate(plan, solvedPredicates)) =>
          val unsolvedPredicates = predicatesToSolve.backingStore.filterNot(rewrittenPredTuple =>
            solvedPredicates.contains(rewrittenPredTuple._2)
          )
          val RemoteBatchingSubQueryResult(rewrittenExprs, planWithProps) = planRemoteBatchPropertiesForSelections(
            queryGraph,
            plan,
            context,
            RewrittenSubQueryPredicates.forMap(unsolvedPredicates)
          )
          RemoteBatchingSubQueryResult(
            rewrittenExprs,
            planWithProps
          )

        case _ => planRemoteBatchPropertiesForSelections(queryGraph, input, context, predicatesToSolve)
      }
    }

    private def planRemoteBatchPropertiesForSelections(
      queryGraph: QueryGraph,
      input: LogicalPlan,
      context: LogicalPlanningContext,
      predicatesToSolve: RewrittenSubQueryPredicatesMap
    ): RemoteBatchingSubQueryResult = {
      val accessedProperties = remainingPropertyAccesses(queryGraph, input, context)

      val rewriter = cachedPropertiesRewriter(input, context)
      val rewrittenSelections =
        RewrittenSubQueryPredicates.forMap(predicatesToSolve.allRewrittenExpressions.map(expr =>
          expr.endoRewrite(rewriter) -> predicatesToSolve.originalExpressionOrSelf(expr)
        ).toMap)
      RemoteBatchingSubQueryResult(
        rewrittenExpressionsWithCachedProperties = rewrittenSelections,
        plan = planBatchProperties(input, context, accessedProperties, rewrittenSelections.allRewrittenExpressions)
      )
    }

    override def planRemoteBatchPropertiesForMutatingPattern[
      T <: MutatingPattern with HasMappableExpressions[T]
    ](
      source: LogicalPlan,
      context: LogicalPlanningContext,
      pattern: T
    ): (LogicalPlan, T) = {
      val RemoteBatchingResult(rewrittenPatternExpressions, rewrittenInner) =
        planRemoteBatchProperties(source, context, pattern.getExpressionsWithPossiblePropertyReferences)
      val rewrittenPattern = pattern.mapExpressions(rewrittenPatternExpressions.rewrittenExpressionOrSelf)
      (rewrittenInner, rewrittenPattern)
    }

    override def planBatchPropertiesForExpressionsWithLookahead(
      queryGraph: QueryGraph,
      input: LogicalPlan,
      context: LogicalPlanningContext,
      expressions: Iterable[Expression]
    ): RemoteBatchingResult = {
      val accessedProperties = remainingPropertyAccesses(queryGraph, input, context)

      val rewriter = cachedPropertiesRewriter(input, context)
      val rewrittenExpressions =
        RewrittenExpressions.forMap(expressions.map(expr => expr -> expr.endoRewrite(rewriter)).toMap)
      RemoteBatchingResult(
        rewrittenExpressions,
        planBatchProperties(input, context, accessedProperties, rewrittenExpressions.allRewrittenExpressions)
      )
    }

    protected def remainingPropertyAccesses(
      queryGraph: QueryGraph,
      input: LogicalPlan,
      context: LogicalPlanningContext,
      predicatesToIgnore: Set[Expression] = Set.empty
    ): Set[PropertyAccess] = {
      accessedPropertiesForPredicates(queryGraph, input, context, predicatesToIgnore) ++
        context.plannerState.contextualPropertyAccess.interestingOrder ++
        context.plannerState.contextualPropertyAccess.horizon ++
        context.plannerState.contextualPropertyAccess.queryGraphMutatingPatterns ++
        context.plannerState.contextualPropertyAccess.propertyAccessInOtherComponents
    }

    override def interestingPropertiesAsIDPExtraRequirement(
      queryGraph: QueryGraph,
      context: LogicalPlanningContext
    ): ExtraRequirement[LogicalPlan] = {
      new ExtraRequirement[LogicalPlan]() {
        override def fulfils(plan: LogicalPlan): Boolean = {
          val availableSymbols = plan.availableSymbols
          val cachedProperties = context.staticComponents.planningAttributes.cachedPropertiesPerPlan.get(plan.id)
          val interestingPropertyAccesses = remainingPropertyAccesses(
            queryGraph,
            plan,
            context
          ).collect {
            case propAccess @ PropertyAccess(variable, _) if availableSymbols.contains(variable) => Some(propAccess)
            case PropertyAccess(variable, propertyName)                                          =>
              // if it is a renamed variable, it suffices to check on the last available symbol since cached properties are updated on all renames.
              context.plannerState.contextualPropertyAccess.entityAliases.findLatestAvailableSymbols(
                variable,
                availableSymbols
              ) match {
                case Some(latestVar) => Some(PropertyAccess(latestVar, propertyName))
                case None            => None
              }
          }.flatten

          if (interestingPropertyAccesses.nonEmpty)
            interestingPropertyAccesses.forall(propertyAccess =>
              cachedProperties.contains(
                propertyAccess.variable,
                PropertyKeyName(propertyAccess.propertyName)(InputPosition.NONE)
              )
            )
          else
            false // we will force this to false since it makes IDP cheaper and let sorted plans take precedence.
        }
      }
    }

    // #EntityIndexSeekPlanProvider.predicatesForIndexSeek replaces non-seekable predicates with range-scannable ones.
    // The non-seekable predicates will then run as a filter on the main shard.
    // Therefore, for such predicates we will need to use the index to fetch properties, otherwise we will end-up with a redundant remoteBatchProperties call.
    private def isExistenceQueryExpressionExecutedLater(propertyPredicate: IndexCompatiblePredicate): Boolean =
      propertyPredicate.queryExpression match {
        case ExistenceQueryExpression => propertyPredicate.predicate match {
            case _: ExistsExpression =>
              false // the original query was an existence query, so no other predicate to execute later
            case _: IsNotNull =>
              false // the original query was an existence query, so no other predicate to execute later
            case _ => true // the original query was not an existence query, so we will execute this predicate later
          }
        case _ => false
      }

    private def shouldGetPropertyValue(
      propertyPredicate: IndexCompatiblePredicate,
      queryGraphPredicates: Set[Expression],
      contextualPropertyAccess: ContextualPropertyAccess
    ): Boolean = {
      isExistenceQueryExpressionExecutedLater(propertyPredicate) ||
      propertyOnIndexVariableIsAccessedAfterIndex(
        propertyPredicate.variable,
        propertyPredicate.propertyKeyName.name,
        queryGraphPredicates,
        Some(propertyPredicate.predicate),
        contextualPropertyAccess
      )
    }

    private def propertyOnIndexVariableIsAccessedAfterIndex(
      indexEntityVariable: LogicalVariable,
      propertyKey: String,
      queryGraphPredicates: Set[Expression],
      predicateSolvedByIndex: Option[Expression],
      contextualPropertyAccess: ContextualPropertyAccess
    ): Boolean = {
      val laterPatternAccesses = contextualPropertyAccess.horizon ++
        contextualPropertyAccess.interestingOrder ++
        contextualPropertyAccess.queryGraphMutatingPatterns ++
        contextualPropertyAccess.propertyAccessInOtherComponents

      propertyAccessesToPredicatesMap(queryGraphPredicates)
        .containsOtherPredicateThanExcludedOne(
          PropertyAccess(indexEntityVariable, propertyKey),
          excludedPredicate = predicateSolvedByIndex
        ) ||
      laterPatternAccesses.exists(propAccess =>
        propAccess.propertyName == propertyKey &&
          contextualPropertyAccess.entityAliases.isSameEntityAs(indexEntityVariable, propAccess.variable)
      )
    }

    override def planPrefetchRemoteBatchPropertiesIfRequired(
      queryGraph: QueryGraph,
      plans: Iterable[LogicalPlan],
      context: LogicalPlanningContext
    ): Iterable[LogicalPlan] = {
      val extraRequirement = interestingPropertiesAsIDPExtraRequirement(queryGraph, context)
      plans.flatMap { plan =>
        if (extraRequirement.fulfils(plan)) Iterator(plan)
        else planRemoteBatchPropertiesWithLookahead(queryGraph, plan, context)
      }.toVector
    }

    override def planRemotePropertiesBeforeCall(
      query: SinglePlannerQuery,
      plan: LogicalPlan,
      context: LogicalPlanningContext
    ): LogicalPlan = {
      // This restricts it to properties in the current CALL horizon
      val restrictedContext = context.withModifiedPlannerState(
        _.withContextualPropertyAccess(findGlobalPropertyAccessesWithContext(query.withoutTail))
      )
      // We want to plan all necessary remote batch properties before planning the subquery, so we have the chance to use previously cached properties for the subquery plan.
      planPrefetchRemoteBatchPropertiesIfRequired(
        QueryGraph.empty,
        Seq(plan),
        restrictedContext
      ).headOption.getOrElse(plan)
    }

    override def planRemotePropertiesBeforeApplyOptional(
      optionalQg: QueryGraph,
      lhsPlan: LogicalPlan,
      context: LogicalPlanningContext
    ): LogicalPlan = {
      // only fetch properties that are accessed in OPTIONAL MATCH
      val propertyAccess = findGlobalPropertyAccessesWithContext(SinglePlannerQuery.empty.withQueryGraph(optionalQg))
      val restrictedContext = context.withModifiedPlannerState(_.withContextualPropertyAccess(propertyAccess))
      planPrefetchRemoteBatchPropertiesIfRequired(
        optionalQg.removeArguments(), // normally we don't fetch properties for arguments, but in this case that's what we want, as we're fetching properties on LHS of an Apply
        Seq(lhsPlan),
        restrictedContext
      ).headOption.getOrElse(lhsPlan)
    }

    private def planRemoteBatchPropertiesWithLookahead(
      queryGraph: QueryGraph,
      input: LogicalPlan,
      context: LogicalPlanningContext
    ): Iterator[LogicalPlan] = {
      val accessedProperties = remainingPropertyAccesses(queryGraph, input, context)
      val alreadyCachedProperties =
        context.staticComponents.planningAttributes.cachedPropertiesPerPlan.get(input.id)
      val availableSymbols = input.availableSymbols
      val cachedPropertiesForHeadPlan = cachedPropertiesFromPropAccesses(
        accessedProperties,
        alreadyCachedProperties,
        availableSymbols,
        queryGraph.argumentIds,
        context
      )

      if (cachedPropertiesForHeadPlan.nonEmpty)
        Iterator(context.staticComponents.logicalPlanProducer.planRemoteBatchProperties(
          input,
          cachedPropertiesForHeadPlan,
          context
        ))
      else Iterator.empty
    }

    private def cachedPropertiesFromPropAccesses(
      accessedProperties: Set[PropertyAccess],
      alreadyCachedProperties: CachedProperties,
      availableSymbols: Set[LogicalVariable],
      queryGraphArguments: Set[LogicalVariable],
      context: LogicalPlanningContext
    ): Set[CachedProperty] =
      accessedProperties.collect {
        case PropertyAccess(variable, propertyName)
          if availableSymbols.contains(variable) &&
            !alreadyCachedProperties.contains(variable, PropertyKeyName(propertyName)(InputPosition.NONE)) &&
            !queryGraphArguments.contains(variable) =>
          toCachedProperty(
            context,
            alreadyCachedProperties,
            variable,
            PropertyKeyName(propertyName)(InputPosition.NONE),
            InputPosition.NONE,
            knownToCacheStore = true
          )
        case PropertyAccess(variable, propertyName) if !availableSymbols.contains(variable) =>
          // if it is a renamed variable, it suffices to check on the last available symbol since cached properties are updated on all renames.
          context.plannerState.contextualPropertyAccess.entityAliases.findLatestAvailableSymbols(
            variable,
            availableSymbols
          ) match {
            case Some(latestVar)
              if !alreadyCachedProperties.contains(
                latestVar,
                PropertyKeyName(propertyName)(InputPosition.NONE)
              ) && !queryGraphArguments.contains(variable) =>
              toCachedProperty(
                context,
                alreadyCachedProperties,
                latestVar,
                PropertyKeyName(propertyName)(InputPosition.NONE),
                InputPosition.NONE,
                knownToCacheStore = true
              )
            case _ => None
          }
      }.flatten

    override def getValueFromIndexBehaviors(
      indexDescriptor: IndexDescriptor,
      propertyPredicates: Seq[IndexCompatiblePredicate],
      exactPredicatesCanGetValue: Boolean,
      context: LogicalPlanningContext,
      queryGraph: QueryGraph
    ): Seq[IndexCompatiblePredicateWithValueBehavior] = {
      val contextualPropertyAccess = context.plannerState.contextualPropertyAccess
      val rewriter = externalPropertyAccessesRewriter(context)

      def determineGetValueBehaviour(predicate: IndexCompatiblePredicate) = {
        if (shouldGetPropertyValue(predicate, queryGraph.selections.flatPredicatesSet, contextualPropertyAccess))
          IndexCompatiblePredicateWithValueBehavior(predicate, GetValue)
        else IndexCompatiblePredicateWithValueBehavior(predicate, DoNotGetValue)
      }

      propertyPredicates.map { predicate =>
        val rewrittenQueryExpression = predicate.queryExpression.endoRewrite(rewriter)
        predicate.copy(queryExpression = rewrittenQueryExpression) match {
          case predicateWithRewrittenExpr
            if predicateWithRewrittenExpr.predicateExactness.isExact && exactPredicatesCanGetValue =>
            determineGetValueBehaviour(predicateWithRewrittenExpr)
          case predicateWithRewrittenExpr =>
            indexDescriptor.valueCapability match {
              case DoNotGetValue =>
                IndexCompatiblePredicateWithValueBehavior(predicateWithRewrittenExpr, DoNotGetValue)
              case _ =>
                determineGetValueBehaviour(predicateWithRewrittenExpr)
            }
        }
      }
    }

    override def getValueFromIndexBehavior(
      indexEntityVariable: LogicalVariable,
      propertyKey: String,
      queryGraphPredicates: Set[Expression],
      contextualPropertyAccess: ContextualPropertyAccess
    ): GetValueFromIndexBehavior = {
      if (
        propertyOnIndexVariableIsAccessedAfterIndex(
          indexEntityVariable,
          propertyKey,
          queryGraphPredicates,
          None,
          contextualPropertyAccess
        )
      )
        GetValue
      else
        DoNotGetValue
    }

    override def propertiesToFetchBeforeQpp(
      predicates: Iterable[Expression],
      availableSymbols: Set[LogicalVariable],
      semanticTable: SemanticTable
    ): CachedProperties = {
      val propMap: Map[LogicalVariable, Set[PropertyKeyName]] =
        PropertyAccessHelper.findPropertyAccesses(predicates)
          .groupMap(_.variable)(pa => PropertyKeyName(pa.propertyName)(InputPosition.NONE))

      propMap.view
        .filterKeys(availableSymbols)
        .foldLeft(CachedProperties.empty) {
          case (acc, (variable, props)) =>
            val tpe = semanticTable.typeFor(variable)
            if (tpe.is(CTNode))
              acc.add(variable, NODE_TYPE, props)
            else if (tpe.is(CTRelationship))
              acc.add(variable, RELATIONSHIP_TYPE, props)
            else
              acc
        }
    }

    private def externalPropertyAccessesRewriter(context: LogicalPlanningContext) = {
      bottomUp.apply(
        rewriter = Rewriter.lift {
          case property @ Property(logicalVariable: LogicalVariable, propertyKeyName)
            if context.plannerState.previouslyCachedProperties.contains(logicalVariable, propertyKeyName) =>
            val entry = context.plannerState.previouslyCachedProperties.entries(logicalVariable)
            CachedProperty(
              entry.originalEntity,
              logicalVariable,
              propertyKeyName,
              entry.entityType
            )(
              property.position
            )
        },
        cancellation = context.staticComponents.cancellationChecker
      )
    }

    private def accessedPropertiesForPredicates(
      queryGraph: QueryGraph,
      input: LogicalPlan,
      context: LogicalPlanningContext,
      predicatesToIgnore: Set[Expression]
    ): Set[PropertyAccess] = {
      val previouslySolvedPredicates = context.staticComponents
        .planningAttributes.solveds
        .get(input.id)
        .asSinglePlannerQuery.queryGraph
        .selections
        .flatPredicatesSet

      // we compute not only the predicates that will be solved by this selection
      val predicatesToBeSolvedLater =
        queryGraph.selections.flatPredicatesSet
          .diff(previouslySolvedPredicates ++ predicatesToIgnore)
          .filter(expr => expr.dependencies.intersect(input.availableSymbols).nonEmpty)
      PropertyAccessHelper.findPropertyAccesses(predicatesToBeSolvedLater)
    }

    private def cachedPropertiesRewriter(
      inputPlan: LogicalPlan,
      context: LogicalPlanningContext
    ) = {
      val alreadyCachedProperties =
        context.staticComponents.planningAttributes.cachedPropertiesPerPlan.get(inputPlan.id)
      topDown.apply(
        rewriter = Rewriter.lift {
          case property @ Property(logicalVariable: LogicalVariable, propertyKeyName)
            if inputPlan.availableSymbols.contains(logicalVariable) =>
            toCachedProperty(
              context,
              alreadyCachedProperties,
              logicalVariable,
              propertyKeyName,
              property.position
            ).getOrElse(property)
        },
        cancellation = context.staticComponents.cancellationChecker
      )
    }

    private def toCachedProperty(
      context: LogicalPlanningContext,
      alreadyCachedProperties: CachedProperties,
      logicalVariable: LogicalVariable,
      propertyKeyName: PropertyKeyName,
      position: InputPosition,
      knownToCacheStore: Boolean = false
    ): Option[CachedProperty] = {
      alreadyCachedProperties.entries.get(logicalVariable) match {
        case Some(entry) =>
          Some(CachedProperty(
            entry.originalEntity,
            logicalVariable,
            propertyKeyName,
            entry.entityType,
            knownToCacheStore
          )(
            position
          ))
        case None =>
          val entityType = context.semanticTable.typeFor(logicalVariable)
          if (entityType.is(CTNode))
            Some(
              CachedProperty(logicalVariable, logicalVariable, propertyKeyName, NODE_TYPE, knownToCacheStore)(position)
            )
          else if (entityType.is(CTRelationship))
            Some(CachedProperty(
              logicalVariable,
              logicalVariable,
              propertyKeyName,
              RELATIONSHIP_TYPE,
              knownToCacheStore
            )(position))
          else
            None
      }
    }

    private def planBatchProperties(
      input: LogicalPlan,
      context: LogicalPlanningContext,
      accessedProperties: Set[PropertyAccess],
      expressions: Iterable[Expression]
    ): LogicalPlan = {
      val props = propertiesToFetch(input, context, accessedProperties, expressions)
      if (props.nonEmpty)
        context.staticComponents.logicalPlanProducer.planRemoteBatchProperties(input, props, context)
      else input
    }

    protected def propertiesToFetch(
      input: LogicalPlan,
      context: LogicalPlanningContext,
      accessedProperties: Set[PropertyAccess],
      expressions: Iterable[Expression],
      includeVariable: Option[LogicalVariable] = None
    ): Set[CachedProperty] = {
      val accessedPropertiesMap = accessedProperties.map {
        accessedProperty =>
          accessedProperty.variable -> PropertyKeyName(accessedProperty.propertyName)(InputPosition.NONE)
      }.groupMap(_._1)(_._2)

      val alreadyCachedProperties =
        context.staticComponents.planningAttributes.cachedPropertiesPerPlan.get(input.id)
          .union(context.plannerState.previouslyCachedProperties)

      val propsForUsedVars = expressions.folder.treeFold(Set[CachedProperty]()) {
        case _: NestedPlanExpression => set => SkipChildren(set)
        case cachedProperty: CachedProperty
          if !alreadyCachedProperties.contains(cachedProperty.entityVariable, cachedProperty.propertyKey) =>
          set =>
            val props = accessedPropertiesMap
              .getOrElse(cachedProperty.entityVariable, Set.empty)
              .incl(cachedProperty.propertyKey)
            val newProps = alreadyCachedProperties.propertiesNotYetCached(cachedProperty.entityVariable, props)
              .map { propertyKeyName =>
                cachedProperty.copy(propertyKey = propertyKeyName, knownToAccessStore = true)(InputPosition.NONE)
              }
            SkipChildren(set ++ newProps)
      }

      val propsForIncludedVar = includeVariable.map(variable =>
        alreadyCachedProperties.propertiesNotYetCached(
          variable,
          accessedPropertiesMap.getOrElse(variable, Set.empty)
        ).flatMap(propertyKeyName =>
          toCachedProperty(
            context,
            alreadyCachedProperties,
            variable,
            propertyKeyName,
            variable.position,
            knownToCacheStore = true
          )
        )
      ).getOrElse(Set.empty)

      propsForUsedVars ++ propsForIncludedVar
    }

    private def propertyAccessesToPredicatesMap(predicates: Set[Expression]): PropertyAccessInPredicates = {
      val propertyAccessToPredicatesMap = predicates.flatMap {
        predicate =>
          PropertyAccessHelper
            .findPropertyAccesses(Seq(predicate))
            .map((_, predicate))
      }.groupMap(_._1)(_._2)

      PropertyAccessInPredicates(propertyAccessToPredicatesMap)
    }

    override def planBatchPropertiesForHorizonSelections(
      queryGraph: QueryGraph,
      input: LogicalPlan,
      context: LogicalPlanningContext,
      predicatesToSolve: Set[Expression],
      interestingOrderConfig: InterestingOrderConfig
    ): RemoteBatchingSubQueryResult = {
      val predicatesToSolveMap = withNoRewrittenExprs(predicatesToSolve)
      context.settings.shardOperatorPushdownStrategy.horizonSelections(
        input,
        queryGraph,
        context,
        interestingOrderConfig,
        predicatesToSolveMap
      ) match {
        case Some(SelectionCandidate(plan, solvedPredicates)) =>
          val unsolvedPredicates = predicatesToSolveMap.backingStore.filterNot(rewrittenPredTuple =>
            solvedPredicates.contains(rewrittenPredTuple._2)
          )
          val RemoteBatchingSubQueryResult(rewrittenExprs, planWithProps) = planRemoteBatchPropertiesForSelections(
            queryGraph,
            plan,
            context,
            RewrittenSubQueryPredicates.forMap(unsolvedPredicates)
          )
          RemoteBatchingSubQueryResult(
            rewrittenExprs,
            planWithProps
          )

        case _ => planRemoteBatchPropertiesForSelections(queryGraph, input, context, predicatesToSolveMap)
      }
    }

    private case class PropertyAccessInPredicates(backingMap: Map[PropertyAccess, Set[Expression]]) extends AnyVal {

      def containsOtherPredicateThanExcludedOne(
        propertyAccess: PropertyAccess,
        excludedPredicate: Option[Expression]
      ): Boolean =
        excludedPredicate match {
          case Some(excludedExpr) => backingMap.get(propertyAccess).exists(_.exists(expr => expr != excludedExpr))
          case None               => backingMap.contains(propertyAccess)
        }

    }

    override def findGlobalPropertyAccessesWithContext(
      query: SinglePlannerQuery
    ): ContextualPropertyAccess = {
      // When `q` contains something that invalidates the cached properties,
      // then we do not want to collect properties beyond the current query graph.
      def containsMutatingPatternThatInvalidatesCachesProps(q: SinglePlannerQuery): Boolean = {
        q.queryGraph.mutatingPatterns.exists {
          case m if m.invalidatesCachedProperties => true
          case _                                  => false
        }
      }

      @tailrec
      def rec(currentQuery: SinglePlannerQuery, acc: ContextualPropertyAccess): ContextualPropertyAccess = {
        val collectOnlyQgProps = containsMutatingPatternThatInvalidatesCachesProps(currentQuery)
        val accumulatedPropertyAccesses = ContextualPropertyAccess(
          queryGraph = acc.queryGraph ++ PropertyAccessHelper.findPropertyAccesses(Seq(currentQuery.queryGraph)),
          queryGraphMutatingPatterns = acc.queryGraphMutatingPatterns ++
            PropertyAccessHelper.findPropertyAccesses(
              currentQuery.queryGraph.mutatingPatterns.flatMap(_.getExpressionsWithPossiblePropertyReferences)
            ),
          horizon =
            if (collectOnlyQgProps)
              acc.horizon
            else
              acc.horizon ++ PropertyAccessHelper.findPropertyAccesses(Seq(currentQuery.horizon)),
          interestingOrder =
            if (collectOnlyQgProps)
              acc.interestingOrder
            else
              acc.interestingOrder ++ PropertyAccessHelper.findPropertyAccesses(Seq(currentQuery.interestingOrder)),
          entityAliases = findEntityAliases(acc.entityAliases, currentQuery.horizon)
        )
        currentQuery.tail match {
          case Some(tailQuery) =>
            if (collectOnlyQgProps)
              accumulatedPropertyAccesses
            else
              rec(tailQuery, accumulatedPropertyAccesses)
          case None => accumulatedPropertyAccesses
        }
      }

      rec(query, ContextualPropertyAccess.empty)
    }

    private def findEntityAliases(
      renamedVariablesFromPrevHorizons: EntityAliases,
      horizon: QueryHorizon
    ): EntityAliases = {
      val renamedVariablesForCurrentHorizon = horizon match {
        case rqp: RegularQueryProjection => EntityAliases(rqp.projections.collect {
            case (newVar: LogicalVariable, oldVar: LogicalVariable) if newVar != oldVar =>
              newVar -> (renamedVariablesFromPrevHorizons.getOriginalVariables(oldVar) + oldVar)
          })

        case CallSubqueryHorizon(subqueryPlannerQuery, _, _, _, _, importedVariables, _)
          if importedVariables.nonEmpty =>
          findEntityAliasesInSubquery(renamedVariablesFromPrevHorizons, subqueryPlannerQuery)

        case _ => EntityAliases.empty
      }
      renamedVariablesFromPrevHorizons ++ renamedVariablesForCurrentHorizon
    }

    private def findEntityAliasesInSubquery(
      renamedVariablesFromPrevHorizons: EntityAliases,
      query: PlannerQuery
    ): EntityAliases = query match {
      case singlePlannerQuery: SinglePlannerQuery =>
        val currentHorizonfindEntityAliases =
          findEntityAliases(renamedVariablesFromPrevHorizons, singlePlannerQuery.horizon)
        singlePlannerQuery.tail match {
          case Some(tail) => findEntityAliasesInSubquery(currentHorizonfindEntityAliases, tail)
          case None       => currentHorizonfindEntityAliases
        }
      case unionQuery: UnionQuery =>
        val renameOnRHS = findEntityAliasesInSubquery(renamedVariablesFromPrevHorizons, unionQuery.rhs)
        val renameOnLHS = findEntityAliasesInSubquery(renamedVariablesFromPrevHorizons, unionQuery.lhs)
        val renamesOnUnionMappings =
          unionQuery.unionMappings.map { case UnionMapping(unionVariable, variableInLhs, variableInRhs) =>
            val lhsOriginals = renameOnLHS.getOriginalVariables(variableInLhs)
            val rhsOriginals = renameOnRHS.getOriginalVariables(variableInRhs)
            // We will only propagate the renaming of the union variable that which had the same original source variable on both sides.
            // CachedProperties will require knowlege of the source variable. If the source is different on either branch, we cannot create a single cached property out of the union variable.
            // For example if UnionMapping is (u, a1, b1)
            // where a was previously renamed from a1 and b was previously renamed from b1, we will not propagate the renaming of u.
            // However if UnionMapping is (u, a1, a2)
            // where both a1 and a2 was previously renamed from a, we will propagate the renaming of u.
            // lhsOrginals and rhsOriginals are the chain of renames, and the first value in the ListSet is the original variable.
            if (lhsOriginals.nonEmpty && lhsOriginals.originalDefinition == rhsOriginals.originalDefinition)
              unionVariable -> lhsOriginals.commonRenames(rhsOriginals)
            else unionVariable -> RenameChain.empty
          }.filter(_._2.nonEmpty).toMap
        renameOnLHS ++ renameOnLHS ++ EntityAliases(renamesOnUnionMappings)
    }

    override def usePreviouslyCachedProperty(
      inputPlan: LogicalPlan,
      context: LogicalPlanningContext
    ): LogicalPlan = {
      inputPlan.endoRewrite(propertyRewriterBasedOnPreviouslyCachedProperty(context))
    }
  }

  private case object SkipRemoteBatching extends RemoteBatchingStrategy {

    override def getValueFromIndexBehaviors(
      indexDescriptor: IndexDescriptor,
      propertyPredicates: Seq[IndexCompatiblePredicate],
      exactPredicatesCanGetValue: Boolean,
      context: LogicalPlanningContext,
      queryGraph: QueryGraph
    ): Seq[IndexCompatiblePredicateWithValueBehavior] = propertyPredicates.map {
      case predicate if predicate.predicateExactness.isExact && exactPredicatesCanGetValue =>
        IndexCompatiblePredicateWithValueBehavior(predicate, CanGetValue)
      case predicate => IndexCompatiblePredicateWithValueBehavior(predicate, indexDescriptor.valueCapability)
    }

    override def getValueFromIndexBehavior(
      indexEntityVariable: LogicalVariable,
      propertyKey: String,
      queryGraphPredicates: Set[Expression],
      contextualPropertyAccess: ContextualPropertyAccess
    ): GetValueFromIndexBehavior = CanGetValue

    override def planBatchPropertiesForSelections(
      queryGraph: QueryGraph,
      input: LogicalPlan,
      context: LogicalPlanningContext,
      predicatesToSolve: RewrittenSubQueryPredicatesMap
    ): RemoteBatchingSubQueryResult =
      RemoteBatchingSubQueryResult(predicatesToSolve, input)

    override def planRemoteBatchPropertiesForMutatingPattern[
      T <: MutatingPattern with HasMappableExpressions[T]
    ](
      source: LogicalPlan,
      context: LogicalPlanningContext,
      pattern: T
    ): (LogicalPlan, T) = (source, pattern)

    override def planBatchPropertiesForExpressionsWithLookahead(
      queryGraph: QueryGraph,
      input: LogicalPlan,
      context: LogicalPlanningContext,
      expressions: Iterable[Expression]
    ): RemoteBatchingResult = RemoteBatchingResult(RewrittenExpressions.withNoRewrittenExprs(expressions), input)

    override def planRemoteBatchProperties(
      inputPlan: LogicalPlan,
      context: LogicalPlanningContext,
      expressions: Iterable[Expression]
    ): RemoteBatchingResult =
      RemoteBatchingResult(RewrittenExpressions.withNoRewrittenExprs(expressions), inputPlan)

    override def interestingPropertiesAsIDPExtraRequirement(
      queryGraph: QueryGraph,
      context: LogicalPlanningContext
    ): ExtraRequirement[LogicalPlan] = ExtraRequirement.empty

    override def planPrefetchRemoteBatchPropertiesIfRequired(
      queryGraph: QueryGraph,
      plans: Iterable[LogicalPlan],
      context: LogicalPlanningContext
    ): Iterable[LogicalPlan] = Iterable.empty

    override def planRemotePropertiesBeforeCall(
      query: SinglePlannerQuery,
      plan: LogicalPlan,
      context: LogicalPlanningContext
    ): LogicalPlan = plan

    override def planRemotePropertiesBeforeApplyOptional(
      optionalQg: QueryGraph,
      lhsPlan: LogicalPlan,
      context: LogicalPlanningContext
    ): LogicalPlan = lhsPlan

    override def planBatchPropertiesForHorizonSelections(
      queryGraph: QueryGraph,
      input: LogicalPlan,
      context: LogicalPlanningContext,
      predicatesToSolve: Set[Expression],
      interestingOrderConfig: InterestingOrderConfig
    ): RemoteBatchingSubQueryResult =
      RemoteBatchingSubQueryResult(withNoRewrittenExprs(predicatesToSolve), input)

    override def propertiesToFetchBeforeQpp(
      predicates: Iterable[Expression],
      availableSymbols: Set[LogicalVariable],
      semanticTable: SemanticTable
    ): CachedProperties =
      CachedProperties.empty

    override def findGlobalPropertyAccessesWithContext(
      query: SinglePlannerQuery
    ): ContextualPropertyAccess = {
      ContextualPropertyAccess.empty
    }

    override def usePreviouslyCachedProperty(
      inputPlan: LogicalPlan,
      context: LogicalPlanningContext
    ): LogicalPlan = {
      inputPlan
    }
  }
}
