/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.steps.index

import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.IndexLookupUnfulfillableNotification
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanFromExpressions
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanRestrictions
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlansForVariable
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlansForVariable.maybeLeafPlans
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrdering
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrdering.PropertyAndPredicateType
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsDistanceSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsExplicitlyPropertyScannable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsPropertyScannable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsPropertySeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsStringRangeSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsValueRangeSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.PropertySeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.Seekable
import org.neo4j.cypher.internal.compiler.planner.logical.steps.DynamicPropertyNotifier
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexPlanner.IndexCandidateHasLabelsPredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexPlanner.IndexCompatiblePredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexPlanner.IndexMatch
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexPlanner.MultipleExactPredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexPlanner.NotExactPredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexPlanner.PredicatesForIndex
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexPlanner.SingleExactPredicate
import org.neo4j.cypher.internal.expressions.Contains
import org.neo4j.cypher.internal.expressions.EndsWith
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.PartialPredicate
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions
import org.neo4j.cypher.internal.frontend.helpers.SeqCombiner
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.AsDynamicPropertyNonSeekable
import org.neo4j.cypher.internal.logical.plans.AsStringRangeNonSeekable
import org.neo4j.cypher.internal.logical.plans.AsValueRangeNonSeekable
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.CompositeQueryExpression
import org.neo4j.cypher.internal.logical.plans.ExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.GetValueFromIndexBehavior
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CypherType

case class NodeIndexPlanner(planProviders: Seq[NodeIndexPlanProvider], restrictions: LeafPlanRestrictions) extends LeafPlanner with LeafPlanFromExpressions {

  override def apply(qg: QueryGraph, interestingOrderConfig: InterestingOrderConfig, context: LogicalPlanningContext): Seq[LogicalPlan] =
    producePlanFor(qg.selections.flatPredicates.toSet, qg, interestingOrderConfig, context)
      .toSeq.flatMap(_.plans)

  override def producePlanFor(predicates: Set[Expression],
                              qg: QueryGraph,
                              interestingOrderConfig: InterestingOrderConfig,
                              context: LogicalPlanningContext): Set[LeafPlansForVariable] = {
    implicit val allLabelPredicatesMap: Map[String, Set[HasLabels]] = qg.selections.labelPredicates
    implicit val semanticTable: SemanticTable = context.semanticTable

    // Find plans solving given property predicates together with any label predicates from QG
    val resultFromPropertyPredicates = if (allLabelPredicatesMap.isEmpty) Set.empty[LeafPlansForVariable] else {
      val compatiblePropertyPredicates: Set[IndexCompatiblePredicate] = findIndexCompatiblePredicates(predicates, qg.argumentIds, context)
      compatiblePropertyPredicates.map(_.name).flatMap { name =>
        val labelPredicates = allLabelPredicatesMap.getOrElse(name, Set.empty)
        val nodePredicates = compatiblePropertyPredicates.filter(p => p.name == name)
        maybeLeafPlans(name, producePlansForSpecificVariable(name, nodePredicates, labelPredicates, qg.hints, qg.argumentIds, context, interestingOrderConfig))
      }
    }

    // Find plans solving given label predicates together with any property predicates from QG
    val resultFromLabelPredicates = {
      lazy val allIndexCompatibles: Set[IndexCompatiblePredicate] = findIndexCompatiblePredicates(qg.selections.flatPredicates.toSet, qg.argumentIds, context)
      val candidateLabelPredicates = predicates.collect { case p@HasLabels(v: LogicalVariable, _) => IndexCandidateHasLabelsPredicate(v.name, p) }
      candidateLabelPredicates.flatMap { candidate =>
        val name = candidate.name
        val labelPredicates = Set(candidate.hasLabels)
        val nodePredicates = allIndexCompatibles.filter(p => p.name == name)
        maybeLeafPlans(name, producePlansForSpecificVariable(name, nodePredicates, labelPredicates, qg.hints, qg.argumentIds, context, interestingOrderConfig))
      }
    }

    val result = resultFromPropertyPredicates ++ resultFromLabelPredicates

    issueNotifications(result, qg, context)

    result
  }

  private def findIndexCompatiblePredicates(
    predicates: Set[Expression],
    argumentIds: Set[String],
    context: LogicalPlanningContext,
  )(implicit
    labelPredicateMap: Map[String, Set[HasLabels]],
    semanticTable: SemanticTable
  ): Set[IndexCompatiblePredicate] = {

    val arguments: Set[LogicalVariable] =
      argumentIds.map(n => Variable(n)(null))

    def valid(ident: LogicalVariable, dependencies: Set[LogicalVariable]): Boolean =
      !arguments.contains(ident) && dependencies.subsetOf(arguments)

    val compatiblePredicates = predicates.flatMap {
      // n.prop IN [ ... ]
      case predicate@AsPropertySeekable(seekable: PropertySeekable) if valid(seekable.ident, seekable.dependencies) =>
        val queryExpression = seekable.args.asQueryExpression
        val exactness = if (queryExpression.isInstanceOf[SingleQueryExpression[_]]) SingleExactPredicate else MultipleExactPredicate
        Set(IndexCompatiblePredicate(seekable.ident, seekable.expr, predicate, queryExpression, seekable.propertyValueType(semanticTable), predicateExactness = exactness,
          solvedPredicate = Some(predicate), dependencies = seekable.dependencies))

      // ... = n.prop
      // In some rare cases, we can't rewrite these predicates cleanly,
      // and so planning needs to search for these cases explicitly
      case predicate@Equals(a, prop@Property(variable: LogicalVariable, _)) if valid(variable, a.dependencies) =>
        val expr = SingleQueryExpression(a)
        Set(IndexCompatiblePredicate(variable, prop, predicate, expr, Seekable.cypherTypeForTypeSpec(semanticTable.getActualTypeFor(prop)), predicateExactness = SingleExactPredicate,
          solvedPredicate = Some(predicate), dependencies = a.dependencies))

      // n.prop STARTS WITH "prefix%..."
      case predicate@AsStringRangeSeekable(seekable) if valid(seekable.ident, seekable.dependencies) =>
        val queryExpression = seekable.asQueryExpression
        Set(IndexCompatiblePredicate(seekable.ident, seekable.property, predicate, queryExpression, seekable.propertyValueType(semanticTable), predicateExactness = NotExactPredicate,
          solvedPredicate = Some(predicate), dependencies = seekable.dependencies))

      // n.prop <|<=|>|>= value
      case predicate@AsValueRangeSeekable(seekable) if valid(seekable.ident, seekable.dependencies) =>
        val queryExpression = seekable.asQueryExpression
        Set(IndexCompatiblePredicate(seekable.ident, seekable.property, predicate, queryExpression, seekable.propertyValueType(semanticTable), predicateExactness = NotExactPredicate,
          solvedPredicate = Some(predicate), dependencies = seekable.dependencies))

      // An index seek for this will almost satisfy the predicate, but with the possibility of some false positives.
      // Since it reduces the cardinality to almost the level of the predicate, we can use the predicate to calculate cardinality,
      // but not mark it as solved, since the planner will still need to solve it with a Filter.
      case predicate@AsDistanceSeekable(seekable) if valid(seekable.ident, seekable.dependencies) =>
        val queryExpression = seekable.asQueryExpression
        Set(IndexCompatiblePredicate(seekable.ident, seekable.property, predicate, queryExpression, seekable.propertyValueType(semanticTable), predicateExactness = NotExactPredicate,
          solvedPredicate = None, dependencies = seekable.dependencies))

      // MATCH (n:User) WHERE exists(n.prop) RETURN n
      case predicate@AsPropertyScannable(scannable) if valid(scannable.ident, Set.empty)  =>
        Set(IndexCompatiblePredicate(scannable.ident, scannable.property, predicate, ExistenceQueryExpression(), CTAny, predicateExactness = NotExactPredicate,
          solvedPredicate = Some(predicate), dependencies = Set.empty))

      // n.prop ENDS WITH 'substring'
      case predicate@EndsWith(prop@Property(variable: Variable, _), expr) if valid(variable, expr.dependencies) =>
        Set(IndexCompatiblePredicate(variable, prop, predicate, ExistenceQueryExpression(), CTAny, predicateExactness = NotExactPredicate,
          solvedPredicate = Some(predicate), dependencies = expr.dependencies))

      // n.prop CONTAINS 'substring'
      case predicate@Contains(prop@Property(variable: Variable, _), expr) if valid(variable, expr.dependencies) =>
        Set(IndexCompatiblePredicate(variable, prop, predicate, ExistenceQueryExpression(), CTAny, predicateExactness = NotExactPredicate,
          solvedPredicate = Some(predicate), dependencies = expr.dependencies))

      case _ =>
        Set.empty[IndexCompatiblePredicate]

    }

    val implicitCompatiblePredicates = predicates.flatMap {
      // n:User ... aggregation(n.prop)
      // or
      // n:User with CREATE CONSTRAINT ON (n:User) ASSERT EXISTS (n.prop)
      case HasLabels(variable: Variable, labels) if valid(variable, Set.empty) => for {
        (property, predicate) <- implicitExistsPredicates(variable, labels.head, context)
        // Don't add implicit predicates if we already have them explicitly
        if !compatiblePredicates.exists(_.predicate == predicate)
      } yield IndexCompatiblePredicate(variable, property, predicate, ExistenceQueryExpression(), CTAny, predicateExactness = NotExactPredicate,
        solvedPredicate = None, dependencies = Set.empty)

      case _ =>
        Set.empty[IndexCompatiblePredicate]
    }

    compatiblePredicates ++ implicitCompatiblePredicates
  }

  private def implicitExistsPredicates(variable: Variable, label: LabelName, context: LogicalPlanningContext): Set[(Property, Expression)] = {
    val constrainedPropNames = context.planContext.getPropertiesWithExistenceConstraint(label.name)

    // Can't currently handle aggregation on more than one variable
    val aggregatedPropNames: Set[String] =
      if (context.aggregatingProperties.forall(prop => prop._1.equals(variable.name)))
        context.aggregatingProperties.map { prop => prop._2 }
      else Set.empty

    // Can't currently handle aggregation on more than one property
    val propNames = if(aggregatedPropNames.size == 1) constrainedPropNames.union(aggregatedPropNames) else constrainedPropNames

    for {
      propertyName <- propNames
      property = Property(variable, PropertyKeyName(propertyName)(variable.position))(variable.position)
      predicate = FunctionInvocation(FunctionName(functions.Exists.name)(variable.position), property)(variable.position)
    } yield (property, predicate)

  }

  private def producePlansForSpecificVariable(idName: String,
                                              indexCompatiblePredicates: Set[IndexCompatiblePredicate],
                                              labelPredicates: Set[HasLabels],
                                              hints: Set[Hint],
                                              argumentIds: Set[String],
                                              context: LogicalPlanningContext,
                                              interestingOrderConfig: InterestingOrderConfig): Set[LogicalPlan] = {
    implicit val semanticTable: SemanticTable = context.semanticTable
    val indexMatches = findIndexMatches(idName, indexCompatiblePredicates, labelPredicates, interestingOrderConfig, context)
    val plans = for {
      provider <- planProviders
      plan <- provider.createPlans(indexMatches, hints, argumentIds, restrictions, context)
    } yield plan
    plans.toSet
  }

  private def findIndexMatches(
    variableName: String,
    indexCompatiblePredicates: Set[IndexCompatiblePredicate],
    labelPredicates: Set[HasLabels],
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext,
  ): Set[IndexMatch] = for {
    labelPredicate <- labelPredicates
    labelName <- labelPredicate.labels
    labelId: LabelId <- context.semanticTable.id(labelName).toSeq
    indexDescriptor: IndexDescriptor <- findIndexesForLabel(labelId, context)
    predicatesForIndex <- predicatesForIndex(indexDescriptor, indexCompatiblePredicates, interestingOrderConfig)(context.semanticTable)
    indexMatch = IndexMatch(
      variableName,
      labelPredicate,
      labelName,
      labelId,
      predicatesForIndex.predicatesInOrder,
      predicatesForIndex.providedOrder,
      predicatesForIndex.indexOrder,
      indexDescriptor,
    )
  } yield indexMatch

  private def findIndexesForLabel(labelId: Int, context: LogicalPlanningContext): Iterator[IndexDescriptor] =
    context.planContext.indexesGetForLabel(labelId)

  /**
   * Find and group all predicates, where one PredicatesForIndex contains one predicate for each indexed property, in the right order.
   */
  private def predicatesForIndex(indexDescriptor: IndexDescriptor, predicates: Set[IndexCompatiblePredicate], interestingOrderConfig: InterestingOrderConfig)
                                (implicit semanticTable: SemanticTable): Seq[PredicatesForIndex] = {

    // Group predicates by which property they include
    val predicatesByProperty = predicates
      .groupBy(icp => semanticTable.id(icp.propertyKeyName))
      // Sort out predicates that are not found in semantic table
      .collect { case (Some(x), v) => (x, v) }

    // For each indexed property, look up the relevant predicates
    val predicatesByIndexedProperty = indexDescriptor.properties
      .map(indexedProp => predicatesByProperty.getOrElse(indexedProp, Set.empty))

    // All combinations of predicates where each inner Seq covers the indexed properties in the correct order.
    // E.g. for an index on foo, bar and the predicates predFoo1, predFoo2, predBar1, this would return
    // Seq(Seq(predFoo1, predBar1), Seq(predFoo2, predBar1)).
    val matchingPredicateCombinations = SeqCombiner.combine(predicatesByIndexedProperty)

    matchingPredicateCombinations
      .map(matchingPredicates => matchPredicateWithIndexDescriptorAndInterestingOrder(matchingPredicates, indexDescriptor, interestingOrderConfig))
  }

  private def matchPredicateWithIndexDescriptorAndInterestingOrder(matchingPredicates: Seq[IndexCompatiblePredicate],
                                                                   indexDescriptor: IndexDescriptor,
                                                                   interestingOrderConfig: InterestingOrderConfig): PredicatesForIndex = {
    val types = matchingPredicates.map(mp => mp.propertyType)

    // TODO: move to IndexMatch?
    // Ask the index for its order capabilities for the types in prefix/subset defined by the interesting order
    val indexPropertiesAndPredicateTypes = matchingPredicates.map(mp => {
      val property = Property(mp.variable, mp.propertyKeyName)(mp.property.position)
      PropertyAndPredicateType(property, mp.predicateExactness == SingleExactPredicate)
    })

    val (providedOrder, indexOrder) =
      ResultOrdering.providedOrderForIndexOperator(interestingOrderConfig.orderToSolve, indexPropertiesAndPredicateTypes, types, indexDescriptor.orderCapability)

    PredicatesForIndex(matchingPredicates, providedOrder, indexOrder)
  }

  private def issueNotifications(result: Set[LeafPlansForVariable], qg: QueryGraph, context: LogicalPlanningContext): Unit = {
    if (result.isEmpty) {
      val seekableIdentifiers: Set[Variable] = findNonSeekableIdentifiers(qg.selections.flatPredicates, context)
      DynamicPropertyNotifier.process(seekableIdentifiers, IndexLookupUnfulfillableNotification, qg, context)
    }
  }

  private def findNonSeekableIdentifiers(predicates: Seq[Expression], context: LogicalPlanningContext): Set[Variable] =
    predicates.flatMap {
      // n['some' + n.prop] IN [ ... ]
      case AsDynamicPropertyNonSeekable(nonSeekableId)
        if context.semanticTable.isNode(nonSeekableId) => Some(nonSeekableId)

      // n['some' + n.prop] STARTS WITH "prefix%..."
      case AsStringRangeNonSeekable(nonSeekableId)
        if context.semanticTable.isNode(nonSeekableId) => Some(nonSeekableId)

      // n['some' + n.prop] <|<=|>|>= value
      case AsValueRangeNonSeekable(nonSeekableId)
        if context.semanticTable.isNode(nonSeekableId) => Some(nonSeekableId)

      case _ => None
    }.toSet


}

object NodeIndexPlanner {

  /**
   * A predicate that could potentially be solved by a property index
   *
   * @param variable           The variable to solve for
   * @param property           The property involved in the predicate
   * @param predicate          The original predicate from the query
   * @param queryExpression    The index query expression
   * @param propertyType       The cypher type of the property
   * @param predicateExactness Determines seek possibility
   * @param solvedPredicate    If a plan is created, this is what to register as solved predicate
   * @param dependencies       Predicate dependencies
   */
  case class IndexCompatiblePredicate(
    variable: LogicalVariable,
    property: LogicalProperty,
    predicate: Expression,
    queryExpression: QueryExpression[Expression],
    propertyType: CypherType,
    predicateExactness: PredicateExactness,
    solvedPredicate: Option[Expression],
    dependencies: Set[LogicalVariable],
  ) {
    def name: String = variable.name

    def propertyKeyName: PropertyKeyName = property.propertyKey

    def isExists: Boolean = queryExpression match {
      case _: ExistenceQueryExpression[Expression] => true
      case _ => false
    }

    def convertToScannable: IndexCompatiblePredicate = queryExpression match {
      case _: CompositeQueryExpression[Expression] =>
        throw new IllegalStateException("A CompositeQueryExpression can't be nested in a CompositeQueryExpression")

      case _ => copy(
        queryExpression = ExistenceQueryExpression(),
        predicateExactness = NotExactPredicate,
        solvedPredicate = solvedPredicate.map(convertToScannablePredicate),
      )
    }

    private def convertToScannablePredicate(expr: Expression) = {
      val original = unwrapPartial(expr)
      original match {
        case AsExplicitlyPropertyScannable(scannable) =>
          scannable.expr
        case expr =>
          val exists = FunctionInvocation(FunctionName(functions.Exists.name)(predicate.position), property)(predicate.position)
          PartialPredicate(exists, expr)
      }
    }

    private def unwrapPartial(expr: Expression) = expr match {
      case pp: PartialPredicate[_] => pp.coveringPredicate
      case e                       => e
    }

  }

  case class PredicatesForIndex(predicatesInOrder: Seq[IndexCompatiblePredicate], providedOrder: ProvidedOrder, indexOrder: IndexOrder)

  /**
   * A label predicate that is a candidate for being solved by an index
   *
   * @param name      Variable name
   * @param hasLabels Original expression
   */
  case class IndexCandidateHasLabelsPredicate(name: String, hasLabels: HasLabels)

  sealed abstract class PredicateExactness(val isExact: Boolean)
  case object SingleExactPredicate extends PredicateExactness(true)
  case object MultipleExactPredicate extends PredicateExactness(true)
  case object NotExactPredicate extends PredicateExactness(false)

  /**
   * Represents a match between a set of predicates and an existing index
   * that covers the label and all properties in the predicates
   */
  case class IndexMatch(
    variableName: String,
    labelPredicate: HasLabels,
    labelName: LabelName,
    labelId: LabelId,
    propertyPredicates: Seq[IndexCompatiblePredicate],
    providedOrder: ProvidedOrder,
    indexOrder: IndexOrder,
    indexDescriptor: IndexDescriptor,
  ) {

    def labelToken: LabelToken = LabelToken(labelName, labelId)

    def predicateSet(newPredicates: Seq[IndexCompatiblePredicate], exactPredicatesCanGetValue: Boolean): PredicateSet =
      PredicateSet(
        variableName,
        labelPredicate,
        labelName,
        newPredicates,
        getValueBehaviors(newPredicates, exactPredicatesCanGetValue),
      )

    private def getValueBehaviors(propertyPredicates: Seq[IndexCompatiblePredicate],
                                  exactPredicatesCanGetValue: Boolean): Seq[GetValueFromIndexBehavior] = {
      // Ask the index for its value capabilities for the types of all properties.
      // We might override some of these later if they value is known in an equality predicate
      val types = propertyPredicates.map(mp => mp.propertyType)
      val propertyBehaviorFromIndex = indexDescriptor.valueCapability(types)

      propertyBehaviorFromIndex.zip(propertyPredicates).map {
        case (_, predicate)
          if predicate.predicateExactness.isExact && exactPredicatesCanGetValue => CanGetValue
        case (behavior, _)                                                      => behavior
      }
    }
  }

  /**
   * A set of predicates to create an index leaf plan from
   */
  case class PredicateSet(
    variableName: String,
    labelPredicate: HasLabels,
    labelName: LabelName,
    propertyPredicates: Seq[IndexCompatiblePredicate],
    getValueBehaviors: Seq[GetValueFromIndexBehavior],
  ) {
    def allSolvedPredicates: Seq[Expression] =
      propertyPredicates.flatMap(_.solvedPredicate) :+ labelPredicate

    def indexedProperties(context: LogicalPlanningContext): Seq[IndexedProperty] = propertyPredicates.zip(getValueBehaviors).map {
      case (predicate, behavior) =>
        val propertyName = predicate.propertyKeyName
        val getValue = behavior
        IndexedProperty(PropertyKeyToken(propertyName, context.semanticTable.id(propertyName).head), getValue)
    }

    def matchingHints(hints: Set[Hint]): Set[UsingIndexHint] = {
      val propertyNames = propertyPredicates.map(_.propertyKeyName.name)
      hints.collect {
        case hint@UsingIndexHint(Variable(`variableName`), LabelOrRelTypeName(labelName.name), propertyKeyNames, _)
          if propertyKeyNames.map(_.name) == propertyNames => hint
      }
    }

  }
}
