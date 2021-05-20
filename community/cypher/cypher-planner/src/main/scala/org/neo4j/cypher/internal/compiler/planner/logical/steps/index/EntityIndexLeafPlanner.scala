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

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanner
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
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.IndexCompatiblePredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.MultipleExactPredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.NonSeekablePredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.NotExactPredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.SingleExactPredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.variable
import org.neo4j.cypher.internal.expressions.Contains
import org.neo4j.cypher.internal.expressions.EndsWith
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.PartialPredicate
import org.neo4j.cypher.internal.expressions.PartialPredicate.PartialDistanceSeekWrapper
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.helpers.SeqCombiner
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.CompositeQueryExpression
import org.neo4j.cypher.internal.logical.plans.ExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.GetValueFromIndexBehavior
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CypherType

abstract class EntityIndexLeafPlanner extends LeafPlanner {

  private[index] def findIndexCompatiblePredicates(
                                                    predicates: Set[Expression],
                                                    argumentIds: Set[String],
                                                    context: LogicalPlanningContext,
                                                  ): Set[IndexCompatiblePredicate] = {
    val arguments: Set[LogicalVariable] = argumentIds.map(variable)

    def valid(ident: LogicalVariable, dependencies: Set[LogicalVariable]): Boolean =
      !arguments.contains(ident) && dependencies.subsetOf(arguments)

    val explicitCompatiblePredicates = predicates.flatMap {
      // n.prop IN [ ... ]
      case predicate@AsPropertySeekable(seekable: PropertySeekable) if valid(seekable.ident, seekable.dependencies) =>
        val queryExpression = seekable.args.asQueryExpression
        val exactness = if (queryExpression.isInstanceOf[SingleQueryExpression[_]]) SingleExactPredicate else MultipleExactPredicate
        Set(IndexCompatiblePredicate(seekable.ident, seekable.expr, predicate, queryExpression, seekable.propertyValueType(context.semanticTable),
          predicateExactness = exactness, solvedPredicate = Some(predicate), dependencies = seekable.dependencies))

      // ... = n.prop
      // In some rare cases, we can't rewrite these predicates cleanly,
      // and so planning needs to search for these cases explicitly
      case predicate@Equals(a, prop@Property(variable: LogicalVariable, _)) if valid(variable, a.dependencies) =>
        val expr = SingleQueryExpression(a)
        Set(IndexCompatiblePredicate(variable, prop, predicate, expr, Seekable.cypherTypeForTypeSpec(context.semanticTable.getActualTypeFor(prop)),
          predicateExactness = SingleExactPredicate, solvedPredicate = Some(predicate), dependencies = a.dependencies))

      // n.prop STARTS WITH "prefix%..."
      case predicate@AsStringRangeSeekable(seekable) if valid(seekable.ident, seekable.dependencies) =>
        val queryExpression = seekable.asQueryExpression
        Set(IndexCompatiblePredicate(seekable.ident, seekable.property, predicate, queryExpression, seekable.propertyValueType(context.semanticTable),
          predicateExactness = NotExactPredicate,
          solvedPredicate = Some(predicate), dependencies = seekable.dependencies))

      // n.prop <|<=|>|>= value
      case predicate@AsValueRangeSeekable(seekable) if valid(seekable.ident, seekable.dependencies) =>
        val queryExpression = seekable.asQueryExpression
        Set(IndexCompatiblePredicate(seekable.ident, seekable.property, predicate, queryExpression, seekable.propertyValueType(context.semanticTable),
          predicateExactness = NotExactPredicate, solvedPredicate = Some(predicate), dependencies = seekable.dependencies))

      // An index seek for this will almost satisfy the predicate, but with the possibility of some false positives.
      // Since it reduces the cardinality to almost the level of the predicate, we can use the predicate to calculate cardinality,
      // but not mark it as solved, since the planner will still need to solve it with a Filter.
      case predicate@AsDistanceSeekable(seekable) if valid(seekable.ident, seekable.dependencies) =>
        val queryExpression = seekable.asQueryExpression
        Set(IndexCompatiblePredicate(seekable.ident, seekable.property, predicate, queryExpression, seekable.propertyValueType(context.semanticTable),
          predicateExactness = NotExactPredicate, solvedPredicate = Some(PartialDistanceSeekWrapper(predicate)), dependencies = seekable.dependencies))

      // MATCH (n:User) WHERE exists(n.prop) RETURN n
      case predicate@AsPropertyScannable(scannable) if valid(scannable.ident, Set.empty) =>
        Set(IndexCompatiblePredicate(scannable.ident, scannable.property, predicate, ExistenceQueryExpression(), CTAny, predicateExactness = NotExactPredicate,
          solvedPredicate = Some(predicate), dependencies = Set.empty).convertToScannable)

      // n.prop ENDS WITH 'substring'
      case predicate@EndsWith(prop@Property(variable: Variable, _), expr) if valid(variable, expr.dependencies) =>
        Set(IndexCompatiblePredicate(variable, prop, predicate, ExistenceQueryExpression(), CTAny, predicateExactness = NonSeekablePredicate,
          solvedPredicate = Some(predicate), dependencies = expr.dependencies))

      // n.prop CONTAINS 'substring'
      case predicate@Contains(prop@Property(variable: Variable, _), expr) if valid(variable, expr.dependencies) =>
        Set(IndexCompatiblePredicate(variable, prop, predicate, ExistenceQueryExpression(), CTAny, predicateExactness = NonSeekablePredicate,
          solvedPredicate = Some(predicate), dependencies = expr.dependencies))

      case _ =>
        Set.empty[IndexCompatiblePredicate]
    }

    val implicitCompatiblePredicates = implicitIndexCompatiblePredicates(context, predicates, explicitCompatiblePredicates, valid)
    explicitCompatiblePredicates ++ implicitCompatiblePredicates
  }

  /**
   * Find any implicit index compatible predicates.
   *
   * @param predicates                   the predicates in the query
   * @param explicitCompatiblePredicates the explicit index compatible predicates that were extracted from predicates
   * @param valid                        a test that can be applied to check if an implicit predicate is valid
   *                                     based on its variable and dependencies as arguments to the lambda function.
   */
  protected def implicitIndexCompatiblePredicates(context: LogicalPlanningContext,
                                                  predicates: Set[Expression],
                                                  explicitCompatiblePredicates: Set[IndexCompatiblePredicate],
                                                  valid: (LogicalVariable, Set[LogicalVariable]) => Boolean): Set[IndexCompatiblePredicate]
}

object EntityIndexLeafPlanner {

  /**
   * Creates IS NOT NULL-predicates of the given variable to the given properties that are inferred from the context rather than read from the query.
   */
  private[index] def implicitIsNotNullPredicates(variable: Variable,
                                                 context: LogicalPlanningContext,
                                                 constrainedPropNames: Set[String],
                                                 explicitCompatiblePredicates: Set[IndexCompatiblePredicate]): Set[IndexCompatiblePredicate] = {
    // Can't currently handle aggregation on more than one variable
    val aggregatedPropNames: Set[String] =
      if (context.aggregatingProperties.forall(prop => prop.variableName.equals(variable.name))) {
        context.aggregatingProperties.map { prop => prop.propertyName }
      } else {
        Set.empty
      }

    // Can't currently handle aggregation on more than one property
    val propNames = if (aggregatedPropNames.size == 1) constrainedPropNames.union(aggregatedPropNames) else constrainedPropNames

    for {
      propertyName <- propNames
      property = Property(variable, PropertyKeyName(propertyName)(variable.position))(variable.position)
      predicate = IsNotNull(property)(variable.position)
      // Don't add implicit predicates if we already have them explicitly
      if !explicitCompatiblePredicates.exists(_.predicate == predicate)
    } yield IndexCompatiblePredicate(
      variable,
      property,
      predicate,
      ExistenceQueryExpression(),
      CTAny,
      predicateExactness = NotExactPredicate,
      solvedPredicate = None,
      dependencies = Set.empty,
      isImplicit = true
    )
  }

  /**
   * Find and group all predicates, where one PredicatesForIndex contains one predicate for each indexed property, in the right order.
   */
  private[index] def predicatesForIndex(indexDescriptor: IndexDescriptor,
                                        predicates: Set[IndexCompatiblePredicate],
                                        interestingOrderConfig: InterestingOrderConfig,
                                        semanticTable: SemanticTable): Set[PredicatesForIndex] = {

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
    val matchingPredicateCombinations = SeqCombiner.combine(predicatesByIndexedProperty).toSet

    matchingPredicateCombinations
      .map(matchingPredicates => matchPredicateWithIndexDescriptorAndInterestingOrder(matchingPredicates, indexDescriptor, interestingOrderConfig))
  }

  private def matchPredicateWithIndexDescriptorAndInterestingOrder(matchingPredicates: Seq[IndexCompatiblePredicate],
                                                                   indexDescriptor: IndexDescriptor,
                                                                   interestingOrderConfig: InterestingOrderConfig): PredicatesForIndex = {
    val types = matchingPredicates.map(mp => mp.propertyType)

    // Ask the index for its order capabilities for the types in prefix/subset defined by the interesting order
    val indexPropertiesAndPredicateTypes = matchingPredicates.map(mp => {
      val property = Property(mp.variable, mp.propertyKeyName)(mp.property.position)
      PropertyAndPredicateType(property, mp.predicateExactness == SingleExactPredicate)
    })

    val (providedOrder, indexOrder) =
      ResultOrdering.providedOrderForIndexOperator(interestingOrderConfig.orderToSolve, indexPropertiesAndPredicateTypes, types, indexDescriptor.orderCapability)

    PredicatesForIndex(matchingPredicates, providedOrder, indexOrder)
  }

  private[index] def variable(name: String): Variable = Variable(name)(InputPosition.NONE)

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
   * @param isImplicit         if `true` than the predicate is not explicitly stated inthe queryy
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
                                       isImplicit: Boolean = false,
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
          val isNotNull = IsNotNull(property)(predicate.position)
          PartialPredicate(isNotNull, expr)
      }
    }

    private def unwrapPartial(expr: Expression) = expr match {
      case pp: PartialPredicate[_] => pp.coveringPredicate
      case e                       => e
    }

  }

  private[index] def getValueBehaviors(indexDescriptor: IndexDescriptor,
                                       propertyPredicates: Seq[IndexCompatiblePredicate],
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

  case class PredicatesForIndex(predicatesInOrder: Seq[IndexCompatiblePredicate], providedOrder: ProvidedOrder, indexOrder: IndexOrder)

  sealed abstract class PredicateExactness(val isExact: Boolean, val isSeekable: Boolean)
  case object SingleExactPredicate extends PredicateExactness(true, true)
  case object MultipleExactPredicate extends PredicateExactness(true, true)
  case object NotExactPredicate extends PredicateExactness(false, true)
  case object NonSeekablePredicate extends PredicateExactness(false, false)
}
