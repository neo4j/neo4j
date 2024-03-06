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
package org.neo4j.cypher.internal.compiler.planner.logical.steps.index

import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.ast.IsTyped
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingAnyIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingPointIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingRangeIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingTextIndexType
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper.PropertyAccess
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrdering
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrdering.PropertyAndPredicateType
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsExplicitlyPropertyScannable
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.IndexCompatiblePredicate
import org.neo4j.cypher.internal.expressions.EntityType
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.PartialPredicate
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.SymbolicName
import org.neo4j.cypher.internal.frontend.helpers.SeqCombiner
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.CompositeQueryExpression
import org.neo4j.cypher.internal.logical.plans.ExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.GetValueFromIndexBehavior
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrderFactory
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.IndexType
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTPoint
import org.neo4j.cypher.internal.util.symbols.CTPointNotNull
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CTStringNotNull
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.internal.schema.IndexQuery.IndexQueryType
import org.neo4j.internal.schema.constraints.SchemaValueType

/**
 * Common functionality of NodeIndexLeafPlanner and RelationshipIndexLeafPlanner.
 */
object EntityIndexLeafPlanner {

  /**
   * Creates IS NOT NULL-predicates of the given variable to the given properties that are inferred from the context rather than read from the query.
   */
  private[index] def implicitIsNotNullPredicates(
    variable: LogicalVariable,
    aggregatingProperties: Set[PropertyAccess],
    constrainedPropNames: Set[String],
    explicitCompatiblePredicates: Set[IndexCompatiblePredicate]
  ): Set[IndexCompatiblePredicate] = {
    // Can't currently handle aggregation on more than one variable
    val aggregatedPropNames: Set[String] =
      if (aggregatingProperties.forall(prop => prop.variable.equals(variable))) {
        aggregatingProperties.map { prop => prop.propertyName }
      } else {
        Set.empty
      }

    //  Can't currently handle aggregation on more than one property
    val propNames =
      if (aggregatedPropNames.size == 1) constrainedPropNames.union(aggregatedPropNames) else constrainedPropNames

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
      predicateExactness = NotExactPredicate,
      solvedPredicate = None,
      dependencies = Set.empty,
      isImplicit = true,
      indexRequirements = Set(IndexRequirement.SupportsIndexQuery(IndexQueryType.EXISTS)),
      cypherType = CTAny
    )
  }

  /**
   * If a type constraint exists for the property of a predicate, we may sometimes also consider a scannable version of the predicate.
   * For example, if a string type constraint exists on n.prop, then any scannable predicate on n.prop is solvable by a text index.
   */
  private def resolveConstraintTypes(
    predicate: IndexCompatiblePredicate,
    typeConstraints: Map[String, Seq[SchemaValueType]]
  ): Seq[IndexCompatiblePredicate] = {
    typeConstraints.get(predicate.propertyKeyName.name) match {
      case Some(Seq(SchemaValueType.STRING)) =>
        Seq(
          predicate,
          predicate.copy(cypherType = CTString),
          predicate.convertToTextScannable.copy(cypherType = CTString)
        )
      case Some(Seq(SchemaValueType.POINT)) =>
        Seq(
          predicate,
          predicate.copy(cypherType = CTPoint),
          predicate.convertToPointScannable.copy(cypherType = CTPoint)
        )
      case _ => Seq(predicate)
    }
  }

  /**
   * Find and group all predicates, where one PredicatesForIndex contains one predicate for each indexed property, in the right order.
   */
  private[index] def predicatesForIndex(
    indexDescriptor: IndexDescriptor,
    predicates: Set[IndexCompatiblePredicate],
    interestingOrderConfig: InterestingOrderConfig,
    semanticTable: SemanticTable,
    typeConstraints: Map[String, Seq[SchemaValueType]],
    providedOrderFactory: ProvidedOrderFactory
  ): Set[PredicatesForIndex] = {

    // Group predicates by which property they include
    val predicatesByProperty = predicates
      .flatMap(resolveConstraintTypes(_, typeConstraints))
      .filter(predicate => predicate.indexRequirements.forall(req => req.satisfiedBy(indexDescriptor, predicate)))
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
      .map(matchingPredicates =>
        matchPredicateWithIndexDescriptorAndInterestingOrder(
          matchingPredicates,
          indexDescriptor,
          interestingOrderConfig,
          providedOrderFactory
        )
      )
  }

  private def matchPredicateWithIndexDescriptorAndInterestingOrder(
    matchingPredicates: Seq[IndexCompatiblePredicate],
    indexDescriptor: IndexDescriptor,
    interestingOrderConfig: InterestingOrderConfig,
    providedOrderFactory: ProvidedOrderFactory
  ): PredicatesForIndex = {

    // Ask the index for its order capability
    val indexPropertiesAndPredicateTypes = matchingPredicates.map(mp => {
      val property = Property(mp.variable, mp.propertyKeyName)(mp.property.position)
      PropertyAndPredicateType(property, mp.predicateExactness == SingleExactPredicate)
    })

    val (providedOrder, indexOrder) =
      ResultOrdering.providedOrderForIndexOperator(
        interestingOrderConfig.orderToSolve,
        indexPropertiesAndPredicateTypes,
        indexDescriptor.orderCapability,
        providedOrderFactory
      )

    PredicatesForIndex(matchingPredicates, providedOrder, indexOrder)
  }

  /**
   * A predicate that could potentially be solved by a property index
   *
   * @param variable             The variable to solve for
   * @param property             The property involved in the predicate
   * @param predicate            The original predicate from the query
   * @param queryExpression      The index query expression
   * @param predicateExactness   Determines seek possibility
   * @param solvedPredicate      If a plan is created, this is what to register as solved predicate
   * @param dependencies         Predicate dependencies
   * @param isImplicit           if `true` than the predicate is not explicitly stated in the query
   * @param indexRequirements    Requirements an index must satisfy to be considered a match for this predicate
   * @param cypherType           the type of the property
   */
  case class IndexCompatiblePredicate(
    variable: LogicalVariable,
    property: LogicalProperty,
    predicate: Expression,
    queryExpression: QueryExpression[Expression],
    predicateExactness: PredicateExactness,
    solvedPredicate: Option[Expression],
    dependencies: Set[LogicalVariable],
    isImplicit: Boolean = false,
    indexRequirements: Set[IndexRequirement],
    cypherType: CypherType
  ) {
    def propertyKeyName: PropertyKeyName = property.propertyKey

    def isExists: Boolean = queryExpression match {
      case _: ExistenceQueryExpression[Expression] => true
      case _                                       => false
    }

    def convertToRangeScannable: IndexCompatiblePredicate = queryExpression match {
      case _: CompositeQueryExpression[Expression] =>
        throw new IllegalStateException("A CompositeQueryExpression can't be nested in a CompositeQueryExpression")

      case _ => copy(
          queryExpression = ExistenceQueryExpression(),
          predicateExactness = NotExactPredicate,
          solvedPredicate = solvedPredicate.map(convertToRangeScannablePredicate),
          indexRequirements = Set(IndexRequirement.SupportsIndexQuery(IndexQueryType.EXISTS)),
          cypherType = CTAny
        )
    }

    private def convertToRangeScannablePredicate(expr: Expression): Expression = {
      val original = unwrapPartial(expr)
      original match {
        case AsExplicitlyPropertyScannable(scannable) =>
          scannable.expr
        case expr =>
          val isNotNull = IsNotNull(property)(predicate.position)
          PartialPredicate(isNotNull, expr)
      }
    }

    def convertToTextScannable: IndexCompatiblePredicate = {
      copy(
        queryExpression = ExistenceQueryExpression(),
        predicateExactness = NotExactPredicate,
        solvedPredicate = solvedPredicate.map(convertToTextScannablePredicate),
        indexRequirements = Set(
          IndexRequirement.SupportsIndexQuery(IndexQueryType.ALL_ENTRIES),
          IndexRequirement.HasType(IndexType.Text)
        )
      )
    }

    private def convertToTextScannablePredicate(expr: Expression): Expression = {
      unwrapPartial(expr) match {
        case AsExplicitlyPropertyScannable(scannable) if cypherType == CTString => scannable.expr
        case expr => PartialPredicate(IsTyped(property, CTStringNotNull)(predicate.position), expr)
      }
    }

    def convertToPointScannable: IndexCompatiblePredicate = {
      copy(
        queryExpression = ExistenceQueryExpression(),
        predicateExactness = NotExactPredicate,
        solvedPredicate = solvedPredicate.map(convertToPointScannablePredicate),
        indexRequirements = Set(
          IndexRequirement.SupportsIndexQuery(IndexQueryType.ALL_ENTRIES),
          IndexRequirement.HasType(IndexType.Point)
        )
      )
    }

    private def convertToPointScannablePredicate(expr: Expression): Expression = {
      unwrapPartial(expr) match {
        case AsExplicitlyPropertyScannable(scannable) if cypherType == CTPoint => scannable.expr
        case expr => PartialPredicate(IsTyped(property, CTPointNotNull)(predicate.position), expr)
      }
    }

    private def unwrapPartial(expr: Expression) = expr match {
      case pp: PartialPredicate[_] => pp.coveringPredicate
      case e                       => e
    }
  }

  sealed trait IndexRequirement extends Product {
    def satisfiedBy(indexDescriptor: IndexDescriptor, predicate: IndexCompatiblePredicate): Boolean
  }

  object IndexRequirement {

    final case class SupportsIndexQuery(indexQueryType: IndexQueryType) extends IndexRequirement {

      override def satisfiedBy(indexDescriptor: IndexDescriptor, predicate: IndexCompatiblePredicate): Boolean = {
        indexDescriptor.isQuerySupported(indexQueryType, predicate.cypherType)
      }
    }

    final case class HasType(indexType: IndexDescriptor.IndexType) extends IndexRequirement {

      override def satisfiedBy(indexDescriptor: IndexDescriptor, predicate: IndexCompatiblePredicate): Boolean = {
        indexDescriptor.indexType == indexType
      }
    }
  }

  private[index] def getValueBehaviors(
    indexDescriptor: IndexDescriptor,
    propertyPredicates: Seq[IndexCompatiblePredicate],
    exactPredicatesCanGetValue: Boolean
  ): Seq[GetValueFromIndexBehavior] = {
    val propertyBehaviorFromIndex = indexDescriptor.valueCapability

    propertyPredicates.map {
      case predicate if predicate.predicateExactness.isExact && exactPredicatesCanGetValue => CanGetValue
      case _                                                                               => propertyBehaviorFromIndex
    }
  }

  case class PredicatesForIndex(
    predicatesInOrder: Seq[IndexCompatiblePredicate],
    providedOrder: ProvidedOrder,
    indexOrder: IndexOrder
  )

  sealed abstract class PredicateExactness(val isExact: Boolean, val isSeekable: Boolean)
  case object SingleExactPredicate extends PredicateExactness(true, true)
  case object MultipleExactPredicate extends PredicateExactness(true, true)
  case object NotExactPredicate extends PredicateExactness(false, true)
  case object NonSeekablePredicate extends PredicateExactness(false, false)
}

/**
 * Represents a possible use of an index which can be used to solve a given sequence of predicates.
 *
 * This trait is to provide a unified interface of the two index match types for node indexes and relationship indexes.
 */
trait IndexMatch {
  def propertyPredicates: Seq[IndexCompatiblePredicate]
  def indexDescriptor: IndexDescriptor
  def predicateSet(newPredicates: Seq[IndexCompatiblePredicate], exactPredicatesCanGetValue: Boolean): PredicateSet
  def variable: LogicalVariable
}

/**
 * Information needed to create a index leaf plan.
 */
trait PredicateSet {
  def variable: LogicalVariable
  def symbolicName: SymbolicName
  def propertyPredicates: Seq[IndexCompatiblePredicate]
  def getValueBehaviors: Seq[GetValueFromIndexBehavior]

  def getEntityType: EntityType

  def allSolvedPredicates: Seq[Expression] =
    propertyPredicates.flatMap(_.solvedPredicate)

  def indexedProperties(context: LogicalPlanningContext): Seq[IndexedProperty] =
    propertyPredicates.zip(getValueBehaviors).map {
      case (predicate, behavior) =>
        val propertyName = predicate.propertyKeyName
        val getValue = behavior
        IndexedProperty(
          PropertyKeyToken(propertyName, context.semanticTable.id(propertyName).head),
          getValue,
          getEntityType
        )
    }

  private def matchingHints(hints: Set[Hint]): Set[UsingIndexHint] = {
    val propertyNames = propertyPredicates.map(_.propertyKeyName.name)
    val localVariableName = variable
    val entityTypeName = symbolicName.name
    hints.collect {
      case hint @ UsingIndexHint(
          `localVariableName`,
          LabelOrRelTypeName(`entityTypeName`),
          propertyKeyNames,
          _,
          _
        ) if propertyKeyNames.map(_.name) == propertyNames => hint
    }
  }

  private def fulfilledByIndexType(indexType: IndexType)(hint: UsingIndexHint): Boolean =
    (hint.indexType, indexType) match {
      case (UsingAnyIndexType, _)                 => true
      case (UsingTextIndexType, IndexType.Text)   => true
      case (UsingRangeIndexType, IndexType.Range) => true
      case (UsingPointIndexType, IndexType.Point) => true
      case _                                      => false
    }

  def fulfilledHints(allHints: Set[Hint], indexType: IndexType, planIsScan: Boolean): Set[UsingIndexHint] =
    matchingHints(allHints)
      .filter(fulfilledByIndexType(indexType))
      .filter(!planIsScan || _.spec.fulfilledByScan)
}
