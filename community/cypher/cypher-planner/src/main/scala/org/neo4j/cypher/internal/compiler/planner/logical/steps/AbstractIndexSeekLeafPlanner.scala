/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.IndexLookupUnfulfillableNotification
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlansForVariable.maybeLeafPlans
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrdering
import org.neo4j.cypher.internal.compiler.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.planner.logical.{LeafPlanFromExpressions, LeafPlanner, LeafPlansForVariable, LogicalPlanningContext}
import org.neo4j.cypher.internal.ir.{InterestingOrder, ProvidedOrder, QueryGraph}
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util.LabelId
import org.neo4j.cypher.internal.v4_0.util.symbols.{CTAny, CypherType}

abstract class AbstractIndexSeekLeafPlanner extends LeafPlanner with LeafPlanFromExpressions {

  // Abstract methods ***********

  protected def constructPlan(idName: String,
                              label: LabelToken,
                              properties: Seq[IndexedProperty],
                              isUnique: Boolean,
                              valueExpr: QueryExpression[Expression],
                              hint: Option[UsingIndexHint],
                              argumentIds: Set[String],
                              providedOrder: ProvidedOrder,
                              interestingOrder: InterestingOrder,
                              context: LogicalPlanningContext,
                              onlyExists: Boolean)
                             (solvedPredicates: Seq[Expression], predicatesForCardinalityEstimation: Seq[Expression]): LogicalPlan

  protected def findIndexesForLabel(labelId: Int, context: LogicalPlanningContext): Iterator[IndexDescriptor]

  // Concrete methods ***********

  override def producePlanFor(predicates: Set[Expression], qg: QueryGraph, interestingOrder: InterestingOrder, context: LogicalPlanningContext): Set[LeafPlansForVariable] = {
    implicit val labelPredicateMap: Map[String, Set[HasLabels]] = qg.selections.labelPredicates
    implicit val semanticTable: SemanticTable = context.semanticTable
    if (labelPredicateMap.isEmpty)
      Set.empty
    else {
      val arguments: Set[LogicalVariable] = qg.argumentIds.map(n => Variable(n)(null))
      val indexCompatibles: Set[IndexCompatiblePredicate] = predicates.collect(
        asIndexCompatiblePredicate(qg.argumentIds, arguments, qg.hints))
      val result = indexCompatibles.map(_.name).flatMap { name =>
        val labelPredicates = labelPredicateMap.getOrElse(name, Set.empty)
        val nodePredicates = indexCompatibles.filter(p => p.name == name)
        maybeLeafPlans(name, producePlansForSpecificVariable(name, nodePredicates, labelPredicates, qg.hints, qg.argumentIds, context, interestingOrder))
      }

      if (result.isEmpty) {
        val seekableIdentifiers: Set[Variable] = findNonSeekableIdentifiers(qg.selections.flatPredicates, context)
        DynamicPropertyNotifier.process(seekableIdentifiers, IndexLookupUnfulfillableNotification, qg, context)
      }
      result
    }
  }

  override def apply(qg: QueryGraph, interestingOrder: InterestingOrder, context: LogicalPlanningContext): Seq[LogicalPlan] = {
    producePlanFor(qg.selections.flatPredicates.toSet, qg, interestingOrder, context).toSeq.flatMap(_.plans)
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

  private def producePlansForSpecificVariable(idName: String, indexCompatiblePredicates: Set[IndexCompatiblePredicate],
                                              labelPredicates: Set[HasLabels],
                                              hints: Set[Hint], argumentIds: Set[String],
                                              context: LogicalPlanningContext,
                                              interestingOrder: InterestingOrder): Set[LogicalPlan] = {
    implicit val semanticTable: SemanticTable = context.semanticTable
    for (labelPredicate <- labelPredicates;
         labelName <- labelPredicate.labels;
         labelId: LabelId <- semanticTable.id(labelName).toSeq;
         indexDescriptor: IndexDescriptor <- findIndexesForLabel(labelId, context);
         (predicates, canGetValues, providedOrder) <- predicatesForIndex(indexDescriptor, indexCompatiblePredicates, interestingOrder))
      yield
        createLogicalPlan(idName, hints, argumentIds, labelPredicate, labelName, labelId, predicates, indexDescriptor.isUnique, canGetValues, providedOrder, interestingOrder, context, semanticTable)
  }

  private def createLogicalPlan(idName: String,
                                hints: Set[Hint],
                                argumentIds: Set[String],
                                labelPredicate: HasLabels,
                                labelName: LabelName,
                                labelId: LabelId,
                                indexCompatiblePredicates: Seq[IndexCompatiblePredicate],
                                isUnique: Boolean,
                                canGetValues: Seq[GetValueFromIndexBehavior],
                                providedOrder: ProvidedOrder,
                                interestingOrder: InterestingOrder,
                                context: LogicalPlanningContext,
                                semanticTable: SemanticTable): LogicalPlan = {
    val hint = {
      val name = idName
      val propertyNames = indexCompatiblePredicates.map(_.propertyKeyName.name)
      hints.collectFirst {
        case hint@UsingIndexHint(Variable(`name`), `labelName`, propertyKeyName, _)
          if propertyKeyName.map(_.name) == propertyNames => hint
      }
    }

    val equalityPredicates = indexCompatiblePredicates.takeWhile(_.exactPredicate)
    val equalityAndNextPredicates =
      if (equalityPredicates == indexCompatiblePredicates) equalityPredicates
      else equalityPredicates :+ indexCompatiblePredicates(equalityPredicates.length)
    val indexedPredicates: Seq[IndexCompatiblePredicate] =
      equalityAndNextPredicates ++ indexCompatiblePredicates.slice(equalityAndNextPredicates.length, indexCompatiblePredicates.length).map(_.convertToExists)

    val queryExpression: QueryExpression[Expression] = mergeQueryExpressionsToSingleOne(indexedPredicates)

    val properties = indexCompatiblePredicates.map(p => p.propertyKeyName).zip(canGetValues).map {
      case (propertyName, getValue) => IndexedProperty(PropertyKeyToken(propertyName, semanticTable.id(propertyName).head), getValue)
    }
    val entryConstructor: (Seq[Expression], Seq[Expression]) => LogicalPlan =
      constructPlan(idName, LabelToken(labelName, labelId), properties, isUnique, queryExpression, hint, argumentIds, providedOrder, interestingOrder, context, indexedPredicates.head.isExists)

    val solvedPredicates = indexedPredicates.zip(indexCompatiblePredicates).filter(p => p._1 == p._2).map(_._1)
                             .filter(_.solvesPredicate).map(p => p.propertyPredicate) :+ labelPredicate

    val predicatesForCardinalityEstimation = indexCompatiblePredicates.map(p => p.propertyPredicate) :+ labelPredicate
    entryConstructor(solvedPredicates, predicatesForCardinalityEstimation)
  }

  private def mergeQueryExpressionsToSingleOne(predicates: Seq[IndexCompatiblePredicate]): QueryExpression[Expression] =
    if (predicates.length == 1)
      predicates.head.queryExpression
    else {
      CompositeQueryExpression(predicates.map(_.queryExpression))
    }

  private def asIndexCompatiblePredicate(argumentIds: Set[String],
                                         arguments: Set[LogicalVariable],
                                         hints: Set[Hint])(
                                          implicit labelPredicateMap: Map[String, Set[HasLabels]],
                                          semanticTable: SemanticTable):
  PartialFunction[Expression, IndexCompatiblePredicate] = {
    def validDependencies(seekable: Seekable[_]): Boolean = {
      seekable.dependencies.forall(arguments) && !arguments(seekable.ident)
    }
    {
      // n.prop IN [ ... ]
      case predicate@AsPropertySeekable(seekable: PropertySeekable) if validDependencies(seekable) =>
        val queryExpression = seekable.args.asQueryExpression
        IndexCompatiblePredicate(seekable.name, seekable.propertyKey, predicate, queryExpression, seekable.propertyValueType(semanticTable), exactPredicate = true,
          hints, argumentIds, solvesPredicate = true)

      // ... = n.prop
      // In some rare cases, we can't rewrite these predicates cleanly,
      // and so planning needs to search for these cases explicitly
      case predicate@Equals(a, prop@Property(seekable@LogicalVariable(_), propKeyName))
        if a.dependencies.forall(arguments) && !arguments(seekable) =>
        val expr = SingleQueryExpression(a)
        IndexCompatiblePredicate(seekable.name, propKeyName, predicate, expr, Seekable.cypherTypeForTypeSpec(semanticTable.getActualTypeFor(prop)), exactPredicate = true,
          hints, argumentIds, solvesPredicate = true)

      // n.prop STARTS WITH "prefix%..."
      case predicate@AsStringRangeSeekable(seekable) if validDependencies(seekable) =>
        val partialPredicate = PartialPredicate(seekable.expr, predicate)
        val queryExpression = seekable.asQueryExpression
        val propertyKey = seekable.propertyKey
        IndexCompatiblePredicate(seekable.name, propertyKey, partialPredicate, queryExpression, seekable.propertyValueType(semanticTable), exactPredicate = false,
          hints, argumentIds, solvesPredicate = true)

      // n.prop <|<=|>|>= value
      case predicate@AsValueRangeSeekable(seekable) if validDependencies(seekable) =>
        val queryExpression = seekable.asQueryExpression
        val keyName = seekable.propertyKeyName
        IndexCompatiblePredicate(seekable.name, keyName, predicate, queryExpression, seekable.propertyValueType(semanticTable), exactPredicate = false,
          hints, argumentIds, solvesPredicate = true)

      // The planned index seek will almost satisfy the predicate, but with the possibility of some false positives.
      // Since it reduces the cardinality to almost the level of the predicate, we can use the predicate to calculate cardinality,
      // but not mark it as solved, since the planner will still need to solve it with a Filter.
      case predicate@AsDistanceSeekable(seekable) if validDependencies(seekable) =>
        val queryExpression = seekable.asQueryExpression
        val keyName = seekable.propertyKeyName
        IndexCompatiblePredicate(seekable.name, keyName, predicate, queryExpression, seekable.propertyValueType(semanticTable), exactPredicate = false,
          hints, argumentIds, solvesPredicate = false)

      // MATCH (n:User) WHERE exists(n.prop) RETURN n
      // Should only be allowed as part of an composite index:
      // "MATCH (n:User) WHERE n.foo = 'foo' AND exists(n.bar) RETURN n" with index on User(foo, bar)
      case predicate@AsPropertyScannable(scannable) if !arguments(scannable.ident) =>
        // scannable.expr is partialPredicate saying it solves exists() but not the predicate
        val solvedPredicate = if (scannable.solvesPredicate) predicate else scannable.expr
        IndexCompatiblePredicate(scannable.name, scannable.propertyKey, solvedPredicate, ExistenceQueryExpression(), CTAny,
          exactPredicate = false, hints, argumentIds, solvesPredicate = true)

      // n.prop ENDS WITH 'substring'
      // It is always converted to exists and will need filtering
      case predicate@EndsWith(prop@Property(variable@Variable(name), keyName), expr) if expr.dependencies.forall(arguments) && !arguments(variable) =>
        // create a partialPredicate saying it solves exists() but not the ENDS WITH
        val solvedPredicate = PartialPredicate(
          FunctionInvocation(FunctionName(functions.Exists.name)(predicate.position), prop)(predicate.position),
          predicate
        )
        IndexCompatiblePredicate(name, keyName, solvedPredicate, ExistenceQueryExpression(), CTAny, exactPredicate = false,
          hints, argumentIds, solvesPredicate = true)

      // n.prop CONTAINS 'substring'
      // It is always converted to exists and will need filtering
      case predicate@Contains(prop@Property(variable@Variable(name), keyName), expr) if expr.dependencies.forall(arguments) && !arguments(variable) =>
        // create a partialPredicate saying it solves exists() but not the CONTAINS
        val solvedPredicate = PartialPredicate(
          FunctionInvocation(FunctionName(functions.Exists.name)(predicate.position), prop)(predicate.position),
          predicate
        )
        IndexCompatiblePredicate(name, keyName, solvedPredicate, ExistenceQueryExpression(), CTAny, exactPredicate = false,
          hints, argumentIds, solvesPredicate = true)
    }
  }

  /**
    * Finds the Seq of IndexCompatiblePredicate that can be solved by the indexDescriptor.
    * Either each property of the index solves some predicate, in which case case this returns Some(...).
    * Or, if at least one property does not solve a predicate, this returns None.
    *
    * Together with the matching IndexCompatiblePredicates it also returns the GetValueFromIndexBehavior for each property. The tuple
    * contains two lists of the same size, which is indexDescriptor.properties.length
    */
  private def predicatesForIndex(indexDescriptor: IndexDescriptor, predicates: Set[IndexCompatiblePredicate], interestingOrder: InterestingOrder)
                                (implicit semanticTable: SemanticTable): Option[(Seq[IndexCompatiblePredicate], Seq[GetValueFromIndexBehavior], ProvidedOrder)] = {
    val maybeMatchingPredicates = indexDescriptor.properties.foldLeft(Option(Seq.empty[IndexCompatiblePredicate])) {
      case (None, _) => None
      case (Some(acc), propertyKeyId) =>
        predicates.find(p => semanticTable.id(p.propertyKeyName).contains(propertyKeyId)) match {
          case Some(found) if !found.isExists ||
            found.isExists && indexDescriptor.isComposite => Some(acc :+ found)
          case _ => None
        }
    }

    maybeMatchingPredicates
      .map { matchingPredicates =>
        matchPredicateWithIndexDescriptorAndInterestingOrder(matchingPredicates, indexDescriptor, interestingOrder)
      }
  }

  private def matchPredicateWithIndexDescriptorAndInterestingOrder(matchingPredicates: Seq[IndexCompatiblePredicate],
                                                                   indexDescriptor: IndexDescriptor,
                                                                   interestingOrder: InterestingOrder): (Seq[IndexCompatiblePredicate], Seq[GetValueFromIndexBehavior], ProvidedOrder) = {
    val types = matchingPredicates.map(mp => mp.propertyType)

    // Ask the index for its value capabilities for the types of all properties.
    // We might override some of these later if they value is known in an equality predicate
    val propertyBehaviorFromIndex = indexDescriptor.valueCapability(types)

    // Combine plannable predicates with their available properties
    val propertyBehaviours = propertyBehaviorFromIndex.zip(matchingPredicates.map(_.exactPredicate)).map {
      case (_, true) => CanGetValue
      case (behavior, _) => behavior
    }

    // Ask the index for its order capabilities for the types in prefix/subset defined by the interesting order
    val indexProperties = matchingPredicates.map(mp => {
      val pos = mp.propertyPredicate.position
      Property(Variable(mp.name)(pos), mp.propertyKeyName)(pos)
    }).slice(0, types.length)

    val providedOrder = ResultOrdering.withIndexOrderCapability(interestingOrder, indexProperties, types, indexDescriptor.orderCapability)

    // Return a tuple of matching predicates(plannables), an equal length seq of property behaviours and a single index ordering capability
    (matchingPredicates, propertyBehaviours, providedOrder)
  }

  /**
    * @param propertyType
    *                     We need to ask the index whether it supports getting values for that type
    * @param exactPredicate
    *                     We might already know if we can get values or not, for exact predicates. If this is `true` we will set GetValue,
    *                     otherwise we need to ask the index.
    */
  private case class IndexCompatiblePredicate(name: String,
                                              propertyKeyName: PropertyKeyName,
                                              propertyPredicate: Expression,
                                              queryExpression: QueryExpression[Expression],
                                              propertyType: CypherType,
                                              exactPredicate: Boolean,
                                              hints: Set[Hint],
                                              argumentIds: Set[String],
                                              solvesPredicate: Boolean)
                                             (implicit labelPredicateMap: Map[String, Set[HasLabels]]) {

    def convertToExists: IndexCompatiblePredicate = queryExpression match {
        case _: ExistenceQueryExpression[Expression] => this
        case _: CompositeQueryExpression[Expression] => throw new IllegalStateException("A CompositeQueryExpression can't be nested in a CompositeQueryExpression")
        case _ =>
          copy(queryExpression = ExistenceQueryExpression(), exactPredicate = false, solvesPredicate = false)
      }

    def isExists: Boolean = queryExpression match {
      case _: ExistenceQueryExpression[Expression] => true
      case _ => false
    }
  }
}
