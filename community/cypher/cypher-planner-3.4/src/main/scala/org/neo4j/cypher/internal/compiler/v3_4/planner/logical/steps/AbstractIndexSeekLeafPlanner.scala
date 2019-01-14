/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.LeafPlansForVariable.maybeLeafPlans
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.{LeafPlanFromExpressions, LeafPlanner, LeafPlansForVariable, LogicalPlanningContext}
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.notification.IndexLookupUnfulfillableNotification
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.ir.v3_4.QueryGraph
import org.neo4j.cypher.internal.planner.v3_4.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.util.v3_4.LabelId
import org.neo4j.cypher.internal.v3_4.logical.plans._
import org.neo4j.cypher.internal.v3_4.expressions._

abstract class AbstractIndexSeekLeafPlanner extends LeafPlanner with LeafPlanFromExpressions {

  // Abstract methods ***********
  protected def constructPlan(idName: String,
                              label: LabelToken,
                              propertyKeys: Seq[PropertyKeyToken],
                              valueExpr: QueryExpression[Expression],
                              hint: Option[UsingIndexHint],
                              argumentIds: Set[String],
                              context: LogicalPlanningContext)
                             (solvedPredicates: Seq[Expression], predicatesForCardinalityEstimation: Seq[Expression]): LogicalPlan

  protected def findIndexesForLabel(labelId: Int, context: LogicalPlanningContext): Iterator[IndexDescriptor]

  protected def findIndexesFor(label: String, properties: Seq[String], context: LogicalPlanningContext): Option[IndexDescriptor]


  override def producePlanFor(predicates: Set[Expression],
                              qg: QueryGraph,
                              context: LogicalPlanningContext): Set[LeafPlansForVariable] = {
    implicit val labelPredicateMap: Map[String, Set[HasLabels]] = qg.selections.labelPredicates
    if (labelPredicateMap.isEmpty)
      Set.empty
    else {
      val arguments: Set[LogicalVariable] = qg.argumentIds.map(n => Variable(n)(null))
      val plannables: Set[IndexPlannableExpression] = predicates.collect(
        indexPlannableExpression(qg.argumentIds, arguments, qg.hints.toSet))
      val result = plannables.map(_.name).flatMap { name =>
        val idName = name
        val labelPredicates = labelPredicateMap.getOrElse(idName, Set.empty)
        val nodePlannables = plannables.filter(p => p.name == name)
        maybeLeafPlans(name, producePlansForSpecificVariable(idName, nodePlannables, labelPredicates, qg.hints, qg.argumentIds, context))
      }

      if (result.isEmpty) {
        val seekableIdentifiers: Set[Variable] = findNonSeekableIdentifiers(qg.selections.flatPredicates, context)
        DynamicPropertyNotifier.process(seekableIdentifiers, IndexLookupUnfulfillableNotification, qg, context)
      }
      result
    }
  }

  override def apply(qg: QueryGraph, context: LogicalPlanningContext, solveds: Solveds, cardinalities: Cardinalities): Seq[LogicalPlan] = {
    producePlanFor(qg.selections.flatPredicates.toSet, qg, context).toSeq.flatMap(_.plans)
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

  private def producePlansForSpecificVariable(idName: String, nodePlannables: Set[IndexPlannableExpression],
                                              labelPredicates: Set[HasLabels],
                                              hints: Seq[Hint], argumentIds: Set[String],
                                              context: LogicalPlanningContext): Set[LogicalPlan] = {
    implicit val semanticTable: SemanticTable = context.semanticTable
    for (labelPredicate <- labelPredicates;
         labelName <- labelPredicate.labels;
         labelId: LabelId <- semanticTable.id(labelName).toSeq;
         indexDescriptor: IndexDescriptor <- findIndexesForLabel(labelId, context);
         plannables: Seq[IndexPlannableExpression] <- plannablesForIndex(indexDescriptor, nodePlannables))
      yield
        createLogicalPlan(idName, hints, argumentIds, labelPredicate, labelName, labelId, plannables, context, semanticTable)
  }

  private def createLogicalPlan(idName: String,
                  hints: Seq[Hint],
                  argumentIds: Set[String],
                  labelPredicate: HasLabels,
                  labelName: LabelName,
                  labelId: LabelId,
                  plannables: Seq[IndexPlannableExpression],
                  context: LogicalPlanningContext,
                  semanticTable: SemanticTable ): LogicalPlan = {
    val hint = {
      val name = idName
      val propertyNames = plannables.map(_.propertyKeyName.name)
      hints.collectFirst {
        case hint@UsingIndexHint(Variable(`name`), `labelName`, properties, _)
          if properties.map(_.name) == propertyNames => hint
      }
    }

    val queryExpression: QueryExpression[Expression] = mergeQueryExpressionsToSingleOne(plannables)

    val propertyKeyTokens = plannables.map(p => p.propertyKeyName).map(n => PropertyKeyToken(n, semanticTable.id(n).head))
    val entryConstructor: (Seq[Expression], Seq[Expression]) => LogicalPlan =
      constructPlan(idName, LabelToken(labelName, labelId), propertyKeyTokens, queryExpression, hint, argumentIds, context)

    val solvedPredicates = plannables.filter(_.solvesPredicate).map(p => p.propertyPredicate) :+ labelPredicate
    val predicatesForCardinalityEstimation = plannables.map(p => p.propertyPredicate) :+ labelPredicate
    entryConstructor(solvedPredicates, predicatesForCardinalityEstimation)
  }

  private def mergeQueryExpressionsToSingleOne(plannables: Seq[IndexPlannableExpression]): QueryExpression[Expression] =
    if (plannables.length == 1)
      plannables.head.queryExpression
    else {
      val expressions: Seq[Expression] = plannables.flatMap(_.queryExpression.expressions)
      CompositeQueryExpression(plannables.map(_.queryExpression))
    }

  private def indexPlannableExpression(argumentIds: Set[String],
                                       arguments: Set[LogicalVariable],
                                       hints: Set[Hint])(implicit labelPredicateMap: Map[String, Set[HasLabels]]):
  PartialFunction[Expression, IndexPlannableExpression] = {
    // n.prop IN [ ... ]
    case predicate@AsPropertySeekable(seekable: PropertySeekable)
      if seekable.args.dependencies.forall(arguments) && !arguments(seekable.ident) =>
      val queryExpression = seekable.args.asQueryExpression
      IndexPlannableExpression(seekable.name, seekable.propertyKey, predicate, queryExpression, hints, argumentIds, solvesPredicate = true)

    // ... = n.prop
    // In some rare cases, we can't rewrite these predicates cleanly,
    // and so planning needs to search for these cases explicitly
    case predicate@Equals(a, Property(seekable@LogicalVariable(_), propKeyName))
      if a.dependencies.forall(arguments) && !arguments(seekable) =>
      val expr = SingleQueryExpression(a)
      IndexPlannableExpression(seekable.name, propKeyName, predicate, expr, hints, argumentIds, solvesPredicate = true)

    // n.prop STARTS WITH "prefix%..."
    case predicate@AsStringRangeSeekable(seekable) =>
      val partialPredicate = PartialPredicate(seekable.expr, predicate)
      val queryExpression = seekable.asQueryExpression
      val propertyKey = seekable.propertyKey
      IndexPlannableExpression(seekable.name, propertyKey, partialPredicate, queryExpression, hints, argumentIds, solvesPredicate = true)

    // n.prop <|<=|>|>= value
    case predicate@AsValueRangeSeekable(seekable) =>
      val queryExpression = seekable.asQueryExpression
      val keyName = seekable.propertyKeyName
      IndexPlannableExpression(seekable.name, keyName, predicate, queryExpression, hints, argumentIds, solvesPredicate = true)

    // The planned index seek will almost satisfy the predicate, but with the possibility of some false positives.
    // Since it reduces the cardinality to almost the level of the predicate, we can use the predicate to calculate cardinality,
    // but not mark it as solved, since the planner will still need to solve it with a Filter.
    case predicate@AsDistanceSeekable(seekable) =>
      val queryExpression = seekable.asQueryExpression
      val keyName = seekable.propertyKeyName
      IndexPlannableExpression(seekable.name, keyName, predicate, queryExpression, hints, argumentIds, solvesPredicate = false)
  }

  private def plannablesForIndex(indexDescriptor: IndexDescriptor, plannables: Set[IndexPlannableExpression])
                                (implicit semanticTable: SemanticTable): Option[Seq[IndexPlannableExpression]] = {
    val foundPredicates: Seq[IndexPlannableExpression] = indexDescriptor.properties.flatMap { propertyKeyId =>
      plannables find (p => semanticTable.id(p.propertyKeyName).contains(propertyKeyId))
    }

    // Currently we only support using the composite index if ALL properties are specified, but this could be generalized
    if (foundPredicates.length == indexDescriptor.properties.length && isSupportedByCurrentIndexes(foundPredicates))
      Some(foundPredicates)
    else
      None
  }

  private def isSupportedByCurrentIndexes(foundPredicates: Seq[IndexPlannableExpression]) = {
    // We currently only support range queries against single prop indexes
    foundPredicates.length == 1 ||
      foundPredicates.forall(_.queryExpression match {
        case _: SingleQueryExpression[_] => true
        case _: ManyQueryExpression[_] => true
        case _ => false
      })
  }

  case class IndexPlannableExpression(name: String, propertyKeyName: PropertyKeyName,
                                      propertyPredicate: Expression, queryExpression: QueryExpression[Expression],
                                      hints: Set[Hint], argumentIds: Set[String], solvesPredicate: Boolean)
                                     (implicit labelPredicateMap: Map[String, Set[HasLabels]])
}
