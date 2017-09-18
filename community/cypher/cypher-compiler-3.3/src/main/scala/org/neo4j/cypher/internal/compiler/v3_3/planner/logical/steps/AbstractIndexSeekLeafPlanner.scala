/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_3.IndexDescriptor
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.LeafPlansForVariable.maybeLeafPlans
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.{LeafPlanFromExpressions, LeafPlanner, LeafPlansForVariable, LogicalPlanningContext}
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.notification.IndexLookupUnfulfillableNotification
import org.neo4j.cypher.internal.frontend.v3_4.{LabelId, SemanticTable}
import org.neo4j.cypher.internal.ir.v3_4.{IdName, QueryGraph}
import org.neo4j.cypher.internal.v3_4.logical.plans._

abstract class AbstractIndexSeekLeafPlanner extends LeafPlanner with LeafPlanFromExpressions {

  // Abstract methods ***********
  protected def constructPlan(idName: IdName,
                              label: LabelToken,
                              propertyKeys: Seq[PropertyKeyToken],
                              valueExpr: QueryExpression[Expression],
                              hint: Option[UsingIndexHint],
                              argumentIds: Set[IdName])
                             (implicit context: LogicalPlanningContext): Seq[Expression] => LogicalPlan

  protected def findIndexesForLabel(labelId: Int)(implicit context: LogicalPlanningContext): Iterator[IndexDescriptor]

  protected def findIndexesFor(label: String, properties: Seq[String])(implicit context: LogicalPlanningContext): Option[IndexDescriptor]


  override def producePlanFor(predicates: Set[Expression], qg: QueryGraph)
                             (implicit context: LogicalPlanningContext): Set[LeafPlansForVariable] = {
    implicit val labelPredicateMap: Map[IdName, Set[HasLabels]] = qg.selections.labelPredicates
    if (labelPredicateMap.isEmpty)
      Set.empty
    else {
      val arguments: Set[Variable] = qg.argumentIds.map(n => Variable(n.name)(null))
      val plannables: Set[IndexPlannableExpression] = predicates.collect(
        indexPlannableExpression(qg.argumentIds, arguments, qg.hints))
      val result = plannables.map(_.name).flatMap { name =>
        val idName = IdName(name)
        val labelPredicates = labelPredicateMap.getOrElse(idName, Set.empty)
        val nodePlannables = plannables.filter(p => p.name == name)
        maybeLeafPlans(name, producePlansForSpecificVariable(idName, nodePlannables, labelPredicates, qg.hints, qg.argumentIds))
      }

      if (result.isEmpty) {
        val seekableIdentifiers: Set[Variable] = findNonSeekableIdentifiers(qg.selections.flatPredicates)
        DynamicPropertyNotifier.process(seekableIdentifiers, IndexLookupUnfulfillableNotification, qg)
      }
      result
    }
  }

  override def apply(qg: QueryGraph)(implicit context: LogicalPlanningContext): Seq[LogicalPlan] = {
    producePlanFor(qg.selections.flatPredicates.toSet, qg).toSeq.flatMap(_.plans)
  }

  private def findNonSeekableIdentifiers(predicates: Seq[Expression])(implicit context: LogicalPlanningContext): Set[Variable] =
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

  private def producePlansForSpecificVariable(idName: IdName, nodePlannables: Set[IndexPlannableExpression],
                                              labelPredicates: Set[HasLabels],
                                              hints: Set[Hint], argumentIds: Set[IdName])
                                             (implicit context: LogicalPlanningContext): Set[LogicalPlan] = {
    implicit val semanticTable: SemanticTable = context.semanticTable
    for (labelPredicate <- labelPredicates;
         labelName <- labelPredicate.labels;
         labelId: LabelId <- labelName.id.toSeq;
         indexDescriptor: IndexDescriptor <- findIndexesForLabel(labelId);
         plannables: Seq[IndexPlannableExpression] <- plannablesForIndex(indexDescriptor, nodePlannables))
      yield
        createLogicalPlan(idName, hints, argumentIds, labelPredicate, labelName, labelId, plannables)
  }

  private def createLogicalPlan(idName: IdName,
                  hints: Set[Hint],
                  argumentIds: Set[IdName],
                  labelPredicate: HasLabels,
                  labelName: LabelName,
                  labelId: LabelId,
                  plannables: Seq[IndexPlannableExpression])
                 (implicit context: LogicalPlanningContext, semanticTable: SemanticTable ): LogicalPlan = {
    val hint = {
      val name = idName.name
      val propertyNames = plannables.map(_.propertyKeyName.name)
      hints.collectFirst {
        case hint@UsingIndexHint(Variable(`name`), `labelName`, properties)
          if properties.map(_.name) == propertyNames => hint
      }
    }

    val queryExpression: QueryExpression[Expression] = mergeQueryExpressionsToSingleOne(plannables)

    val propertyKeyTokens = plannables.map(p => p.propertyKeyName).map(n => PropertyKeyToken(n, n.id.head))
    val entryConstructor: Seq[Expression] => LogicalPlan =
      constructPlan(idName, LabelToken(labelName, labelId), propertyKeyTokens, queryExpression, hint, argumentIds)

    entryConstructor(plannables.map(p => p.propertyPredicate) :+ labelPredicate)
  }

  private def mergeQueryExpressionsToSingleOne(plannables: Seq[IndexPlannableExpression]): QueryExpression[Expression] =
    if (plannables.length == 1)
      plannables.head.queryExpression
    else {
      val expressions: Seq[Expression] = plannables.flatMap(_.queryExpression.expressions)
      CompositeQueryExpression(plannables.map(_.queryExpression))
    }

  private def indexPlannableExpression(argumentIds: Set[IdName],
                                       arguments: Set[Variable],
                                       hints: Set[Hint])(implicit labelPredicateMap: Map[IdName, Set[HasLabels]]):
  PartialFunction[Expression, IndexPlannableExpression] = {
    // n.prop IN [ ... ]
    case predicate@AsPropertySeekable(seekable: PropertySeekable)
      if seekable.args.dependencies.forall(arguments) && !arguments(seekable.ident) =>
      val queryExpression = seekable.args.asQueryExpression
      IndexPlannableExpression(seekable.name, seekable.propertyKey, predicate, queryExpression, hints, argumentIds)

    // ... = n.prop
    // In some rare cases, we can't rewrite these predicates cleanly,
    // and so planning needs to search for these cases explicitly
    case predicate@Equals(a, Property(seekable@Variable(_), propKeyName))
      if a.dependencies.forall(arguments) && !arguments(seekable) =>
      val expr = SingleQueryExpression(a)
      IndexPlannableExpression(seekable.name, propKeyName, predicate, expr, hints, argumentIds)

    // n.prop STARTS WITH "prefix%..."
    case predicate@AsStringRangeSeekable(seekable) =>
      val partialPredicate = PartialPredicate(seekable.expr, predicate)
      val queryExpression = seekable.asQueryExpression
      val propertyKey = seekable.propertyKey
      IndexPlannableExpression(seekable.name, propertyKey, partialPredicate, queryExpression, hints, argumentIds)

    // n.prop <|<=|>|>= value
    case predicate@AsValueRangeSeekable(seekable) =>
      val queryExpression = seekable.asQueryExpression
      val keyName = seekable.propertyKeyName
      IndexPlannableExpression(seekable.name, keyName, predicate, queryExpression, hints, argumentIds)
  }

  private def plannablesForIndex(indexDescriptor: IndexDescriptor, plannables: Set[IndexPlannableExpression])
                                (implicit semanticTable: SemanticTable): Option[Seq[IndexPlannableExpression]] = {
    val foundPredicates: Seq[IndexPlannableExpression] = indexDescriptor.properties.flatMap { propertyKeyId =>
      plannables find (p => p.propertyKeyName.id.contains(propertyKeyId))
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
                                      hints: Set[Hint], argumentIds: Set[IdName])
                                     (implicit labelPredicateMap: Map[IdName, Set[HasLabels]])
}
