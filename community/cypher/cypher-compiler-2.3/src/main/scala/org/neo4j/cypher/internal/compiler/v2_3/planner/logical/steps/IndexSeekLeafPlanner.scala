/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_3.commands.QueryExpression
import org.neo4j.cypher.internal.compiler.v2_3.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.spi.SchemaTypes.IndexDescriptor
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.notification.IndexLookupUnfulfillableNotification


abstract class AbstractIndexSeekLeafPlanner extends LeafPlanner {
  def apply(qg: QueryGraph)(implicit context: LogicalPlanningContext) = {
    implicit val semanticTable = context.semanticTable
    val predicates: Seq[Expression] = qg.selections.flatPredicates
    val labelPredicateMap: Map[IdName, Set[HasLabels]] = qg.selections.labelPredicates

    def producePlanFor(name: String, propertyKeyName: PropertyKeyName, propertyPredicate: Expression, queryExpression: QueryExpression[Expression]) = {
      val idName = IdName(name)
      for (labelPredicate <- labelPredicateMap.getOrElse(idName, Set.empty);
           labelName <- labelPredicate.labels;
           indexDescriptor <- findIndexesFor(labelName.name, propertyKeyName.name);
           labelId <- labelName.id)
      yield {
        val propertyName = propertyKeyName.name
        val hint = qg.hints.collectFirst {
          case hint @ UsingIndexHint(Identifier(`name`), `labelName`, PropertyKeyName(`propertyName`)) => hint
        }
        val entryConstructor: (Seq[Expression]) => LogicalPlan =
          constructPlan(idName, LabelToken(labelName, labelId), PropertyKeyToken(propertyKeyName, propertyKeyName.id.head),
                        queryExpression, hint, qg.argumentIds)
        entryConstructor(Seq(propertyPredicate, labelPredicate))
      }
    }

    val arguments = qg.argumentIds.map(n => Identifier(n.name)(null))

    val resultPlans = predicates.collect {
      // n.prop IN [ ... ]
      case predicate@AsPropertySeekable(seekable)
        if seekable.args.dependencies.forall(arguments) && !arguments(seekable.ident) =>
        producePlanFor(seekable.name, seekable.propertyKey, predicate, seekable.args.asQueryExpression)

      // n.prop STARTS WITH "prefix%..."
      case predicate@AsStringRangeSeekable(seekable) =>
        producePlanFor(seekable.name, seekable.propertyKey, PartialPredicate(seekable.expr, predicate), seekable.asQueryExpression)

      // n.prop <|<=|>|>= value
      case predicate@AsValueRangeSeekable(seekable) =>
        producePlanFor(seekable.name, seekable.propertyKeyName, predicate, seekable.asQueryExpression)
    }.flatten

    if (resultPlans.isEmpty) {
      DynamicPropertyNotifier.process(findNonSeekableIdentifiers(predicates), IndexLookupUnfulfillableNotification, qg)
    }

    resultPlans
  }

  private def findNonSeekableIdentifiers(predicates: Seq[Expression])(implicit context: LogicalPlanningContext) =
    predicates.flatMap {
      // n['some' + n.prop] IN [ ... ]
      case predicate@AsDynamicPropertyNonSeekable(nonSeekableId)
        if context.semanticTable.isNode(nonSeekableId) => Some(nonSeekableId)

      // n['some' + n.prop] STARTS WITH "prefix%..."
      case predicate@AsStringRangeNonSeekable(nonSeekableId)
        if context.semanticTable.isNode(nonSeekableId) => Some(nonSeekableId)

      // n['some' + n.prop] <|<=|>|>= value
      case predicate@AsValueRangeNonSeekable(nonSeekableId)
        if context.semanticTable.isNode(nonSeekableId) => Some(nonSeekableId)

      case _ => None
    }.toSet

  protected def constructPlan(idName: IdName,
                              label: LabelToken,
                              propertyKey: PropertyKeyToken,
                              valueExpr: QueryExpression[Expression],
                              hint: Option[UsingIndexHint],
                              argumentIds: Set[IdName])
                             (implicit context: LogicalPlanningContext): (Seq[Expression]) => LogicalPlan

  protected def findIndexesFor(label: String, property: String)(implicit context: LogicalPlanningContext): Option[IndexDescriptor]
}

object uniqueIndexSeekLeafPlanner extends AbstractIndexSeekLeafPlanner {
  protected def constructPlan(idName: IdName,
                              label: LabelToken,
                              propertyKey: PropertyKeyToken,
                              valueExpr: QueryExpression[Expression],
                              hint: Option[UsingIndexHint],
                              argumentIds: Set[IdName])
                             (implicit context: LogicalPlanningContext): (Seq[Expression]) => LogicalPlan =
    (predicates: Seq[Expression]) =>
      context.logicalPlanProducer.planNodeUniqueIndexSeek(idName, label, propertyKey, valueExpr, predicates, hint, argumentIds)

  protected def findIndexesFor(label: String, property: String)(implicit context: LogicalPlanningContext): Option[IndexDescriptor] =
    context.planContext.getUniqueIndexRule(label, property)
}

object indexSeekLeafPlanner extends AbstractIndexSeekLeafPlanner {
  protected def constructPlan(idName: IdName,
                              label: LabelToken,
                              propertyKey: PropertyKeyToken,
                              valueExpr: QueryExpression[Expression],
                              hint: Option[UsingIndexHint],
                              argumentIds: Set[IdName])
                             (implicit context: LogicalPlanningContext): (Seq[Expression]) => LogicalPlan =
    (predicates: Seq[Expression]) =>
      context.logicalPlanProducer.planNodeIndexSeek(idName, label, propertyKey, valueExpr, predicates, hint, argumentIds)

  protected def findIndexesFor(label: String, property: String)(implicit context: LogicalPlanningContext): Option[IndexDescriptor] = {
    if (uniqueIndex(label, property).isDefined) None else anyIndex(label, property)
  }

  private def anyIndex(label: String, property: String)(implicit context: LogicalPlanningContext) = context.planContext.getIndexRule(label, property)
  private def uniqueIndex(label: String, property: String)(implicit context: LogicalPlanningContext) = context.planContext.getUniqueIndexRule(label, property)
}

object legacyHintLeafPlanner extends LeafPlanner {
  def apply(qg: QueryGraph)(implicit context: LogicalPlanningContext) = {
    qg.hints.toSeq.collect {
      case hint: LegacyIndexHint if !qg.argumentIds(IdName(hint.identifier.name)) =>
        context.logicalPlanProducer.planLegacyHintSeek(IdName(hint.identifier.name), hint, qg.argumentIds)
    }
  }
}
