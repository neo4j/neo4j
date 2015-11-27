/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_0.commands.QueryExpression
import org.neo4j.cypher.internal.compiler.v3_0.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_0.SemanticTable
import org.neo4j.cypher.internal.frontend.v3_0.ast._
import org.neo4j.cypher.internal.frontend.v3_0.notification.IndexLookupUnfulfillableNotification
import org.neo4j.kernel.api.index.IndexDescriptor

abstract class AbstractIndexSeekLeafPlanner extends LeafPlanner {

  def apply(qg: QueryGraph)(implicit context: LogicalPlanningContext): Seq[LogicalPlan] = {
    implicit val semanticTable = context.semanticTable
    val predicates: Seq[Expression] = qg.selections.flatPredicates
    val labelPredicateMap: Map[IdName, Set[HasLabels]] = qg.selections.labelPredicates

    val resultPlans = collectPlans(predicates, qg.argumentIds, labelPredicateMap, qg.hints)

    if (resultPlans.isEmpty) {
      DynamicPropertyNotifier.process(findNonSeekableIdentifiers(predicates), IndexLookupUnfulfillableNotification, qg)
    }

    resultPlans
  }

  protected def findNonSeekableIdentifiers(predicates: Seq[Expression])(implicit context: LogicalPlanningContext) =
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

  protected def collectPlans(predicates: Seq[Expression], argumentIds: Set[IdName],
                             labelPredicateMap: Map[IdName, Set[HasLabels]],
                             hints: Set[Hint])
                            (implicit semanticTable: SemanticTable, context: LogicalPlanningContext): Seq[LogicalPlan] = {
    val arguments: Set[Variable] = argumentIds.map(n => Variable(n.name)(null))

    predicates.collect {
      // n.prop IN [ ... ]
      case predicate@AsPropertySeekable(seekable)
        if seekable.args.dependencies.forall(arguments) && !arguments(seekable.ident) =>
          producePlanFor(seekable.name, seekable.propertyKey, predicate,
          seekable.args.asQueryExpression, labelPredicateMap, hints, argumentIds)

      // n.prop STARTS WITH "prefix%..."
      case predicate@AsStringRangeSeekable(seekable) =>
          producePlanFor(seekable.name, seekable.propertyKey, PartialPredicate(seekable.expr, predicate),
          seekable.asQueryExpression, labelPredicateMap, hints, argumentIds)

      // n.prop <|<=|>|>= value
      case predicate@AsValueRangeSeekable(seekable) =>
          producePlanFor(seekable.name, seekable.propertyKeyName, predicate,
          seekable.asQueryExpression, labelPredicateMap, hints, argumentIds)
    }.flatten
  }

  private def producePlanFor(name: String, propertyKeyName: PropertyKeyName,
                             propertyPredicate: Expression, queryExpression: QueryExpression[Expression],
                             labelPredicateMap: Map[IdName, Set[HasLabels]],
                             hints: Set[Hint], argumentIds: Set[IdName])
                            (implicit semanticTable: SemanticTable, context: LogicalPlanningContext): Set[LogicalPlan] = {
    val idName = IdName(name)
    for (labelPredicate <- labelPredicateMap.getOrElse(idName, Set.empty);
         labelName <- labelPredicate.labels;
         indexDescriptor <- findIndexesFor(labelName.name, propertyKeyName.name);
         labelId <- labelName.id)
      yield {
        val propertyName = propertyKeyName.name
        val hint = hints.collectFirst {
          case hint @ UsingIndexHint(Variable(`name`), `labelName`, PropertyKeyName(`propertyName`)) => hint
        }
        val entryConstructor: (Seq[Expression]) => LogicalPlan =
          constructPlan(idName, LabelToken(labelName, labelId), PropertyKeyToken(propertyKeyName, propertyKeyName.id.head),
            queryExpression, hint, argumentIds)
        entryConstructor(Seq(propertyPredicate, labelPredicate))
      }
  }

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

object allIndexSeekLeafPlanner extends AbstractIndexSeekLeafPlanner {
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
    context.planContext.getIndexRule(label, property)
  }
}

//only looks at non-unique indexes
object nonUniqueindexSeekLeafPlanner extends AbstractIndexSeekLeafPlanner {
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
      case hint: LegacyIndexHint if !qg.argumentIds(IdName(hint.variable.name)) =>
        context.logicalPlanProducer.planLegacyHintSeek(IdName(hint.variable.name), hint, qg.argumentIds)
    }
  }
}
