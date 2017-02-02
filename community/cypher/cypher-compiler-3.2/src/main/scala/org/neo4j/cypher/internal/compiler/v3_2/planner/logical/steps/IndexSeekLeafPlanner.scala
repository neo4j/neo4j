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
package org.neo4j.cypher.internal.compiler.v3_2.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_2.commands.{QueryExpression, SingleQueryExpression}
import org.neo4j.cypher.internal.compiler.v3_2.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.LeafPlansForVariable.maybeLeafPlans
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical._
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_2.SemanticTable
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.notification.IndexLookupUnfulfillableNotification
import org.neo4j.cypher.internal.ir.v3_2.IdName
import org.neo4j.cypher.internal.compiler.v3_2.IndexDescriptor

abstract class AbstractIndexSeekLeafPlanner extends LeafPlanner with LeafPlanFromExpression {

  override def producePlanFor(e: Expression, qg: QueryGraph)(implicit context: LogicalPlanningContext): Option[LeafPlansForVariable] = {
    val arguments: Set[Variable] = qg.argumentIds.map(n => Variable(n.name)(null))
    val labelPredicateMap: Map[IdName, Set[HasLabels]] = qg.selections.labelPredicates
    val hints = qg.hints

    implicit val semanticTable = context.semanticTable

    e match {
      case predicate@AsPropertySeekable(seekable: PropertySeekable)
        if seekable.args.dependencies.forall(arguments) && !arguments(seekable.ident) =>
        val plans = producePlansFor(seekable.name, seekable.propertyKey, predicate,
          seekable.args.asQueryExpression, labelPredicateMap, hints, qg.argumentIds)
        maybeLeafPlans(seekable.name, plans)

      // ... = n.prop
      // In some rare cases, we can't rewrite these predicates cleanly,
      // and so planning needs to search for these cases explicitly
      case predicate@Equals(a, Property(seekable@Variable(name), propKeyName))
        if a.dependencies.forall(arguments) && !arguments(seekable) =>
        val expr = SingleQueryExpression(a)
        val plans = producePlansFor(seekable.name, propKeyName, predicate, expr, labelPredicateMap, hints, qg.argumentIds)
        maybeLeafPlans(seekable.name, plans)

      // n.prop STARTS WITH "prefix%..."
      case predicate@AsStringRangeSeekable(seekable) =>
        val plans = producePlansFor(seekable.name, seekable.propertyKey, PartialPredicate(seekable.expr, predicate),
          seekable.asQueryExpression, labelPredicateMap, hints, qg.argumentIds)
        maybeLeafPlans(seekable.name, plans)

      // n.prop <|<=|>|>= value
      case predicate@AsValueRangeSeekable(seekable) =>
        val plans = producePlansFor(seekable.name, seekable.propertyKeyName, predicate, seekable.asQueryExpression,
          labelPredicateMap, hints, qg.argumentIds)
        maybeLeafPlans(seekable.name, plans)

      case _ =>
        None
    }
  }

  override def apply(qg: QueryGraph)(implicit context: LogicalPlanningContext): Seq[LogicalPlan] = {
    val resultPlans = qg.selections.flatPredicates.flatMap {
      e => producePlanFor(e, qg).toSeq.flatMap(_.plans)
    }

    if (resultPlans.isEmpty) {
      DynamicPropertyNotifier.process(findNonSeekableIdentifiers(qg.selections.flatPredicates), IndexLookupUnfulfillableNotification, qg)
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
                            (implicit semanticTable: SemanticTable, context: LogicalPlanningContext): Seq[(String, Set[LogicalPlan])]  = {
    val arguments: Set[Variable] = argumentIds.map(n => Variable(n.name)(null))

    predicates.collect {
      // n.prop IN [ ... ]
      case predicate@AsPropertySeekable(seekable: PropertySeekable)
        if seekable.args.dependencies.forall(arguments) && !arguments(seekable.ident) =>
        (seekable.name, producePlansFor(seekable.name, seekable.propertyKey, predicate,
          seekable.args.asQueryExpression, labelPredicateMap, hints, argumentIds))

      // ... = n.prop
      // In some rare cases, we can't rewrite these predicates cleanly,
      // and so planning needs to search for these cases explicitly
      case predicate@Equals(a, Property(seekable@Variable(name), propKeyName))
        if a.dependencies.forall(arguments) && !arguments(seekable) =>
        val expr = SingleQueryExpression(a)
        (seekable.name, producePlansFor(seekable.name, propKeyName, predicate,
          expr, labelPredicateMap, hints, argumentIds))

      // n.prop STARTS WITH "prefix%..."
      case predicate@AsStringRangeSeekable(seekable) =>
        (seekable.name, producePlansFor(seekable.name, seekable.propertyKey, PartialPredicate(seekable.expr, predicate),
          seekable.asQueryExpression, labelPredicateMap, hints, argumentIds))

      // n.prop <|<=|>|>= value
      case predicate@AsValueRangeSeekable(seekable) =>
        (seekable.name, producePlansFor(seekable.name, seekable.propertyKeyName, predicate,
          seekable.asQueryExpression, labelPredicateMap, hints, argumentIds))
    }
  }

  private def producePlansFor(name: String, propertyKeyName: PropertyKeyName,
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

