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

import org.neo4j.cypher.internal.compiler.v2_3.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{LeafPlanner, LogicalPlanningContext}
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.notification.IndexLookupUnfulfillableNotification

object indexScanLeafPlanner extends LeafPlanner {
  override def apply(qg: QueryGraph)(implicit context: LogicalPlanningContext): Seq[LogicalPlan] = {
    implicit val semanticTable = context.semanticTable
    val predicates: Seq[Expression] = qg.selections.flatPredicates
    val labelPredicates: Map[IdName, Set[HasLabels]] = qg.selections.labelPredicates

    val resultPlans = predicates.collect {
      // MATCH (n:User) WHERE exists(n.prop) RETURN n
      case predicate@AsPropertyScannable(scannable) =>
        val name = scannable.name
        val propertyKey = scannable.propertyKey
        val propertyKeyName = propertyKey.name

        val idName = IdName(name)
        for (labelPredicate <- labelPredicates.getOrElse(idName, Set.empty);
             labelName <- labelPredicate.labels;
             indexDescriptor <- findIndexesFor(labelName.name, propertyKeyName);
             labelId <- labelName.id)
          yield {
            val hint = qg.hints.collectFirst {
              case hint@UsingIndexHint(Identifier(`name`), `labelName`, PropertyKeyName(`propertyKeyName`)) => hint
            }
            context.logicalPlanProducer.planNodeIndexScan(idName, LabelToken(labelName, labelId),
              PropertyKeyToken(propertyKey, propertyKey.id.head), Seq(scannable.expr, labelPredicate),
              hint, qg.argumentIds)
          }

    }.flatten

    if (resultPlans.isEmpty) {
      DynamicPropertyNotifier.process(findNonScannableIdentifiers(predicates), IndexLookupUnfulfillableNotification, qg)
    }

    resultPlans
  }

  private def findNonScannableIdentifiers(predicates: Seq[Expression])(implicit context: LogicalPlanningContext) =
    predicates.flatMap {
      case predicate@AsDynamicPropertyNonScannable(nonScannableId) if context.semanticTable.isNode(nonScannableId) =>
        Some(nonScannableId)
      case predicate@AsStringRangeNonSeekable(nonScannableId) if context.semanticTable.isNode(nonScannableId) =>
        Some(nonScannableId)
      case _ =>
        None
    }.toSet

  private def findIndexesFor(label: String, property: String)(implicit context: LogicalPlanningContext) =
    context.planContext.getIndexRule(label, property) orElse context.planContext.getUniqueIndexRule(label, property)
}
