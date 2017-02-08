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
package org.neo4j.cypher.internal.compiler.v3_1.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_1.commands.QueryExpression
import org.neo4j.cypher.internal.compiler.v3_1.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.plans.{IdName, LogicalPlan, NodeUniqueIndexSeek}
import org.neo4j.cypher.internal.compiler.v3_1.spi.SchemaTypes.IndexDescriptor
import org.neo4j.cypher.internal.frontend.v3_1.ast._

/*
 * Plan the following type of plan
 *
 *  - as := AssertSame
 *  - ui := NodeUniqueIndexSeek)
 *
 *       (as)
 *       /  \
 *    (as) (ui2)
 *    /  \
 * (ui1) (ui2)
 */
object mergeUniqueIndexSeekLeafPlanner extends AbstractIndexSeekLeafPlanner {

  override def apply(qg: QueryGraph)(implicit context: LogicalPlanningContext): Seq[LogicalPlan] = {

    implicit val semanticTable = context.semanticTable
    val predicates: Seq[Expression] = qg.selections.flatPredicates
    val labelPredicateMap: Map[IdName, Set[HasLabels]] = qg.selections.labelPredicates


    val allPlans = collectPlans(predicates, qg.argumentIds, labelPredicateMap, qg.hints)
    val resultPlans = allPlans.flatMap(_._2)
    val nodes = allPlans.map(_._1).toSet

    if (resultPlans.size < 2 || nodes.size != 1) resultPlans
    else Seq(resultPlans.reduce(context.logicalPlanProducer.planAssertSameNode(IdName(nodes.head), _, _)))
  }

  override def constructPlan(idName: IdName,
                              label: LabelToken,
                              propertyKey: PropertyKeyToken,
                              valueExpr: QueryExpression[Expression],
                              hint: Option[UsingIndexHint],
                              argumentIds: Set[IdName])
                             (implicit context: LogicalPlanningContext): (Seq[Expression]) => NodeUniqueIndexSeek =
    (predicates: Seq[Expression]) =>
      context.logicalPlanProducer.planNodeUniqueIndexSeek(idName, label, propertyKey, valueExpr, predicates, hint, argumentIds)

  override def findIndexesFor(label: String, property: String)(implicit context: LogicalPlanningContext): Option[IndexDescriptor] =
    context.planContext.getUniqueIndexRule(label, property)
}
