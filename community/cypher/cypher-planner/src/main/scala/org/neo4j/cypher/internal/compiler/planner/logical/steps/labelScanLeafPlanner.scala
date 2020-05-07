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

import org.neo4j.cypher.internal.ast.UsingScanHint
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanFromExpression
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlansForVariable
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrdering
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

object labelScanLeafPlanner extends LeafPlanner with LeafPlanFromExpression {

  override def producePlanFor(e: Expression, qg: QueryGraph, interestingOrder: InterestingOrder, context: LogicalPlanningContext): Option[LeafPlansForVariable] = {
    e match {
      case labelPredicate@HasLabels(variable@Variable(varName), labels) =>
        if (qg.patternNodes(varName) && !qg.argumentIds(varName)) {
          val labelName = labels.head
          val hint = qg.hints.collectFirst {
            case hint@UsingScanHint(`variable`, `labelName`) => hint
          }
          val providedOrder = ResultOrdering.providedOrderForLabelScan(interestingOrder, variable)
          val plan = context.logicalPlanProducer.planNodeByLabelScan(variable, labelName, Seq(labelPredicate), hint, qg.argumentIds, providedOrder, context)
          Some(LeafPlansForVariable(varName, Set(plan)))
        } else
          None
      case _ =>
        None
    }
  }

  override def apply(qg: QueryGraph, interestingOrder: InterestingOrder, context: LogicalPlanningContext): Seq[LogicalPlan] =
    qg.selections.flatPredicates.flatMap(e => producePlanFor(e, qg, interestingOrder, context).toSeq.flatMap(_.plans))
}
