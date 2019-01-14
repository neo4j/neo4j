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

import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.{LeafPlanFromExpression, LeafPlanner, LeafPlansForVariable, LogicalPlanningContext}
import org.neo4j.cypher.internal.frontend.v3_4.ast.UsingScanHint
import org.neo4j.cypher.internal.ir.v3_4.QueryGraph
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.v3_4.expressions.{Expression, HasLabels, Variable}

object labelScanLeafPlanner extends LeafPlanner with LeafPlanFromExpression {

  override def producePlanFor(e: Expression, qg: QueryGraph, context: LogicalPlanningContext): Option[LeafPlansForVariable] = {
    e match {
      case labelPredicate@HasLabels(v@Variable(varName), labels) =>
        val id = varName
        if (qg.patternNodes(id) && !qg.argumentIds(id)) {
          val labelName = labels.head
          val hint = qg.hints.collectFirst {
            case hint@UsingScanHint(Variable(`varName`), `labelName`) => hint
          }
          val plan = context.logicalPlanProducer.planNodeByLabelScan(id, labelName, Seq(labelPredicate), hint, qg.argumentIds, context)
          Some(LeafPlansForVariable(varName, Set(plan)))
        } else
          None
      case _ =>
        None
    }
  }

  override def apply(qg: QueryGraph, context: LogicalPlanningContext, solveds: Solveds, cardinalities: Cardinalities) =
    qg.selections.flatPredicates.flatMap(e => producePlanFor(e, qg, context).toSeq.flatMap(_.plans))
}
