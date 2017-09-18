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

import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.{LeafPlanFromExpression, LeafPlanner, LeafPlansForVariable, LogicalPlanningContext}
import org.neo4j.cypher.internal.frontend.v3_4.ast.{Expression, HasLabels, UsingScanHint, Variable}
import org.neo4j.cypher.internal.ir.v3_4.{IdName, QueryGraph}

object labelScanLeafPlanner extends LeafPlanner with LeafPlanFromExpression {

  override def producePlanFor(e: Expression, qg: QueryGraph)(implicit context: LogicalPlanningContext): Option[LeafPlansForVariable] = {
    e match {
      case labelPredicate@HasLabels(v@Variable(varName), labels) =>
        val id = IdName(varName)
        if (qg.patternNodes(id) && !qg.argumentIds(id)) {
          val labelName = labels.head
          val hint = qg.hints.collectFirst {
            case hint@UsingScanHint(Variable(`varName`), `labelName`) => hint
          }
          val plan = context.logicalPlanProducer.planNodeByLabelScan(id, labelName, Seq(labelPredicate), hint, qg.argumentIds)
          Some(LeafPlansForVariable(IdName(varName), Set(plan)))
        } else
          None
      case _ =>
        None
    }
  }

  override def apply(qg: QueryGraph)(implicit context: LogicalPlanningContext) =
    qg.selections.flatPredicates.flatMap(e => producePlanFor(e, qg).toSeq.flatMap(_.plans))
}
