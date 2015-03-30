/**
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical

import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.{Index, LabelName, EstimatedRows}
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.{Id, InternalPlanDescription, NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._

object LogicalPlan2PlanDescription extends ((LogicalPlan, Map[LogicalPlan, Id]) => InternalPlanDescription) {
  override def apply(plan: LogicalPlan, idMap: Map[LogicalPlan, Id]): InternalPlanDescription = {
    val symbols = plan.availableSymbols.map(_.name)
    val planDescription = plan match {

      case AllNodesScan(IdName(id), arguments) =>
        PlanDescriptionImpl(id = idMap(plan), "AllNodesScan", NoChildren, Seq.empty, symbols)

      case NodeByLabelScan(IdName(id), label, arguments) =>
        PlanDescriptionImpl(id = idMap(plan), "NodeByLabelScan", NoChildren, Seq(LabelName(label.name)), symbols)

      case NodeByIdSeek(IdName(id), nodeIds, arguments) =>
        PlanDescriptionImpl(id = idMap(plan), "NodeByIdSeek", NoChildren, Seq(), symbols)

      case NodeIndexSeek(IdName(id), label, propKey, value, arguments) =>
        PlanDescriptionImpl(id = idMap(plan), "NodeIndexSeek", NoChildren, Seq(Index(label.name, propKey.name)), symbols)

      case NodeIndexUniqueSeek(IdName(id), label, propKey, value, arguments) =>
        PlanDescriptionImpl(id = idMap(plan), "NodeIndexUniqueSeek", NoChildren, Seq(Index(label.name, propKey.name)), symbols)

      case _ => ???
    }
    planDescription.addArgument(EstimatedRows(plan.solved.estimatedCardinality.amount))
  }
}
