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

import org.neo4j.cypher.internal.frontend.v2_3.ast.{HasLabels, Identifier}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.LazyLabel
import org.neo4j.cypher.internal.compiler.v2_3.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{IdName, LogicalPlan}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{CandidateGenerator, LogicalPlanningContext}

case object selectHasLabelWithJoin extends CandidateGenerator[LogicalPlan] {

  def apply(plan: LogicalPlan, queryGraph: QueryGraph)(implicit context: LogicalPlanningContext): Seq[LogicalPlan] =
    queryGraph.selections.unsolvedPredicates(plan).collect {
      case s@HasLabels(id: Identifier, Seq(labelName)) =>
        val labelScan = context.logicalPlanProducer.planNodeByLabelScan(IdName(id.name), LazyLabel(labelName)(context.semanticTable), Seq(s), None, Set.empty)
        context.logicalPlanProducer.planNodeHashJoin(Set(IdName(id.name)), plan, labelScan, Set.empty)
    }

}
