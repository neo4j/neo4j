/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.slotted

import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.BFSPruningVarExpand
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.LogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.PruningVarExpand
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes
import org.neo4j.cypher.internal.physicalplanning.PipelineBreakingPolicy

object SlottedPipelineBreakingPolicy extends PipelineBreakingPolicy {

  // When making changes here, please keep in mind that slot discarding (SlottedRow.compact)
  // relies on breaks to function correctly.
  override def breakOn(lp: LogicalPlan, applyPlans: PhysicalPlanningAttributes.ApplyPlans): Boolean = {

    lp match {
      // leaf operators
      case _: LogicalLeafPlan => true

      // 1 child operators
      case _: Aggregation |
        _: Expand |
        _: OptionalExpand |
        _: VarExpand |
        _: PruningVarExpand |
        _: BFSPruningVarExpand |
        _: FindShortestPaths |
        _: UnwindCollection |
        _: Eager
        // _: ProjectEndpoints | This is cardinality increasing (if undirected) but doesn't break currently
        // _: LoadCSV | This is cardinality increasing but doesn't break currently
        // _: ProcedureCall | This is cardinality increasing but doesn't break currently
        //                    Also, if the procedure is void it cannot increase cardinality.
        // _: FindShortestPaths | This is cardinality increasing but doesn't break currently
        => true

      // 2 child operators
      case _: CartesianProduct |
        _: RightOuterHashJoin |
        _: LeftOuterHashJoin |
        _: NodeHashJoin |
        _: ValueHashJoin |
        _: Union => true

      case _ =>
        false
    }
  }
}
