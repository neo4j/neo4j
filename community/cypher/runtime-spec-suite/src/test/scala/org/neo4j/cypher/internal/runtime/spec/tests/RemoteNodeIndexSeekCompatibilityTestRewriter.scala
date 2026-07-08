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
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.RemoteNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.RemoteNodeUniqueIndexSeek
import org.neo4j.cypher.internal.runtime.spec.RewritingRuntimeTest
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.RewriterStopper
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.bottomUp

/**
 * Rewrites every [[NodeIndexSeek]] in the logical plan into a [[RemoteNodeIndexSeek]],
 * and every [[NodeUniqueIndexSeek]] in a read-only logical plan into a [[RemoteNodeUniqueIndexSeek]].
 */
trait RemoteNodeIndexSeekCompatibilityTestRewriter[CONTEXT <: RuntimeContext] extends RewritingRuntimeTest[CONTEXT] {
  self: RuntimeTestSuite[CONTEXT] =>

  override def rewriter(logicalQuery: LogicalQuery): Rewriter = {
    val readOnly = logicalQuery.logicalPlan.readOnly
    bottomUp(
      Rewriter.lift {
        case plan: NodeIndexSeek =>
          RemoteNodeIndexSeek(
            plan.idName,
            plan.label,
            plan.properties,
            plan.valueExpr,
            plan.argumentIds,
            plan.indexOrder,
            plan.indexType,
            plan.supportPartitionedScan
          )(SameId(plan.id))
        case plan: NodeUniqueIndexSeek if readOnly =>
          RemoteNodeUniqueIndexSeek(
            plan.idName,
            plan.label,
            plan.properties,
            plan.valueExpr,
            plan.argumentIds,
            plan.indexOrder,
            plan.indexType,
            plan.supportPartitionedScan
          )(SameId(plan.id))
      },
      stopper
    )
  }

  private val stopper: RewriterStopper = {
    case _: LogicalPlan => false
    case _              => true
  }
}
