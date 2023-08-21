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
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.MultiNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeekLeafPlan
import org.neo4j.cypher.internal.runtime.spec.RewritingRuntimeTest
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp

/**
 * This test attempts to make sure that multi node index seek stays compatible
 * with node index seek. Since node index seeks can be rewritten to multi node
 * index seeks it's important that the two implementations stay compatible.
 */
trait MultiNodeIndexSeekCompatibilityTestRewriter[CONTEXT <: RuntimeContext] extends RewritingRuntimeTest[CONTEXT] {
  self: RuntimeTestSuite[CONTEXT] =>

  /**
   * Rewrites all node index seeks to multi node index seek.
   */
  override def rewriter(logicalQuery: LogicalQuery): Rewriter = {
    bottomUp(
      Rewriter.lift {
        case plan: NodeIndexSeekLeafPlan => MultiNodeIndexSeek(Array(plan))(logicalQuery.idGen)
      },
      stopper
    )
  }

  private def stopper(a: AnyRef): Boolean = a match {
    case _: AssertSameNode     => true
    case _: MultiNodeIndexSeek => true
    case _: LogicalPlan        => false
    case _                     => true
  }
}
