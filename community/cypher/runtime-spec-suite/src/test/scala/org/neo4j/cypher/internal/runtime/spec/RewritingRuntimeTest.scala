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
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.options.CypherDebugOptions
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSupport.WorkloadMode
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanCombinationRewriter.TestPlanCombinationRewriterHint
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.logging.InternalLogProvider

/**
 * This trait can be mix-ined to [[RuntimeTestSuite]]s to enable logical plan rewrites before execution.
 */
trait RewritingRuntimeTest[CONTEXT <: RuntimeContext] {
  self: RuntimeTestSuite[CONTEXT] =>

  def rewriter(logicalQuery: LogicalQuery): Rewriter

  override protected def createRuntimeTestSupport(
    graphDb: GraphDatabaseService,
    edition: Edition[CONTEXT],
    runtime: CypherRuntime[CONTEXT],
    workloadMode: WorkloadMode,
    logProvider: InternalLogProvider
  ): RuntimeTestSupport[CONTEXT] = {
    new RewritingRuntimeTestSupport(graphDb, edition, runtime, workloadMode, logProvider, debugOptions)
  }

  class RewritingRuntimeTestSupport(
    graphDb: GraphDatabaseService,
    edition: Edition[CONTEXT],
    runtime: CypherRuntime[CONTEXT],
    workloadMode: WorkloadMode,
    logProvider: InternalLogProvider,
    // No default value: a default getter would synthesize a companion object, which the
    // Scala 2.13 TASTy reader cannot resolve when nested in a Scala 3 trait
    debugOptions: CypherDebugOptions
  ) extends RuntimeTestSupport[CONTEXT](graphDb, edition, runtime, workloadMode, logProvider, debugOptions) {

    override protected[spec] def rewriteLogicalQuery(
      logicalQuery: LogicalQuery,
      anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
      testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint]
    ): LogicalQuery = {
      val rewrittenPlan = logicalQuery.logicalPlan.endoRewrite(rewriter(logicalQuery))
      val afterTraitRewrite = logicalQuery.copy(logicalPlan = rewrittenPlan)
      super.rewriteLogicalQuery(afterTraitRewrite, anonymousVariableNameGenerator, testPlanCombinationRewriterHints)
    }
  }
}
