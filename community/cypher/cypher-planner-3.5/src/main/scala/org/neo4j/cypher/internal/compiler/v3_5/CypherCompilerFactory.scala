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
package org.neo4j.cypher.internal.compiler.v3_5

import java.time.Clock

import org.neo4j.cypher.internal.compiler.v3_5.phases.{CompilerContext, LogicalPlanState}
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical._
import org.neo4j.cypher.internal.planner.v3_5.spi.CostBasedPlannerName
import org.opencypher.v9_0.frontend.phases.{ASTRewriter, Monitors, Transformer}
import org.opencypher.v9_0.rewriting.RewriterStepSequencer
import org.opencypher.v9_0.rewriting.rewriters.IfNoParameter

class CypherCompilerFactory[C <: CompilerContext, T <: Transformer[C, LogicalPlanState, LogicalPlanState]] {
  val monitorTag = "cypher3.3"

  def costBasedCompiler(config: CypherCompilerConfiguration,
                        clock: Clock,
                        monitors: Monitors,
                        rewriterSequencer: (String) => RewriterStepSequencer,
                        plannerName: Option[CostBasedPlannerName],
                        updateStrategy: Option[UpdateStrategy],
                        contextCreator: ContextCreator[C]): CypherCompiler[C] = {
    val rewriter = new ASTRewriter(rewriterSequencer, IfNoParameter, getDegreeRewriting = true)
    val metricsFactory = CachedMetricsFactory(SimpleMetricsFactory)
    val actualUpdateStrategy: UpdateStrategy = updateStrategy.getOrElse(defaultUpdateStrategy)
    CypherCompiler( rewriter, monitors, rewriterSequencer,
      metricsFactory, config, actualUpdateStrategy, clock, contextCreator)
  }
}
