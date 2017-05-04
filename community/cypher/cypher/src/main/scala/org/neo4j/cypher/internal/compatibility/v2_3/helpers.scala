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
package org.neo4j.cypher.internal.compatibility.v2_3

import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compiler.v2_3
import org.neo4j.cypher.internal.compiler.v2_3.CompilationPhaseTracer.CompilationPhaseEvent
import org.neo4j.cypher.internal.compiler.v2_3.{CypherCompilerConfiguration => CypherCompilerConfiguration2_3}
import org.neo4j.cypher.internal.frontend.v2_3.{InputPosition => InputPosition2_3}
import org.neo4j.cypher.internal.frontend.v3_3.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, phases}
import org.neo4j.kernel.impl.query.{QueryExecutionMonitor, TransactionalContext}

object helpers {
  implicit def monitorFailure(t: Throwable)(implicit monitor: QueryExecutionMonitor, tc: TransactionalContext): Unit = {
    monitor.endFailure(tc.executingQuery(), t)
  }

  def as2_3(config: CypherCompilerConfiguration) = CypherCompilerConfiguration2_3(config.queryCacheSize,
    config.statsDivergenceThreshold, config.queryPlanTTL, config.useErrorsOverWarnings,
    config.idpMaxTableSize, config.idpIterationDuration, config.nonIndexedLabelWarningThreshold)

  /** This is awful but needed until 2.3 is updated no to send in the tracer here */
  def as2_3(tracer: CompilationPhaseTracer): v2_3.CompilationPhaseTracer = {
    new v2_3.CompilationPhaseTracer {
      override def beginPhase(phase: v2_3.CompilationPhaseTracer.CompilationPhase) = {
        val wrappedPhase =
          if (phase == v2_3.CompilationPhaseTracer.CompilationPhase.AST_REWRITE)
            CompilationPhaseTracer.CompilationPhase.AST_REWRITE
          else if (phase == v2_3.CompilationPhaseTracer.CompilationPhase
            .CODE_GENERATION)
            phases.CompilationPhaseTracer.CompilationPhase.CODE_GENERATION
          else if (phase == v2_3.CompilationPhaseTracer.CompilationPhase
            .LOGICAL_PLANNING)
            phases.CompilationPhaseTracer.CompilationPhase.LOGICAL_PLANNING
          else if (phase == v2_3.CompilationPhaseTracer.CompilationPhase.PARSING)
            phases.CompilationPhaseTracer.CompilationPhase.PARSING
          else if (phase == v2_3.CompilationPhaseTracer.CompilationPhase
            .PIPE_BUILDING)
            phases.CompilationPhaseTracer.CompilationPhase.PIPE_BUILDING
          else if (phase == v2_3.CompilationPhaseTracer.CompilationPhase
            .SEMANTIC_CHECK)
            phases.CompilationPhaseTracer.CompilationPhase.SEMANTIC_CHECK
          else throw new InternalException(s"Cannot handle $phase in 2.3")

        val wrappedEvent = tracer.beginPhase(wrappedPhase)

        new CompilationPhaseEvent {
          override def close(): Unit = wrappedEvent.close()
        }
      }
    }
  }

  def as2_3(pos: InputPosition): InputPosition2_3 = InputPosition2_3(pos.offset, pos.line, pos.column)

}
