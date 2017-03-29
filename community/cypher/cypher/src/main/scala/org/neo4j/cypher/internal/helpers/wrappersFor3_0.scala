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
package org.neo4j.cypher.internal.helpers

import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compiler.v3_0
import org.neo4j.cypher.internal.compiler.v3_0.CompilationPhaseTracer.CompilationPhaseEvent
import org.neo4j.cypher.internal.compiler.v3_0.{CypherCompilerConfiguration => CypherCompilerConfiguration3_0}
import org.neo4j.cypher.internal.compiler.v3_1.{CompilationPhaseTracer, CypherCompilerConfiguration}
import org.neo4j.cypher.internal.frontend.v3_0.{InputPosition => InputPosition3_0}
import org.neo4j.cypher.internal.frontend.v3_1.InputPosition

/**
  * Contains necessary wrappers for supporting 3_0 in 3.1
  */
object wrappersFor3_0 {

  def as3_0(config: CypherCompilerConfiguration) =
    CypherCompilerConfiguration3_0(config.queryCacheSize,
                                   config.statsDivergenceThreshold,
                                   config.queryPlanTTL,
                                   config.useErrorsOverWarnings,
                                   config.idpMaxTableSize,
                                   config.idpIterationDuration,
                                   config.errorIfShortestPathFallbackUsedAtRuntime,
                                   config.errorIfShortestPathHasCommonNodesAtRuntime,
                                   config.legacyCsvQuoteEscaping,
                                   config.nonIndexedLabelWarningThreshold)

  /** This is awful but needed until 3_0 is updated no to send in the tracer here */
  def as3_0(tracer: CompilationPhaseTracer): v3_0.CompilationPhaseTracer = {
    new v3_0.CompilationPhaseTracer {
      override def beginPhase(phase: v3_0.CompilationPhaseTracer.CompilationPhase) = {
        val wrappedPhase =
          if (phase == v3_0.CompilationPhaseTracer.CompilationPhase.AST_REWRITE)
            org.neo4j.cypher.internal.compiler.v3_1.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
          else if (phase == v3_0.CompilationPhaseTracer.CompilationPhase
            .CODE_GENERATION)
            org.neo4j.cypher.internal.compiler.v3_1.CompilationPhaseTracer.CompilationPhase.CODE_GENERATION
          else if (phase == v3_0.CompilationPhaseTracer.CompilationPhase
            .LOGICAL_PLANNING)
            org.neo4j.cypher.internal.compiler.v3_1.CompilationPhaseTracer.CompilationPhase.LOGICAL_PLANNING
          else if (phase == v3_0.CompilationPhaseTracer.CompilationPhase.PARSING)
            org.neo4j.cypher.internal.compiler.v3_1.CompilationPhaseTracer.CompilationPhase.PARSING
          else if (phase == v3_0.CompilationPhaseTracer.CompilationPhase
            .PIPE_BUILDING)
            org.neo4j.cypher.internal.compiler.v3_1.CompilationPhaseTracer.CompilationPhase.PIPE_BUILDING
          else if (phase == v3_0.CompilationPhaseTracer.CompilationPhase
            .SEMANTIC_CHECK)
            org.neo4j.cypher.internal.compiler.v3_1.CompilationPhaseTracer.CompilationPhase.SEMANTIC_CHECK
          else throw new InternalException(s"Cannot handle $phase in 2.3")

        val wrappedEvent = tracer.beginPhase(wrappedPhase)

        new CompilationPhaseEvent {
          override def close(): Unit = wrappedEvent.close()
        }
      }
    }
  }

  def as3_0(pos: InputPosition): InputPosition3_0 = InputPosition3_0(pos.offset, pos.line, pos.column)
}
