package org.neo4j.cypher.internal.compatibility.v3_1

import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compiler.v3_1
import org.neo4j.cypher.internal.compiler.v3_1.CompilationPhaseTracer.{CompilationPhaseEvent, CompilationPhase => v3_1Phase}
import org.neo4j.cypher.internal.compiler.v3_1.{CypherCompilerConfiguration => CypherCompilerConfiguration3_1}
import org.neo4j.cypher.internal.compiler.v3_2.CompilationPhaseTracer.{CompilationPhase => v3_2Phase}
import org.neo4j.cypher.internal.compiler.v3_2.{CompilationPhaseTracer, CypherCompilerConfiguration}
import org.neo4j.cypher.internal.frontend.v3_1.{InputPosition => InputPosition3_1}
import org.neo4j.cypher.internal.frontend.v3_2.InputPosition
import org.neo4j.kernel.impl.query.{QueryExecutionMonitor, TransactionalContext}

object helpers {
  implicit def monitorFailure(t: Throwable)(implicit monitor: QueryExecutionMonitor, tc: TransactionalContext): Unit = {
    monitor.endFailure(tc.executingQuery(), t)
  }
  def as3_1(config: CypherCompilerConfiguration) =
    CypherCompilerConfiguration3_1(config.queryCacheSize,
      config.statsDivergenceThreshold,
      config.queryPlanTTL,
      config.useErrorsOverWarnings,
      config.idpMaxTableSize,
      config.idpIterationDuration,
      config.errorIfShortestPathFallbackUsedAtRuntime,
      config.nonIndexedLabelWarningThreshold)

  /** This is awful but needed until 3_0 is updated no to send in the tracer here */
  def as3_1(tracer: CompilationPhaseTracer): v3_1.CompilationPhaseTracer = {
    new v3_1.CompilationPhaseTracer {
      override def beginPhase(phase: v3_1.CompilationPhaseTracer.CompilationPhase) = {
        val wrappedPhase = phase match {
          case v3_1Phase.AST_REWRITE => v3_2Phase.AST_REWRITE
          case v3_1Phase.CODE_GENERATION => v3_2Phase.CODE_GENERATION
          case v3_1Phase.LOGICAL_PLANNING => v3_2Phase.LOGICAL_PLANNING
          case v3_1Phase.PARSING => v3_2Phase.PARSING
          case v3_1Phase.PIPE_BUILDING => v3_2Phase.PIPE_BUILDING
          case v3_1Phase.SEMANTIC_CHECK => v3_2Phase.SEMANTIC_CHECK
          case _ => throw new InternalException(s"Cannot handle $phase in 3.2")
        }

        val wrappedEvent = tracer.beginPhase(wrappedPhase)

        new CompilationPhaseEvent {
          override def close(): Unit = wrappedEvent.close()
        }
      }
    }
  }

  def as3_1(pos: InputPosition): InputPosition3_1 = InputPosition3_1(pos.offset, pos.line, pos.column)

}
