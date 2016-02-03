package org.neo4j.cypher.internal.compiler.v3_0.spi

import org.neo4j.cypher.internal.compiler.v3_0.executionplan.{READ_WRITE, READ_ONLY}
import org.neo4j.cypher.internal.frontend.v3_0.spi.{ProcReadWrite, ProcReadOnly, ProcedureMode, ProcedureSignature}

object ProcedureModeSupport {
  implicit class ProcedureModeSupportWrapper(mode: ProcedureMode) {
    def callMode: ProcedureCallMode = mode match {
      case ProcReadOnly => LazyReadOnlyCallMode
      case ProcReadWrite => EagerReadWriteCallMode
    }
  }
}
