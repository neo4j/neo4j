package org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionContext, PipelineInformation}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.PrimitiveExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.Id

case class SingleRowRegisterPipe(pipelineInformation: PipelineInformation)(val id: Id = new Id) extends Pipe {


  def internalCreateResults(state: QueryState) =
    Iterator(PrimitiveExecutionContext(pipelineInformation))
}
