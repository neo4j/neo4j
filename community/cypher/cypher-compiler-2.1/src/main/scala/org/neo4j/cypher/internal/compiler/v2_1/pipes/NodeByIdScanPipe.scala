package org.neo4j.cypher.internal.compiler.v2_1.pipes

import org.neo4j.cypher.internal.compiler.v2_1.{symbols, PlanDescription, ExecutionContext, LabelId}
import symbols._

class NodeByIdScanPipe(ident: String, nodeId: Long) extends Pipe {

  override protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] =
    Iterator(ExecutionContext.from(ident -> state.query.nodeOps.getById(nodeId)))

  override def exists(predicate: Pipe => Boolean): Boolean = predicate(this)

  override def executionPlanDescription: PlanDescription = ???

  override def symbols: SymbolTable = new SymbolTable(Map(ident -> CTNode))
}
