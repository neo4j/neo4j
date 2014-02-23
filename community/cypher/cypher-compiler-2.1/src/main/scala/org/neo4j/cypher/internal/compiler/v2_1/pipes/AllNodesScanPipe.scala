package org.neo4j.cypher.internal.compiler.v2_1.pipes

import org.neo4j.cypher.internal.compiler.v2_1.{symbols, PlanDescription, ExecutionContext}
import symbols._

case class AllNodesScanPipe(id: String) extends Pipe {
  override protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    state.query.nodeOps.all.map(n => ExecutionContext.from(id -> n))
  }

  override def exists(pred: (Pipe) => Boolean): Boolean = pred(this)

  override def executionPlanDescription: PlanDescription = ???

  override def symbols: SymbolTable = new SymbolTable(Map(id -> CTNode))
}