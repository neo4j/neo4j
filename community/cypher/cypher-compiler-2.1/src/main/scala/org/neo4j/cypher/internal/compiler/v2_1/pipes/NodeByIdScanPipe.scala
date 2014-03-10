package org.neo4j.cypher.internal.compiler.v2_1.pipes

import org.neo4j.cypher.internal.compiler.v2_1.{symbols, PlanDescription, ExecutionContext, LabelId}
import symbols._
import org.neo4j.cypher.internal.compiler.v2_1.commands.expressions.{NumericHelper, Expression}

case class NodeByIdScanPipe(ident: String, nodeIdExpr: Expression) extends Pipe with NumericHelper {

  override protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val nodeId = asLongEntityId(nodeIdExpr(ExecutionContext.empty)(state))
    Iterator(ExecutionContext.from(ident -> state.query.nodeOps.getById(nodeId)))
  }

  override def exists(predicate: Pipe => Boolean): Boolean = predicate(this)

  override def executionPlanDescription: PlanDescription = ???

  override def symbols: SymbolTable = new SymbolTable(Map(ident -> CTNode))
}
