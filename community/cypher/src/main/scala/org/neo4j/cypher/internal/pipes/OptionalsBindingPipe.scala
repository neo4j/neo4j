package org.neo4j.cypher.internal.pipes

import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.symbols.SymbolTable
import org.neo4j.cypher.internal.commands.values.UnboundValue

class OptionalsBindingPipe(source: Pipe) extends PipeWithSource(source) {

  val symbols: SymbolTable = source.symbols

  override def executionPlanDescription = source.executionPlanDescription.andThen(this, "OptionalsBinding")

  def throwIfSymbolsMissing(symbols: SymbolTable) {}

  protected def internalCreateResults(in: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    in.map { (m: ExecutionContext) => bindExecutionContext(m) }

  private def bindExecutionContext(ctx: ExecutionContext): ExecutionContext = {
    ctx.newFrom( ctx.mapValues(bindValue(_)) )
  }

  private def bindValue(v: Any): Any = v match {
    case UnboundValue => null
    case _            => v
  }
}