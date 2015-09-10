package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{Effects, ReadsNodes}
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.{NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.NameId
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

case class CountStoreNodeAggregationPipe(ident: String, labelName: Option[String])(val estimatedCardinality: Option[Double] = None)
                                                           (implicit pipeMonitor: PipeMonitor) extends Pipe with RonjaPipe {

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val baseContext = state.initialContext.getOrElse(ExecutionContext.empty)
    val labelId = labelName match {
      case Some(name) => state.query.getLabelId(name)
      case _ => NameId.WILDCARD
    }
    val count = state.query.nodeCountByCountStore(labelId)
    Seq(baseContext.newWith1(s"count($ident)", count)).iterator
  }

  def exists(predicate: Pipe => Boolean): Boolean = predicate(this)

  def planDescriptionWithoutCardinality = PlanDescriptionImpl(this.id, "CountStoreNodeAggregation", NoChildren, Seq(), identifiers)

  def symbols = new SymbolTable(Map(ident -> CTInteger))

  override def monitor = pipeMonitor

  override def localEffects: Effects = Effects()

  def dup(sources: List[Pipe]): Pipe = {
    require(sources.isEmpty)
    this
  }

  def sources: Seq[Pipe] = Seq.empty

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))
}
