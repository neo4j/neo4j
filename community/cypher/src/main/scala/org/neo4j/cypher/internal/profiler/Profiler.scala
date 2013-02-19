package org.neo4j.cypher.internal.profiler

import org.neo4j.cypher.internal.pipes.{QueryState, Pipe, PipeDecorator}
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.spi.{QueryContext, DelegatingQueryContext}
import collection.mutable
import org.neo4j.cypher.{ProfilerStatisticsNotReadyException, PlanDescription}

class ProfilerDecorator extends PipeDecorator {

  val contextStats: mutable.Map[Pipe, ProfilingQueryContext] = mutable.Map.empty
  val iterStats: mutable.Map[Pipe, ProfilingIterator] = mutable.Map.empty

  def decorate(pipe: Pipe, iter: Iterator[ExecutionContext]): Iterator[ExecutionContext] = {
    val resultIter = new ProfilingIterator(iter)

    assert(!iterStats.contains(pipe), "Can't profile the same iterator twice")

    iterStats(pipe) = resultIter

    resultIter
  }

  def decorate(pipe: Pipe, state: QueryState): QueryState = {
    val context = new ProfilingQueryContext(state.query)

    assert(!contextStats.contains(pipe), "Can't profile the same pipe twice")

    contextStats(pipe) = context
    state.copy(query = context)
  }

  def decorate(plan: PlanDescription): PlanDescription = plan.mapArgs {
    p: PlanDescription =>
      var newArgs = p.args

      if(iterStats(p.pipe).nonEmpty)
        throw new ProfilerStatisticsNotReadyException()

      newArgs
  }
}

class ProfilingQueryContext(inner: QueryContext) extends DelegatingQueryContext(inner) {

}

class ProfilingIterator(inner: Iterator[ExecutionContext]) extends Iterator[ExecutionContext] {
  def hasNext: Boolean = inner.hasNext

  def next(): ExecutionContext = inner.next()
}