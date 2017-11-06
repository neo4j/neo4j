package org.neo4j.cypher.internal.runtime.vectorized.dispatcher

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.PipelineInformation
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.internal.util.v3_4.InternalException
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.values.virtual.MapValue

import scala.collection.mutable

class SingleThreadedExecutor(operators: Pipeline, queryContext: QueryContext, pipelineInformation: PipelineInformation,
                             params: MapValue) {

  private val MORSEL_SIZE = 100000

  def accept[E <: Exception](visitor: QueryResultVisitor[E]): Unit = {
    var leafOp = operators
    while (leafOp.dependency != NoDependencies) {
      leafOp = leafOp.dependency.pipeline
    }

    val jobStack: mutable.Stack[(Message, Pipeline)] = new mutable.Stack[(Message, Pipeline)]()
    val iteration = new Iteration(None)
    jobStack.push((StartLeafLoop(iteration), leafOp))
    val state = QueryState(params, visitor)
    val eagerAcc = new mutable.ArrayBuffer[Morsel]()
    var eagerRecipient: Pipeline = null
    do {
      if(eagerAcc.nonEmpty) {
        jobStack.push((StartLoopWithEagerData(eagerAcc.toVector, iteration), eagerRecipient))
        eagerAcc.clear()
        eagerRecipient = null
      }

      while (jobStack.nonEmpty) {
        val (message, pipeline) = jobStack.pop()
        val data = Morsel.create(pipeline.slotInformation, MORSEL_SIZE)
        val continuation = pipeline.operate(message, data, queryContext, state)
        if (continuation != EndOfLoop(iteration)) {
          jobStack.push((ContinueLoopWith(continuation), pipeline))
        }

        pipeline.parent match {
          case Some(mother) if mother.dependency.isInstanceOf[Eager] =>
            if(eagerRecipient != null && mother != eagerRecipient)
              throw new InternalException("oh noes")
            eagerRecipient = mother
            eagerAcc.append(data)

          case Some(mother) if mother.dependency.isInstanceOf[Lazy] =>
            jobStack.push((StartLoopWithSingleMorsel(data, iteration), pipeline.parent.get))

          case _ =>
        }
      }
    }
    while (eagerAcc.nonEmpty)
  }
}