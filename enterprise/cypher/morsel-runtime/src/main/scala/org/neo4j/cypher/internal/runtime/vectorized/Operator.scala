/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.vectorized

import java.util
import java.util.concurrent.atomic.AtomicInteger

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.parallel.Task
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.values.virtual.{MapValue, VirtualValues}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

trait Operator {
  def init(context: QueryContext, state: QueryState, inputMorsel: MorselExecutionContext): ContinuableOperatorTask
}

trait ReduceOperator {
  def init(context: QueryContext, state: QueryState, inputMorsels: Seq[MorselExecutionContext]): ContinuableOperatorTask
}

trait StatelessOperator extends OperatorTask

trait OperatorTask {
  def operate(data: MorselExecutionContext,
              context: QueryContext,
              state: QueryState): Unit
}

trait ContinuableOperatorTask extends OperatorTask {
  def canContinue: Boolean
}

trait ReduceCollector {

  def acceptMorsel(inputMorsel: MorselExecutionContext): Unit

  def produceTaskScheduled(task: String): Unit

  def produceTaskCompleted(task: String, context: QueryContext, state: QueryState): Option[Task]
}

class ReducePipeline(start: ReduceOperator,
                     override val slots: SlotConfiguration,
                     override val upstream: Option[Pipeline]) extends Pipeline {

  override def reduceForUpstream(downstreamReduce: Option[ReducePipeline]): Option[ReducePipeline] = Some(this)

  override def toString: String = {
    val x = (start +: operators).map(x => x.getClass.getSimpleName)
    s"ReducePipeline(${x.mkString(",")})"
  }

  override def acceptMorsel(inputMorsel: MorselExecutionContext, context: QueryContext, state: QueryState): Seq[Task] = {

    state.reduceCollector.get.acceptMorsel(inputMorsel)
    Nil
  }

  def init() = new Collector

  class Collector() extends ReduceCollector {

    private val eagerData = new java.util.concurrent.ConcurrentLinkedQueue[MorselExecutionContext]()
    private val taskCount = new AtomicInteger(0)

    def acceptMorsel(inputMorsel: MorselExecutionContext): Unit = {
      eagerData.add(inputMorsel)
    }

    def produceTaskScheduled(task: String): Unit = {
      val tasks = taskCount.incrementAndGet()
      if (Pipeline.DEBUG)
        println("taskCount [%3d]: scheduled %s".format(tasks, task))
    }

    def produceTaskCompleted(task: String, context: QueryContext, state: QueryState): Option[Task] = {
      val tasksLeft = taskCount.decrementAndGet()
      if (Pipeline.DEBUG)
        println("taskCount [%3d]: completed %s".format(tasksLeft, task))

      if (tasksLeft == 0) {
        val inputMorsels: Array[MorselExecutionContext] = eagerData.asScala.toArray
        Some(initTask(start.init(context, state, inputMorsels), context, state))
      }
      else if (tasksLeft < 0) {
        throw new IllegalStateException("Reference counting of tasks has failed: now at task count " + tasksLeft)
      }
      else
        None
    }
  }
}

object QueryState {
  val EMPTY = QueryState(VirtualValues.EMPTY_MAP, null, 10000, singeThreaded = true)
}

case class QueryState(params: MapValue,
                      visitor: QueryResultVisitor[_],
                      morselSize: Int,
                      singeThreaded: Boolean, // hack until we solve [Transaction 1 - * Threads] problem
                      reduceCollector: Option[ReduceCollector] = None)

class RegularPipeline(start: Operator,
                      override val slots: SlotConfiguration,
                      override val upstream: Option[Pipeline]) extends Pipeline {

  def init(inputMorsel: MorselExecutionContext, context: QueryContext, state: QueryState): PipelineTask = {
    initTask(start.init(context, state, inputMorsel), context, state)
  }

  override def acceptMorsel(inputMorsel: MorselExecutionContext, context: QueryContext, state: QueryState): Seq[Task] =
    List(pipelineTask(start.init(context, state, inputMorsel), context, state))

  override def reduceForUpstream(downstreamReduce: Option[ReducePipeline]): Option[ReducePipeline] = downstreamReduce

  override def toString: String = {
    val x = (start +: operators).map(x => x.getClass.getSimpleName)
    s"RegularPipeline(${x.mkString(",")})"
  }
}

object Pipeline {
  private[vectorized] val DEBUG = false
}

abstract class Pipeline() {

  self =>

  // abstract
  def upstream: Option[Pipeline]
  def acceptMorsel(inputMorsel: MorselExecutionContext, context: QueryContext, state: QueryState): Seq[Task]
  def slots: SlotConfiguration
  def reduceForUpstream(downstreamReduce: Option[ReducePipeline]): Option[ReducePipeline]

  // operators
  protected val operators: ArrayBuffer[StatelessOperator] = new ArrayBuffer[StatelessOperator]
  def addOperator(operator: StatelessOperator): Unit =
    operators += operator

  // downstream
  var downstream: Option[Pipeline] = None
  var downstreamReduce: Option[ReducePipeline] = None
  def endPipeline: Boolean = downstream.isEmpty

  /**
    * Walks the tree, setting parent information everywhere so we can push up the tree
    */
  def construct: Pipeline = {
    setDownstream(None, None)
    this
  }

  protected def setDownstream(downstream: Option[Pipeline], downstreamReduce: Option[ReducePipeline]): Unit = {
    this.downstream = downstream
    this.downstreamReduce = downstreamReduce
    this.upstream.foreach(_.setDownstream(Some(this), reduceForUpstream(downstreamReduce)))
  }

  def initTask(startOperatorTask: ContinuableOperatorTask, context: QueryContext, state: QueryState): PipelineTask = {
    val stateWithReduceCollector = state.copy(reduceCollector = downstreamReduce.map(_.init()))
    pipelineTask(startOperatorTask, context, stateWithReduceCollector)
  }

  def pipelineTask(startOperatorTask: ContinuableOperatorTask, context: QueryContext, state: QueryState): PipelineTask = {
    state.reduceCollector.foreach(_.produceTaskScheduled(this.toString))
    PipelineTask(startOperatorTask,
                 operators,
                 context,
                 state,
                 downstream)
  }

  case class PipelineTask(start: ContinuableOperatorTask,
                          operators: IndexedSeq[OperatorTask],
                          originalQueryContext: QueryContext,
                          state: QueryState,
                          downstream: Option[Pipeline]) extends Task {

    override def executeWorkUnit(): Seq[Task] = {
      val outputMorsel = Morsel.create(slots, state.morselSize)
      val currentRow = new MorselExecutionContext(outputMorsel, slots.numberOfLongs, slots.numberOfReferences, 0)
      val queryContext =
        if (state.singeThreaded) originalQueryContext
        else originalQueryContext.createNewQueryContext()
      start.operate(currentRow, queryContext, state)

      for (op <- operators) {
        currentRow.resetToFirstRow()
        op.operate(currentRow, queryContext, state)
      }

      if (org.neo4j.cypher.internal.runtime.vectorized.Pipeline.DEBUG) {
        println(s"Pipeline: $self")

        val longCount = slots.numberOfLongs
        val refCount = slots.numberOfReferences

        println("Resulting rows")
        for (i <- 0 until outputMorsel.validRows) {
          val ls =  util.Arrays.toString(outputMorsel.longs.slice(i * longCount, (i + 1) * longCount))
          val rs =  util.Arrays.toString(outputMorsel.refs.slice(i * refCount, (i + 1) * refCount).asInstanceOf[Array[AnyRef]])
          println(s"$ls $rs")
        }
        println(s"can continue: ${start.canContinue}")
        println()
        println("-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/")
      }

      currentRow.resetToFirstRow()
      val downstreamTasks = downstream.map(_.acceptMorsel(currentRow, queryContext, state)).getOrElse(Nil)

      state.reduceCollector match {
        case Some(x) if !start.canContinue =>
          downstreamTasks ++ x.produceTaskCompleted(self.toString, queryContext, state)

        case _ =>
          downstreamTasks
      }
    }

    override def canContinue: Boolean = start.canContinue
  }
}
