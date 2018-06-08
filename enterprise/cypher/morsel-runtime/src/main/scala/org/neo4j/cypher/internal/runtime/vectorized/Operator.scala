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

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.parallel.Task
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.values.virtual.MapValue

import scala.collection.JavaConverters._

trait Operator {
  def init(context: QueryContext, state: QueryState, inputMorsel: MorselExecutionContext): ContinuableOperatorTask

  def addDependency(pipeline: Pipeline): Dependency
}

trait ReduceOperator {
  def init(context: QueryContext, state: QueryState, inputMorsels: Seq[MorselExecutionContext]): ContinuableOperatorTask

  def addDependency(pipeline: Pipeline): Dependency
}

trait OperatorTask {
  def operate(data: MorselExecutionContext,
              context: QueryContext,
              state: QueryState): Unit
}

trait ContinuableOperatorTask extends OperatorTask {

  def canContinue: Boolean
}

trait MiddleOperator {
  def init(context: QueryContext): OperatorTask
}

sealed trait Dependency {
  def foreach(f: Pipeline => Unit): Unit

  def pipeline: Pipeline
}

case class Lazy(pipeline: Pipeline) extends Dependency {
  override def foreach(f: Pipeline => Unit): Unit = f(pipeline)
}

case class Eager(pipeline: Pipeline) extends Dependency {
  override def foreach(f: Pipeline => Unit): Unit = f(pipeline)
  lazy val eagerData = new java.util.concurrent.ConcurrentLinkedQueue[MorselExecutionContext]()
}

case object NoDependencies extends Dependency {
  override def foreach(f: Pipeline => Unit): Unit = {}

  override def pipeline = throw new IllegalArgumentException("No dependencies here!")
}

case class QueryState(params: MapValue, visitor: QueryResultVisitor[_], morselSize: Int = 10000)

case class ReducePipeline(start: ReduceOperator,
                          operators: IndexedSeq[MiddleOperator],
                          slots: SlotConfiguration,
                          dependency: Dependency) extends Pipeline {

  val eagerData = new java.util.concurrent.ConcurrentLinkedQueue[MorselExecutionContext]()

  override def addOperator(operator: MiddleOperator): ReducePipeline = copy(operators = operators :+ operator)

  override def acceptMorsel(inputMorsel: MorselExecutionContext,
                            isFinalMorsel: Boolean,
                            context: QueryContext,
                            state: QueryState): Seq[Task] = {
    eagerData.add(inputMorsel)
    if (isFinalMorsel) {
      val inputMorsels: Array[MorselExecutionContext] = eagerData.asScala.toArray
      List(PipelineTask(start.init(context, state, inputMorsels),
        operators.map(_.init(context)),
        context,
        state,
        parent))
    }
    else
      Nil
  }

  override def toString: String = {
    val x = (start +: operators).map(x => x.getClass.getSimpleName)
    s"ReducePipeline(${x.mkString(",")})"
  }
}

case class RegularPipeline(start: Operator,
                           operators: IndexedSeq[MiddleOperator],
                           slots: SlotConfiguration,
                           dependency: Dependency) extends Pipeline {

  override def addOperator(operator: MiddleOperator): RegularPipeline = copy(operators = operators :+ operator)

  def init(inputMorsel: MorselExecutionContext, context: QueryContext, state: QueryState): PipelineTask = {
    PipelineTask(start.init(context, state, inputMorsel), operators.map(_.init(context)), context, state, parent)
  }

  override def acceptMorsel(inputMorsel: MorselExecutionContext, isFinalMorsel: Boolean, context: QueryContext, state: QueryState): Seq[Task] =
    List(init(inputMorsel, context, state))

  override def toString: String = {
    val x = (start +: operators).map(x => x.getClass.getSimpleName)
    s"RegularPipeline(${x.mkString(",")})"
  }
}

object Pipeline {
  private val DEBUG = true
}

abstract class Pipeline() {

  self =>

  def dependency: Dependency
  def acceptMorsel(inputMorsel: MorselExecutionContext, isFinalMorsel: Boolean, context: QueryContext, state: QueryState): Seq[Task]
  def slots: SlotConfiguration
  def addOperator(operator: MiddleOperator): Pipeline

  var parent: Option[Pipeline] = None
  def endPipeline: Boolean = parent.isEmpty

  /*
  Walks the tree, setting parent information everywhere so we can push up the tree
   */
  def construct: Pipeline = {
    dependency.foreach(_.noIamYourFather(this))
    this
  }

  protected def noIamYourFather(daddy: Pipeline): Unit = {
    dependency.foreach(_.noIamYourFather(this))
    parent = Some(daddy)
  }

  case class PipelineTask(start: ContinuableOperatorTask,
                          operators: IndexedSeq[OperatorTask],
                          context: QueryContext,
                          state: QueryState,
                          downstream: Option[Pipeline]) extends Task {

    override def executeWorkUnit(): Seq[Task] = {
      val outputMorsel = Morsel.create(slots, state.morselSize)
      val currentRow = new MorselExecutionContext(outputMorsel, slots.numberOfLongs, slots.numberOfReferences, 0)
      start.operate(currentRow, context, state)

      for (op <- operators) {
        currentRow.resetToFirstRow()
        op.operate(currentRow, context, state)
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

      downstream.map(_.acceptMorsel(currentRow, !start.canContinue, context, state)).getOrElse(Nil)
    }

    override def canContinue(): Boolean = start.canContinue
  }
}
