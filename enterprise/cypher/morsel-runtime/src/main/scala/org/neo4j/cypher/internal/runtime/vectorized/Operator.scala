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
import org.neo4j.cypher.internal.runtime.vectorized.Pipeline.DEBUG
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.values.virtual.MapValue

trait Operator {
  def operate(message: Message,
              currentRow: MorselExecutionContext,
              context: QueryContext,
              state: QueryState): Continuation

  def addDependency(pipeline: Pipeline): Dependency
}

trait MiddleOperator {
  def operate(iterationState: Iteration,
              currentRow: MorselExecutionContext,
              context: QueryContext,
              state: QueryState): Unit
}

/*
The return type allows an operator to signal if the a morsel it has operated on contains interesting information or not
 */
sealed trait ReturnType

object MorselType extends ReturnType

object UnitType extends ReturnType

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

case class QueryState(params: MapValue, visitor: QueryResultVisitor[_])

object PipeLineWithEagerDependency {
  def unapply(arg: Pipeline): Option[java.util.Queue[MorselExecutionContext]] = arg match {
    case Pipeline(_,_,_, eager@Eager(_)) => Some(eager.eagerData)
    case _ => None
  }
}

case class Pipeline(start: Operator,
                    operators: IndexedSeq[MiddleOperator],
                    slots: SlotConfiguration,
                    dependency: Dependency)
                   (var parent: Option[Pipeline] = None) {

  def endPipeline: Boolean = parent.isEmpty

  def addOperator(operator: MiddleOperator): Pipeline = copy(operators = operators :+ operator)(parent)


  def operate(message: Message, data: Morsel, context: QueryContext, state: QueryState): Continuation = {
    val currentRow = new MorselExecutionContext(data, slots.numberOfLongs, slots.numberOfReferences, 0)
    val next = start.operate(message, currentRow, context, state)

    val longCount = slots.numberOfLongs
    val refCount = slots.numberOfReferences

    if (DEBUG) {
      println(s"Message: $message")
      println(s"Pipeline: $this")

      println(s"Rows after ${start.getClass.getSimpleName}")
      for (i <- 0 until data.validRows) {
        val ls =  util.Arrays.toString(data.longs.slice(i * longCount, (i + 1) * longCount))
        val rs =  util.Arrays.toString(data.refs.slice(i * refCount, (i + 1) * refCount).asInstanceOf[Array[AnyRef]])
        println(s"$ls $rs")
      }
    }

    operators.foreach { op =>
      currentRow.resetToFirstRow()
      op.operate(next.iteration, currentRow, context, state)

      if (DEBUG) {
        println(s"Rows after ${op.getClass.getSimpleName}")
        for (i <- 0 until data.validRows) {
          val ls =  util.Arrays.toString(data.longs.slice(i * longCount, (i + 1) * longCount))
          val rs =  util.Arrays.toString(data.refs.slice(i * refCount, (i + 1) * refCount).asInstanceOf[Array[AnyRef]])
          println(s"$ls $rs")
        }
      }
    }

    if (DEBUG) {
      println(s"Resulting continuation: $next")
      println()
      println("-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/")
    }

    next
  }

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

  override def toString: String = {
    val x = (start +: operators).map(x => x.getClass.getSimpleName)
    s"Pipeline(${x.mkString(",")})"
  }
}

object Pipeline {
  private val DEBUG = false
}
