/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.vectorized

import java.util

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.values.virtual.MapValue

trait Operator {
  def operate(message: Message,
              data: Morsel,
              context: QueryContext,
              state: QueryState): Continuation

  def addDependency(pipeline: Pipeline): Dependency
}

trait MiddleOperator {
  def operate(iterationState: Iteration,
              data: Morsel,
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
}

case object NoDependencies extends Dependency {
  override def foreach(f: Pipeline => Unit): Unit = {}

  override def pipeline = throw new IllegalArgumentException("No dependencies here!")
}

case class QueryState(params: MapValue, visitor: QueryResultVisitor[_])

case class Pipeline(start: Operator,
                    operators: IndexedSeq[MiddleOperator],
                    slots: SlotConfiguration,
                    dependency: Dependency)
                   (var parent: Option[Pipeline] = None) {

  def endPipeline: Boolean = parent.isEmpty

  def addOperator(operator: MiddleOperator): Pipeline = copy(operators = operators :+ operator)(parent)

  def operate(message: Message, data: Morsel, context: QueryContext, state: QueryState): Continuation = {
    val next = start.operate(message, data, context, state)

    operators.foreach { op =>
      op.operate(next.iteration, data, context, state)
    }

    if (false /*BEDUG!*/ ) {
      println(s"Message: $message")
      println(s"Pipeline: $this")


      val longCount = slots.numberOfLongs
      val rows = for (i <- 0 until(data.validRows * longCount, longCount)) yield {
        util.Arrays.toString(data.longs.slice(i, i + longCount))
      }
      val longValues = rows.mkString(System.lineSeparator())
      println(
        s"""Resulting rows:
           |$longValues""".
          stripMargin)
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
