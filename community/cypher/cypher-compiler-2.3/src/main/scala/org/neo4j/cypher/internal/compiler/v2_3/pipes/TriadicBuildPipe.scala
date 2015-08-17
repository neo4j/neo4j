/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.collection.primitive.{Primitive, PrimitiveLongSet}
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.KeyNames
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.{InternalPlanDescription, PlanDescriptionImpl, SingleChild}
import org.neo4j.cypher.internal.compiler.v2_3.{CypherTypeException, ExecutionContext}
import org.neo4j.graphdb.Node

import scala.collection.mutable.ListBuffer
import scala.collection.{AbstractIterator, Iterator}

case class TriadicBuildPipe(sourcePipe: Pipe, source: String, seen: String)
                           (val estimatedCardinality: Option[Double] = None)(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(sourcePipe, pipeMonitor) with RonjaPipe with NoEffectsPipe {
  override def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    new LazyGroupingIterator[ExecutionContext](input) {
      override def getKey(row: ExecutionContext) = row(source)

      override def getValue(row: ExecutionContext) = row(seen) match {
        case n: Node => Some(n.getId)
        case null => None
        case x => throw new CypherTypeException(s"Expected a node at `$seen` but got $x")
      }

      override def setState(triadicSet: PrimitiveLongSet) = if (triadicSet == null) {
        state.triadicState.remove(seen)
      } else {
        state.triadicState.put(seen, triadicSet)
      }
    }
  }

  override def planDescriptionWithoutCardinality: InternalPlanDescription =
    PlanDescriptionImpl(this.id, "TriadicBuild", SingleChild(sourcePipe.planDescription),
      Seq(KeyNames(Seq(seen))), identifiers)

  override def withEstimatedCardinality(estimated: Double) =
    copy()(Some(estimated))

  override def symbols =
    sourcePipe.symbols

  override def dup(sources: List[Pipe]) = {
    val (head :: Nil) = sources
    copy(sourcePipe = head)(estimatedCardinality)
  }
}

case class TriadicProbePipe(sourcePipe: Pipe, source: String, seen: String, target: String)
                           (val estimatedCardinality: Option[Double] = None)(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(sourcePipe, pipeMonitor) with RonjaPipe with NoEffectsPipe {

  override def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    input.filter {
      ctx =>
        ctx(target) match {
          case n: Node =>
            !state.triadicState(seen).contains(n.getId)
          case _ => false
        }
    }
  }

  override def planDescriptionWithoutCardinality: InternalPlanDescription =
    PlanDescriptionImpl(this.id, "TriadicProbe", SingleChild(sourcePipe.planDescription),
      Seq(KeyNames(Seq(seen, target))), identifiers)

  override def withEstimatedCardinality(estimated: Double) =
    copy()(Some(estimated))

  override def symbols =
    sourcePipe.symbols

  override def dup(sources: List[Pipe]) = {
    val (head :: Nil) = sources
    copy(sourcePipe = head)(estimatedCardinality)
  }
}

abstract class LazyGroupingIterator[ROW >: Null <: AnyRef](val input: Iterator[ROW]) extends AbstractIterator[ROW] {
  def setState(state: PrimitiveLongSet)
  def getKey(row: ROW): Any
  def getValue(row: ROW): Option[Long]

  var current: Iterator[ROW] = null
  var nextRow: ROW = null

  override def next() = if(hasNext) current.next() else Iterator.empty.next()

  override def hasNext: Boolean = {
    if (current != null && current.hasNext)
      true
    else {
      val firstRow = if(nextRow != null) {
        val row = nextRow
        nextRow = null
        row
      } else if(input.hasNext) {
        input.next()
      } else null
      if (firstRow == null) {
        current = null
        setState(null)
        false
      }
      else {
        val buffer = new ListBuffer[ROW]
        val valueSet = Primitive.longSet()
        setState(valueSet)
        buffer += firstRow
        update(valueSet, firstRow)
        val key = getKey(firstRow)
        // N.B. should we rewrite takeWhile to a while-loop?
        buffer ++= input.takeWhile{ row =>
          val s = getKey(row)
          if (s == key) {
            update(valueSet, row)
            true
          } else {
            nextRow = row
            false
          }
        }
        current = buffer.iterator
        true
      }
    }
  }

  def update(triadicSet: PrimitiveLongSet, row: ROW): AnyVal = {
    for (value <- getValue(row))
      triadicSet.add(value)
  }
}
