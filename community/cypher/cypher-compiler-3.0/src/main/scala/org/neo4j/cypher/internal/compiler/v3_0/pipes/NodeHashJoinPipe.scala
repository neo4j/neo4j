/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.pipes

import org.neo4j.cypher.internal.compiler.v3_0.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription.Arguments.KeyNames
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.{InternalPlanDescription, PlanDescriptionImpl, TwoChildren}
import org.neo4j.cypher.internal.frontend.v3_0.CypherTypeException
import org.neo4j.graphdb.Node

import scala.collection.mutable
import scala.reflect.ClassTag

case class NodeHashJoinPipe(nodeVariables: Set[String], left: Pipe, right: Pipe,
                            probeTableCreator: ProbeTableCreator = HashMapProbeTableCreator, reversalSize: Long = 8192L, dynamicReverse: Boolean = true)
                           (val estimatedCardinality: Option[Double] = None)(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(left, pipeMonitor) with RonjaPipe {

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    if (input.isEmpty)
      return Iterator.empty

    val rhsIterator = right.createResults(state)

    if (rhsIterator.isEmpty)
      return Iterator.empty

    val table = buildProbeTable(input, continueAfterMaxSize = !dynamicReverse)

    if (table.isEmpty)
      return Iterator.empty

    if (input.nonEmpty) {
      // If the LHS input is not empty, it means we bailed out from probe table building from hitting the size limit
      // We will opportunistically try and build the probe table on the RHS, hoping that it is smaller
      val tableR = buildProbeTable(rhsIterator, continueAfterMaxSize = true)
      mergeProbeTables(table, tableR) ++ returnResults(input, tableR)
    } else {
      // If we have emptied the LHS, just use the probe table on the RHS
      returnResults(rhsIterator, table)
    }
  }

  private def mergeProbeTables(tableL: ProbeTable[Vector[Long], ExecutionContext],
                            tableR: ProbeTable[Vector[Long], ExecutionContext]): Iterator[ExecutionContext] = {
    tableL.elements.flatMap {
      case (k, values) => tableR.probe(k)
    }
  }

  private def returnResults(input: Iterator[ExecutionContext], table: ProbeTable[Vector[Long], ExecutionContext]): Iterator[ExecutionContext] = {
    val result = for {context: ExecutionContext <- input
                      joinKey <- computeKey(context)}
      yield {
        val seq = table.probe(joinKey)
        seq.map(context ++ _)
      }

    result.flatten
  }

  def planDescriptionWithoutCardinality: InternalPlanDescription =
    new PlanDescriptionImpl(
      id = id,
      name = "NodeHashJoin",
      children = TwoChildren(left.planDescription, right.planDescription),
      arguments = Seq(KeyNames(nodeVariables.toSeq)),
      variables
    )

  def symbols = left.symbols.add(right.symbols.variables)

  override val sources = Seq(left, right)

  def dup(sources: List[Pipe]): Pipe = {
    val (left :: right :: Nil) = sources
    copy(left = left, right = right)(estimatedCardinality)
  }

  override def localEffects = Effects()

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  private def buildProbeTable(input: Iterator[ExecutionContext], continueAfterMaxSize: Boolean): (ProbeTable[Vector[Long], ExecutionContext]) = {
    val table = probeTableCreator.create[Vector[Long], ExecutionContext]

    while (input.hasNext && (table.size < reversalSize || continueAfterMaxSize)) {
      val context = input.next()
      computeKey(context).foreach(joinKey => table.add(joinKey, context))
    }

    table
  }

  private val cachedVariables = nodeVariables.toIndexedSeq

  private def computeKey(context: ExecutionContext): Option[Vector[Long]] = {
    val key = new Array[Long](cachedVariables.length)

    for (idx <- cachedVariables.indices) {
      key(idx) = context(cachedVariables(idx)) match {
        case n: Node => n.getId
        case null => return None
        case _ => throw new CypherTypeException("Created a plan that uses non-nodes when expecting a node")
      }
    }
    Some(key.toVector)
  }
}

trait ProbeTable[Key, Value] {
  def add(k: Key, v: Value)

  def isEmpty: Boolean

  def probe(k: Key): Iterator[Value]

  def size: Long // Defined as how many K/V pairs have been added

  def elements: Iterator[(Key, Seq[Value])]
}

class HashMapProbeTable[Key, Value] extends ProbeTable[Key, Value] {
  private val inner = new mutable.HashMap[Key, mutable.MutableList[Value]]
  private var _size = 0L

  override def add(k: Key, v: Value) = {
    _size += 1
    val seq = inner.getOrElseUpdate(k, mutable.MutableList.empty)
    seq += v
  }

  override def isEmpty = inner.isEmpty

  override def probe(k: Key) = inner.get(k).map(_.toIterator).getOrElse(Iterator.empty)

  override def size = _size

  override def elements = {
    inner.toIterator.map { case (k, values) => k -> values.toSeq }
  }
}

trait ProbeTableCreator {
  def create[K: ClassTag, V: ClassTag]: ProbeTable[K, V]
}

object HashMapProbeTableCreator extends ProbeTableCreator {
  override def create[K: ClassTag, V: ClassTag] = new HashMapProbeTable[K, V]()
}