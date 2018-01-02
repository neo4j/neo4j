/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.{InternalPlanDescription, PlanDescriptionImpl, TwoChildren}
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.graphdb.Node

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class NodeOuterHashJoinPipe(nodeIdentifiers: Set[String], source: Pipe, inner: Pipe, nullableIdentifiers: Set[String])
                                (val estimatedCardinality: Option[Double] = None)(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor) with RonjaPipe {
  val nullColumns: Map[String, Any] = nullableIdentifiers.map(_ -> null).toMap

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {

    if(input.isEmpty)
      return Iterator.empty

    val probeTable = buildProbeTableAndFindNullRows(input)

    val seenKeys = mutable.Set[Vector[Long]]()
    val joinedRows = (
      for {context <- inner.createResults(state)
           joinKey <- computeKey(context)}
      yield {
        val seq = probeTable(joinKey)
        seenKeys.add(joinKey)
        seq.map(context ++ _)
      }).flatten

    def rowsWithoutRhsMatch: Iterator[ExecutionContext] = (probeTable.keySet -- seenKeys).iterator.flatMap {
      x => probeTable(x).map(addNulls)
    }

    val rowsWithNullAsJoinKey = probeTable.nullRows.map(addNulls)

    rowsWithNullAsJoinKey ++ joinedRows ++ rowsWithoutRhsMatch
  }

  private def addNulls(in:ExecutionContext): ExecutionContext = in.newWith(nullColumns)

  def planDescriptionWithoutCardinality: InternalPlanDescription =
    new PlanDescriptionImpl(this.id,
      "NodeOuterHashJoin",
      TwoChildren(source.planDescription, inner.planDescription),
      Seq.empty,
      identifiers
    )

  def symbols: SymbolTable = source.symbols.add(inner.symbols.identifiers)

  override val sources = Seq(source, inner)

  def dup(sources: List[Pipe]): Pipe = {
    val (source :: inner :: Nil) = sources
    copy(source = source, inner = inner)(estimatedCardinality)
  }

  override def localEffects = Effects()

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  private def buildProbeTableAndFindNullRows(input: Iterator[ExecutionContext]): ProbeTable = {
    val probeTable = new ProbeTable()

    for (context <- input) {
      val key = computeKey(context)

      key match {
        case Some(joinKey) => probeTable.addValue(joinKey, context)
        case None          => probeTable.addNull(context)
      }
    }

    probeTable
  }

  private val myIdentifiers = nodeIdentifiers.toIndexedSeq

  private def computeKey(context: ExecutionContext): Option[Vector[Long]] = {
    val key = new Array[Long](myIdentifiers.length)

    for (idx <- 0 until myIdentifiers.length) {
      key(idx) = context(myIdentifiers(idx)) match {
        case n: Node => n.getId
        case _ => return None
      }
    }
    Some(key.toVector)
  }
}

class ProbeTable() {
  private val table: mutable.HashMap[Vector[Long], mutable.MutableList[ExecutionContext]] =
    new mutable.HashMap[Vector[Long], mutable.MutableList[ExecutionContext]]

  private val rowsWithNullInKey: ListBuffer[ExecutionContext] = new ListBuffer[ExecutionContext]()

  def addValue(key: Vector[Long], newValue: ExecutionContext) {
    val values = table.getOrElseUpdate(key, mutable.MutableList.empty)
    values += newValue
  }

  def addNull(context: ExecutionContext) = rowsWithNullInKey += context

  val EMPTY = mutable.MutableList.empty
  def apply(key: Vector[Long]) = table.getOrElse(key, EMPTY)

  def keySet: collection.Set[Vector[Long]] = table.keySet

  def nullRows = rowsWithNullInKey.iterator
}
