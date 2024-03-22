/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.collection.trackable.HeapTrackingArrayList
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ParameterMapping
import org.neo4j.cypher.internal.runtime.PrefetchingIterator
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.kernel.impl.query.QueryExecution
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.GraphReferenceValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.MapValueBuilder

case class RunQueryAtPipe(
  source: Pipe,
  query: String,
  graph: Expression,
  parameters: Map[Parameter, Expression],
  columns: Set[LogicalVariable],
  parameterMapping: ParameterMapping
)(override val id: Id) extends Pipe {

  private val BUFFER_SIZE = 1024 // a nice computery number

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] =
    source.createResults(state)
      .flatMap(iterator(_, state))

  private def iterator(row: CypherRow, state: QueryState): RunQueryAtIterator = {
    val transaction = {
      val dbName = graph.apply(row, state).asInstanceOf[GraphReferenceValue]
      state.query.transactionalContext.constituentTransactionFactory.transactionFor(dbName.getDbRef)
    }

    val paramCapacity = parameters.size + parameterMapping.size
    val params = if (paramCapacity > 0) {
      val mapValueBuilder = new MapValueBuilder(paramCapacity)
      parameters.foreach { case (param, value) =>
        mapValueBuilder.add(param.name, value.apply(row, state))
      }

      parameterMapping.foreach { case (param, offsetAndDefault) =>
        mapValueBuilder.add(param, state.params(offsetAndDefault.offset))
      }

      mapValueBuilder.build
    } else {
      MapValue.EMPTY
    }

    new RunQueryAtIterator(
      s => transaction.executeQuery(query, params, s),
      () => row.createClone(),
      columns,
      BUFFER_SIZE,
      state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x).getScopedMemoryTracker
    )
  }
}

class RunQueryAtIterator(
  getExecution: QuerySubscriber => QueryExecution,
  newRow: () => CypherRow,
  expectedFields: Set[LogicalVariable],
  batchSize: Int,
  memoryTracker: MemoryTracker
) extends PrefetchingIterator[CypherRow] { self =>

  private val buffer = HeapTrackingArrayList.newArrayList[CypherRow](batchSize, memoryTracker)
  private val current = new Array[AnyValue](expectedFields.size)
  private var error = Option.empty[Throwable]
  private var moreAvailable = true
  private var currentIndex = 0
  private var currentHeap = 0L

  private lazy val execution = {
    val ex = getExecution(new QuerySubscriber {
      def onResult(numberOfFields: Int): Unit = ()
      def onRecord(): Unit = ()

      def onField(offset: Int, value: AnyValue): Unit = {
        self.currentHeap += value.estimatedHeapUsage()
        self.current
          .update(offset, value)
      }

      def onRecordCompleted(): Unit = {
        memoryTracker.allocateHeap(currentHeap)
        self.buffer.add(createRow())
        self.currentHeap = 0L
      }

      def onError(throwable: Throwable): Unit = {
        self.error = Some(throwable)
      }

      def onResultCompleted(statistics: QueryStatistics): Unit = {
        moreAvailable = false
      }
    })

    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      ex.fieldNames().toSet == expectedFields.map(_.name),
      "RunQueryAt received unexpected fields "
        + ex.fieldNames().mkString("[", ", ", "]")
        + s"; expected "
        + expectedFields.map(_.name).mkString("[", ",", "]")
    )

    ex
  }

  private def createRow(): CypherRow = {
    val row = newRow()
    execution.fieldNames().zip(self.current).foreach { case (name, value) =>
      row.set(name, value)
    }
    row
  }

  def produceNext(): Option[CypherRow] = {
    error.foreach { throw _ }
    if (currentIndex < buffer.size) {
      val row = buffer.get(currentIndex)
      currentIndex += 1
      Some(row)
    } else if (moreAvailable) {
      buffer.clear()
      execution.request(batchSize)
      moreAvailable &&= execution.await()
      currentIndex = 0
      produceNext()
    } else {
      None
    }
  }

  protected[this] def closeMore(): Unit = {
    self.buffer.close()
    self.memoryTracker.close()
  }
}
