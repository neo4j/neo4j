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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.QueryStatistics
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper.literal
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ProduceResultsPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.query.RecordingQuerySubscriber
import org.neo4j.values.AnyValue

import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.Future

import scala.jdk.CollectionConverters.ListHasAsScala

class ProduceResultSlottedPipeStressTest extends CypherFunSuite {

  private val slotConfig = SlotConfiguration.empty.newLong("n", nullable = false, CTNode)

  test(s"concurrent slotted produce result execution should not crash") {

    // Given
    val produceResults = ProduceResultsPipe(sourcePipe, Array(literal(42)))(Id.INVALID_ID)
    val expected = execute(produceResults)

    // When
    val nThreads = 10
    val executor = Executors.newFixedThreadPool(nThreads)
    val futureResultsAsExpected =
      for (_ <- 1 to nThreads) yield executor.submit(new Callable[Array[AnyValue]] {
        override def call(): Array[AnyValue] = {
          (for (_ <- 1 to 1000) yield {
            execute(produceResults)
          }).toArray
        }
      })

    // Then no crashes...
    for (
      futureResultSet: Future[Array[AnyValue]] <- futureResultsAsExpected;
      result: AnyValue <- futureResultSet.get
    ) {
      // ...and correct results
      result should equal(expected)
    }
    executor.shutdown()
  }

  private def execute(produceResults: ProduceResultsPipe): AnyValue = {
    val subscriber = new RecordingQuerySubscriber
    subscriber.onResult(1)
    val iterator = produceResults.createResults(QueryStateHelper.emptyWith(subscriber = subscriber))
    subscriber.onResultCompleted(QueryStatistics.empty)
    // equivalent of request(1)
    iterator.next()

    subscriber.getOrThrow().asScala.head(0)
  }

  private val sourcePipe: Pipe =
    new Pipe {

      override protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] =
        ClosingIterator.single(SlottedRow(slotConfig))
      override val id: Id = Id.INVALID_ID
    }
}
