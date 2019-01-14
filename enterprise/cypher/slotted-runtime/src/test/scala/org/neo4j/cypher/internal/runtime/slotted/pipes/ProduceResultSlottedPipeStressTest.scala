/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import java.util.concurrent.{Callable, Executors, Future}

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, QueryState}
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, QueryStateHelper}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.cypher.internal.util.v3_4.symbols.CTNode
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.result.QueryResult
import org.neo4j.cypher.result.QueryResult.Record
import org.neo4j.values.AnyValue

class ProduceResultSlottedPipeStressTest extends CypherFunSuite {

  private val column = "x"
  private val slotConfig = SlotConfiguration.empty.newLong("n", nullable = false, CTNode)

  test(s"concurrent slotted produce result execution should not crash") {

    // Given
    val produceResults = ProduceResultSlottedPipe(sourcePipe, List(column -> Literal(42)))(Id.INVALID_ID)
    val expected = execute(produceResults)

    // When
    val nThreads = 10
    val executor = Executors.newFixedThreadPool(nThreads)
    val futureResultsAsExpected =
      for (i <- 1 to nThreads) yield
        executor.submit(new Callable[Array[AnyValue]] {
          override def call(): Array[AnyValue] = {
            (for (j <- 1 to 1000) yield {
              execute(produceResults)
            }).toArray
          }
        })

    // Then no crashes...
    for (futureResultSet: Future[Array[AnyValue]] <- futureResultsAsExpected;
         result: AnyValue <- futureResultSet.get
    ) {
      // ...and correct results
      result should equal(expected)
    }
    executor.shutdown()
  }

  private def execute(produceResults: ProduceResultSlottedPipe): AnyValue = {
    val iterator = produceResults.createResults(QueryStateHelper.empty).asInstanceOf[Iterator[QueryResult.Record]]
    val row: Record = iterator.next()
    val result = row.fields()(0)
    row.release()
    result
  }

  private val sourcePipe: Pipe =
    new Pipe {
      override protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] =
        Iterator(SlottedExecutionContext(slotConfig))
      override val id: Id = Id.INVALID_ID
    }
}
