/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.internal.collector

import java.util
import java.util.stream.{Stream, StreamSupport}
import java.util.{Spliterator, Spliterators}

import org.neo4j.graphdb.ExecutionPlanDescription
import org.neo4j.values.virtual.MapValue

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Data collector section which contains query invocation data. This includes the query itself,
  * the logical plan, cardinality information, and listing of seen invocations.
  */
object QueriesSection {

  sealed trait InvocationData
  case class SingleInvocation(queryParameters: MapValue,
                              elapsedTimeMicros: Long,
                              compilationTimeMicros: Long,
                              startTimestampMillis: Long) extends InvocationData

  case class ProfileData(dbHits: util.ArrayList[Long], rows: util.ArrayList[Long], params: util.Map[String, AnyRef])

  case class QueryKey(queryText: String, fullQueryTextHash: Int, plan: ExecutionPlanDescription)

  class QueryData() {
    val invocations = new ArrayBuffer[SingleInvocation]
    val profiles = new ArrayBuffer[ProfileData]
  }

  private val QUERY_FILTER = "(?:(?i)call)\\s+(?:dbms\\.|db\\.stats\\.)".r

  def retrieve(querySnapshots: java.util.Iterator[TruncatedQuerySnapshot],
               anonymizer: QueryAnonymizer,
               maxInvocations: Int): Stream[RetrieveResult] = {
    val queries = new mutable.HashMap[QueryKey, QueryData]()
    while (querySnapshots.hasNext) {
      val snapshot = querySnapshots.next()
      val queryString = snapshot.queryText
      if (QUERY_FILTER.findFirstMatchIn(queryString).isEmpty) {
        val queryKey = QueryKey(queryString, snapshot.fullQueryTextHash, snapshot.queryPlanSupplier.get())
        val snapshotList = queries.getOrElseUpdate(queryKey, new QueryData())
        snapshotList.invocations += SingleInvocation(snapshot.queryParameters,
                                                     snapshot.elapsedTimeMicros,
                                                     snapshot.compilationTimeMicros,
                                                     snapshot.startTimestampMillis)
      }
    }

    asRetrieveStream(queries.toIterator.map({
      case (queryKey, queryData) =>
        val data = new util.HashMap[String, AnyRef]()
        data.put("query", anonymizer.queryText(queryKey.queryText))

        val estimatedRows = new util.ArrayList[Double]
        data.put("queryExecutionPlan", planToMap(queryKey.plan, estimatedRows))
        data.put("estimatedRows", estimatedRows)

        data.put("invocations", invocations(queryData.invocations.take(maxInvocations), anonymizer))
        data.put("invocationSummary", invocationSummary(queryData.invocations))
        new RetrieveResult(Sections.QUERIES, data)
    }))
  }

  private def planToMap(plan: ExecutionPlanDescription, estimatedRows: util.ArrayList[Double]): util.Map[String, AnyRef] = {
    val data = new util.HashMap[String, AnyRef]()
    val id: Integer = estimatedRows.size
    estimatedRows.add(plan.getArguments.get("EstimatedRows").asInstanceOf[Double])
    data.put("id", id)
    data.put("operator", plan.getName)
    val children = plan.getChildren
    children.size match {
      case 0 => // nothing to do
      case 1 => data.put("lhs", planToMap(children.get(0), estimatedRows))
      case 2 =>
        data.put("lhs", planToMap(children.get(0), estimatedRows))
        data.put("rhs", planToMap(children.get(1), estimatedRows))
      case x =>
        throw new IllegalStateException("Cannot handle operators with more that 2 children, got "+x)
    }
    data
  }

  private def invocations(invocations: ArrayBuffer[QueriesSection.SingleInvocation],
                          anonymizer: QueryAnonymizer
                         ): util.ArrayList[util.Map[String, AnyRef]] = {
    val result = new util.ArrayList[util.Map[String, AnyRef]]()
    for (SingleInvocation(queryParameters, elapsedTimeMicros, compilationTimeMicros, startTimestampMillis) <- invocations) {
      val data = new util.HashMap[String, AnyRef]()
      if (queryParameters.size() > 0)
        data.put("params", anonymizer.queryParams(queryParameters))

      val compileTime = compilationTimeMicros
      val elapsed = elapsedTimeMicros
      if (compileTime > 0) {
        data.put("elapsedCompileTimeInUs", Long.box(compileTime))
        data.put("elapsedExecutionTimeInUs", Long.box(elapsed - compileTime))
      } else
        data.put("elapsedExecutionTimeInUs", Long.box(elapsed))
      data.put("startTimestampMillis", Long.box(startTimestampMillis))
      result.add(data)
    }

    result
  }

  private def invocationSummary(invocations: ArrayBuffer[QueriesSection.SingleInvocation]
                               ): util.Map[String, AnyRef] = {
    val result = new util.HashMap[String, AnyRef]()
    val compileTime = new Stats
    val executionTime = new Stats
    for (invocation <- invocations) {
      compileTime.onValue(invocation.compilationTimeMicros)
      executionTime.onValue(invocation.elapsedTimeMicros - invocation.compilationTimeMicros)
    }

    result.put("compileTimeInUs", compileTime.asMap())
    result.put("executionTimeInUs", executionTime.asMap())
    result.put("invocationCount", Long.box(invocations.size))
    result
  }

  private def asRetrieveStream(iterator: Iterator[RetrieveResult]): Stream[RetrieveResult] = {
    import scala.collection.JavaConverters._
    StreamSupport.stream(Spliterators.spliterator(iterator.asJava, 0L, Spliterator.NONNULL), false)
  }

  class Stats {
    private var min = Long.MaxValue
    private var max = Long.MinValue
    private var sum = 0L
    private var count = 0L

    def onValue(x: Long): Unit = {
      min = math.min(x, min)
      max = math.max(x, max)
      sum += x
      count += 1
    }

    def asMap(): util.HashMap[String, AnyRef] = {
      val data = new util.HashMap[String, AnyRef]()
      data.put("min", Long.box(min))
      data.put("max", Long.box(max))
      data.put("avg", Long.box(sum / count))
      data
    }
  }
}
