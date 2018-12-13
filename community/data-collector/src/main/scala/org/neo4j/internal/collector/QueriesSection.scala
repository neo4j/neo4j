/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import java.util.{Spliterator, Spliterators}
import java.util.stream.{Stream, StreamSupport}

import org.neo4j.graphdb.ExecutionPlanDescription
import org.neo4j.kernel.api.query.QuerySnapshot
import org.neo4j.values.ValueMapper
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
                              compilationTimeMicros: Long) extends InvocationData

  case class ProfileData(dbHits: util.ArrayList[Long], rows: util.ArrayList[Long], params: util.Map[String, AnyRef])

  case class QueryKey(queryText: String, plan: ExecutionPlanDescription)

  class QueryData() {
    val invocations = new ArrayBuffer[SingleInvocation]
    val profiles = new ArrayBuffer[ProfileData]
  }

  def retrieve(querySnapshots: java.util.Iterator[QuerySnapshot],
               valueMapper: ValueMapper.JavaMapper): Stream[RetrieveResult] = {
    val queries = new mutable.HashMap[QueryKey, QueryData]()
    while (querySnapshots.hasNext) {
      val snapshot = querySnapshots.next()
      val queryString = snapshot.queryText()
      if (!queryString.contains("CALL db.stats.")) {
        val snapshotList = queries.getOrElseUpdate(QueryKey(queryString, snapshot.queryPlan()), new QueryData())
        snapshotList.invocations += SingleInvocation(snapshot.queryParameters(),
                                                     snapshot.elapsedTimeMicros(),
                                                     snapshot.compilationTimeMicros())
      }
    }

    asRetrieveStream(queries.toIterator.map({
      case (queryKey, queryData) =>
        val data = new util.HashMap[String, AnyRef]()
        data.put("query", queryKey.queryText)

        val estimatedRows = new util.ArrayList[Double]
        data.put("queryExecutionPlan", planToMap(queryKey.plan, estimatedRows))
        data.put("estimatedRows", estimatedRows)

        data.put("invocations", invocations(queryData.invocations, valueMapper))
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
                          valueMapper: ValueMapper.JavaMapper
                         ): util.ArrayList[util.Map[String, AnyRef]] = {
    val result = new util.ArrayList[util.Map[String, AnyRef]]()
    for (invocationData <- invocations) {
      invocationData match {
        case SingleInvocation(queryParameters, elapsedTimeMicros, compilationTimeMicros) =>
          val data = new util.HashMap[String, AnyRef]()
          if (queryParameters.size() > 0)
            data.put("params", queryParameters.map(valueMapper))

          val compileTime = compilationTimeMicros
          val elapsed = elapsedTimeMicros
          if (compileTime > 0) {
            data.put("elapsedCompileTimeInUs", java.lang.Long.valueOf(compileTime))
            data.put("elapsedExecutionTimeInUs", java.lang.Long.valueOf(elapsed - compileTime))
          } else
            data.put("elapsedExecutionTimeInUs", java.lang.Long.valueOf(elapsed))
          result.add(data)
      }
    }

    result
  }

  private def asRetrieveStream(iterator: Iterator[RetrieveResult]): Stream[RetrieveResult] = {
    import scala.collection.JavaConverters._
    StreamSupport.stream(Spliterators.spliterator(iterator.asJava, 0L, Spliterator.NONNULL), false)
  }
}
