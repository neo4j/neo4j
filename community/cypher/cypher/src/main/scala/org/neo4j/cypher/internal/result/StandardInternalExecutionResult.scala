/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.result

import org.neo4j.cypher.internal.RuntimeName
import org.neo4j.cypher.internal.javacompat.{ResultRowImpl, ResultSubscriber}
import org.neo4j.cypher.internal.plandescription.{InternalPlanDescription, PlanDescriptionBuilder}
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.v4_0.util.TaskCloser
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.cypher.result.{QueryResult, RuntimeResult, VisitableRuntimeResult}
import org.neo4j.exceptions.ProfilerStatisticsNotReadyException
import org.neo4j.graphdb.Notification
import org.neo4j.graphdb.Result.{ResultRow, ResultVisitor}
import org.neo4j.kernel.impl.query.QuerySubscriber

class StandardInternalExecutionResult(context: QueryContext,
                                      runtime: RuntimeName,
                                      runtimeResult: RuntimeResult,
                                      taskCloser: TaskCloser,
                                      override val queryType: InternalQueryType,
                                      override val executionMode: ExecutionMode,
                                      planDescriptionBuilder: PlanDescriptionBuilder,
                                      subscriber: QuerySubscriber)
  extends InternalExecutionResult {

  self =>

  override def initiate(): Unit = {
    // For write only queries and queries that return no rows, execute all
    // work immediately, and close all resources.
    if (queryType == WRITE || fieldNames().isEmpty) {
      request(1)
      await()
      close(Success)
    }

    subscriber match {
      case coreAPI: ResultSubscriber =>
        // OBS: check before materialization
        val consumedBeforeMaterialize = runtimeResult.consumptionState == ConsumptionState.EXHAUSTED

        // By policy we materialize the result directly unless it's a read only query.
        if (queryType != READ_ONLY) {
          coreAPI.materialize(this)
        }

        // ... and if we do not return any rows, we close all resources.
        if (consumedBeforeMaterialize) {
          close(Success)
        }

      case _ => //do nothing
    }
  }

  /*
  ======= OPEN / CLOSE ==========
   */

  protected def isOpen: Boolean = !isClosed

  override def isClosed: Boolean = taskCloser.isClosed

  override def close(reason: CloseReason): Unit = {
    runtimeResult.cancel()
    taskCloser.close(reason == Success)
  }

  override def request(numberOfRows: Long): Unit = runtimeResult.request(numberOfRows)

  override def cancel(): Unit = {
    runtimeResult.cancel()
    close()
  }

  override def await(): Boolean = runtimeResult.await()

  /*
  ======= META DATA ==========
   */

  override def fieldNames(): Array[String] = runtimeResult.fieldNames()

  override lazy val executionPlanDescription: InternalPlanDescription = {

    if (executionMode == ProfileMode) {
      if (runtimeResult.consumptionState != ConsumptionState.EXHAUSTED) {
        taskCloser.close(success = false)
        throw new ProfilerStatisticsNotReadyException()
      }
      planDescriptionBuilder.profile(runtimeResult.queryProfile)
    } else {
      planDescriptionBuilder.explain()
    }

  }

  override def notifications: Iterable[Notification] = Set.empty

  protected def accept(body: ResultRow => Unit): Unit = {
    accept(new ResultVisitor[RuntimeException] {
      override def visit(row: ResultRow): Boolean = {
        body(row)
        true
      }
    })
  }

  override def isVisitable: Boolean = runtimeResult.isInstanceOf[VisitableRuntimeResult]

  override def accept[E <: Exception](visitor: ResultVisitor[E]): QueryStatistics =  runtimeResult match {
    case v: VisitableRuntimeResult =>
      v.accept(new QueryResultVisitor[E] {
        private val names = fieldNames()
        override def visit(record: QueryResult.Record): Boolean = {
          val fields = record.fields()
          val length = names.length
          //to avoid resize we do lenght/ loadfactor
          val mapData = new java.util.HashMap[String, AnyRef]((length * 1.33).asInstanceOf[Int])

          var i = 0
          while (i < length) {
            mapData.put(names(i), context.asObject(fields(i)))
            i += 1
          }
          visitor.visit(new ResultRowImpl(mapData))
        }
      })
      v.queryStatistics()
    case _ => throw new IllegalStateException("Can't call accept on a non-visitable result")

  }
}




