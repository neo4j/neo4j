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
package org.neo4j.cypher.internal.result

import org.neo4j.cypher.internal.RuntimeName
import org.neo4j.cypher.internal.plandescription.{InternalPlanDescription, PlanDescriptionBuilder}
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.v4_0.util.{ProfilerStatisticsNotReadyException, TaskCloser}
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.graphdb.Notification
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
  }

  /*
  ======= OPEN / CLOSE ==========
   */

  protected def isOpen: Boolean = !isClosed

  override def isClosed: Boolean = taskCloser.isClosed

  override def close(reason: CloseReason): Unit = {
    taskCloser.close(reason == Success)
  }

  override def request(numberOfRows: Long): Unit = runtimeResult.request(numberOfRows)

  override def cancel(): Unit = {
    runtimeResult.cancel()
    close()
  }

  override def await(): Boolean = runtimeResult.await()

  /*
  ======= DUMP TO STRING ==========
   */

//  override def dumpToString(): String =
//
//  override def dumpToString(writer: PrintWriter): Unit = {
//    val resultStringBuilder = ResultStringBuilder(fieldNames(), context.transactionalContext)
//    accept(resultStringBuilder)
//    resultStringBuilder.result(writer, queryStatistics())
//  }

  /*
  ======= META DATA ==========
   */

  override def queryStatistics(): QueryStatistics = runtimeResult.queryStatistics()

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
}




