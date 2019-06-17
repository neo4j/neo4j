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
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.cypher.exceptionHandler.runSafely
import org.neo4j.cypher.internal.runtime.{QueryStatistics, ResourceManager}
import org.neo4j.cypher.result.{QueryProfile, RuntimeResult}
import org.neo4j.kernel.impl.query.{QuerySubscriber, TransactionalContext}

/**
  * This is needed for tests, because closing is usually handled in org.neo4j.cypher.internal.result.ClosingExecutionResult,
  * which we are not using here. We need to close the results to make sure that updates are committed and that cursors are closed.
  */
class ClosingRuntimeResult(inner: RuntimeResult,
                           txContext: TransactionalContext,
                           resourceManager: ResourceManager,
                           subscriber: QuerySubscriber,
                           assertAllReleased: () => Unit) extends RuntimeResult{

  private var error: Throwable = _

  override def fieldNames(): Array[String] = inner.fieldNames()

  override def consumptionState(): RuntimeResult.ConsumptionState = inner.consumptionState()

  override def queryStatistics(): QueryStatistics = inner.queryStatistics()

  override def queryProfile(): QueryProfile = inner.queryProfile()

  override def close(): Unit = {
    inner.close()
    closeResources(true)
  }

  override def request(numberOfRecords: Long): Unit =
    runSafely[Unit](inner.request(numberOfRecords))(t => {
      this.error = t
      subscriber.onError(t)
      close()
  })

  override def cancel(): Unit = inner.cancel()

  override def await(): Boolean = {
    if (this.error != null) {
      throw error
    }

    try {
      val moreData = inner.await()
      if (!moreData) {
        closeResources(true)
      }
      moreData
    } catch {
      case t: Throwable =>
        closeResources(false)
        throw t
    }
  }

  private def closeResources(success: Boolean): Unit = {
    resourceManager.close(success)
    assertAllReleased()
    txContext.close(success)
  }
}
