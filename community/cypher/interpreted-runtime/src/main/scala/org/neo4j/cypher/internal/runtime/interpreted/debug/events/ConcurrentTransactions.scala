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
package org.neo4j.cypher.internal.runtime.interpreted.debug.events

import org.neo4j.cypher.internal.runtime.debug.events.DebugCategory

enum ConcurrentTransactions extends DebugCategory.ConcurrentTransactions {

  case ProduceNext(context: String)

  case WaitingOnOutputQueue(context: String, pendingTaskCount: Int)

  case NoMoreRowsToPrefetch(context: String)

  case OutputtingRow(context: String)

  case QueuedAnInputBatch(context: String)

  case CreatedNewTask(context: String)

  case WaitingOnRetryQueue(context: String, delay: Long)

  case TimedWaitingOnOutputQueue(context: String, delay: Long)

  case ProcessingTaskResult(context: String, taskResult: String)

  case AddingBatchToRetryQueue(context: String)

  case PendingInputNewBatch(context: String)

  case PendingInputQueuedBatch(context: String)

  case PendingInputNotAvailable(context: String)

  case PendingOutputReady(context: String)

  case PendingOutputNotAvailable(context: String)

  case HavePendingTasks(context: String, pendingTaskCount: Int)

  /** Drained-on-error event. */
  case Drained(detail: String)
}

enum ConcurrentTransactionsWorker extends DebugCategory.ConcurrentTransactionsWorker {

  case Done(worker: String)

  case Exception(worker: String, throwable: Throwable)

  case StartingBatch(worker: String, rowCount: Long)

  case HaveResults(worker: String)
}
