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
package org.neo4j.cypher.internal.runtime.debug.events

/**
 * Data describing something interesting that happened in the runtime.
 *
 * Events are pure data: the typed payload worth logging, nothing about how
 * it prints. Text and structured renderings live in [[DebugEventRenderer]],
 * whose production implementation is one big match in pipelined-runtime —
 * the only module that can see every event type.
 */
trait DebugEvent

object DebugCategory {
  trait Asm extends DebugEvent
  trait BatchFormation extends DebugEvent
  trait Buffers extends DebugEvent
  trait Cleanup extends DebugEvent
  trait ConcurrentTransactions extends DebugEvent
  trait ConcurrentTransactionsWorker extends DebugEvent
  trait Cursors extends DebugEvent
  trait ErrorHandling extends DebugEvent
  trait GeneratedIrCode extends DebugEvent
  trait Locks extends DebugEvent
  trait MemoryTracking extends DebugEvent
  trait MorselReuse extends DebugEvent
  trait PhysicalPlanning extends DebugEvent
  trait Pipelines extends DebugEvent
  trait Progress extends DebugEvent
  trait Queries extends DebugEvent
  trait Scheduling extends DebugEvent
  trait Tracker extends DebugEvent
  trait TransactionalContext extends DebugEvent
  trait Workers extends DebugEvent

  object Workers {

    /**
     * Separate marker trait to dynamically unmute Workers logging on stall events.
     * If this is enabled in [[Debug.isEnabled]], [[Workers]] should be too
     */
    trait OnlyWhenStalled extends DebugEvent
  }

}

trait DebugLog {
  def log(event: DebugEvent, context: Debug.LogContext): Unit
}
