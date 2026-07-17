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
 * Top-level JSON property a [[StructuredDebugLog]] can write per record.
 * The sink is parameterised by a sequence of these so deployments can
 * tighten or widen their schema without touching the sink itself.
 */
enum StructuredProperty {

  /** Wall-clock millis from the sink's clock — written as a JSON number. */
  case Timestamp

  /** Name of the logging thread, captured by `Debug.log` in the [[LogContext]]. */
  case Thread

  /** Event class tail, e.g. `Scheduling.NextTaskFailed`. */
  case Event

  /** The [[CallSite]] the event was logged from — enclosing definition, file and line. */
  case Site

  /** Human-readable [[DebugEventRenderer.render]] — useful for grep but redundant if `Fields` is enough. */
  case Message

  /** Structured payload from [[DebugEventRenderer.fields]]. */
  case Fields
}
