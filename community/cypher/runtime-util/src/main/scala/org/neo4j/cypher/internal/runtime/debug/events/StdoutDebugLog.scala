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

import org.neo4j.cypher.internal.runtime.debug.events.Debug.LogContext

/**
 * Default [[DebugLog]] implementation — writes lines to stdout.
 */
final class StdoutDebugLog(
  renderer: DebugEventRenderer = DebugEventRenderer.loaded
) extends DebugLog {

  override def log(event: DebugEvent, context: LogContext): Unit = {
    // Not using println because that is synchronized and can hide parallel problems.
    print(renderer.render(event, context) + "\n")
  }
}

object StdoutDebugLog {
  val default: StdoutDebugLog = new StdoutDebugLog()
}
