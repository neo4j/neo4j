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
import org.neo4j.service.Services

import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
 * Turns [[DebugEvent]]s into text and structured fields. Events are
 * pure data; all rendering lives in one implementation of this trait,
 * discovered from the classpath via [[java.util.ServiceLoader]].
 *
 * The usable implementation is `RuntimeDebugEventRenderer` in
 * pipelined-runtime (the only module that can see every event type). On a
 * classpath without it, sinks fall back to [[DebugEventRenderer.ToString]],
 * which prints the case-class toString. Debug logging is a developer-only
 * facility, so losing pretty rendering on community-only classpaths is
 * acceptable.
 *
 * Rendering happens only when a [[DebugLog]] handles the event — no
 * rendering work is done at the call site.
 */
trait DebugEventRenderer {

  /**
   * Human-readable log line for the event. The [[LogContext]] — call site
   * and logging thread, captured by `Debug.log` — is always supplied;
   * renderings use it where the detail helps.
   */
  def render(event: DebugEvent, context: LogContext): String

  /**
   * Structured payload for the event, derived from event fields.
   */
  def fields(event: DebugEvent, context: LogContext): Seq[(String, LogValue)]
}

object DebugEventRenderer {

  /** Fallback used when no renderer is registered on the classpath. */
  object ToString extends DebugEventRenderer {
    override def render(event: DebugEvent, context: LogContext): String = event.toString

    override def fields(event: DebugEvent, context: LogContext): Seq[(String, LogValue)] =
      Seq("message" -> LogValue.str(event.toString))
  }

  lazy val loaded: DebugEventRenderer =
    Services.loadAll(classOf[DebugEventRenderer]).asScala.headOption.getOrElse(ToString)
}
