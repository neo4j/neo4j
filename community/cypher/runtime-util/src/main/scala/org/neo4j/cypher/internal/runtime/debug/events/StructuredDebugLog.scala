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

import java.io.PrintStream

/**
 * [[DebugLog]] implementation that emits one JSON object per event
 * (newline-delimited JSON / NDJSON), suitable for ingestion by log
 * aggregators that expect structured records.
 *
 * The sink is intentionally dumb: it serialises the [[LogValue]] tree
 * the [[DebugEventRenderer]] hands it and does no reflection or
 * type-dispatch on event payloads. That keeps the schema of the
 * structured log under the renderer's control rather than accidentally
 * tied to case-class field layout.
 *
 * Each line has the shape:
 *
 * {{{
 *   {"ts":1735315200000,"thread":"worker-3","event":"Scheduling.NextTaskFailed",
 *    "site":{"enclosing":"org...WorkerRunner#scheduleNextTask","file":"WorkerRunner.scala","line":42},
 *    "fields":{"error":{"class":"java.lang.RuntimeException","message":"boom"},
 *              "threadName":"worker-3"}}
 * }}}
 *
 * Which top-level properties appear, and in which order, is controlled
 * by [[properties]]. The default — `Timestamp, Thread, Event, Site,
 * Fields` — matches what most consumers want; tighten or extend by
 * passing a different sequence. The `event` tag is derived from the
 * event's runtime class so consumers can filter on type without grepping
 * free-form text.
 *
 * @param out        destination for the JSON lines.
 * @param properties top-level JSON properties to emit, in order. Empty
 *                   yields `{}`; repeats emit twice (no de-duping).
 * @param clock      supplies the timestamp written to the `ts` property. Tests
 *                   can pin this to a fixed value.
 * @param renderer   supplies the `message` text and `fields` payload per event.
 */
final class StructuredDebugLog(
  out: PrintStream,
  properties: Seq[StructuredProperty] = StructuredDebugLog.DefaultProperties,
  clock: () => Long = () => System.currentTimeMillis(),
  renderer: DebugEventRenderer = DebugEventRenderer.loaded
) extends DebugLog {

  override def log(event: DebugEvent, context: LogContext): Unit = {
    val sb = new java.lang.StringBuilder(128)
    sb.append('{')
    var first = true
    properties.foreach { p =>
      if (!first) sb.append(',')
      StructuredDebugLog.appendProperty(sb, p, event, context, clock, renderer)
      first = false
    }
    sb.append('}')
    sb.append('\n')
    // Single write to keep concurrent loggers from interleaving partial JSON.
    out.print(sb.toString)
  }
}

object StructuredDebugLog {

  val DefaultProperties: Seq[StructuredProperty] = Seq(
    StructuredProperty.Timestamp,
    StructuredProperty.Thread,
    StructuredProperty.Event,
    StructuredProperty.Site,
    StructuredProperty.Fields
  )

  /** Sink that writes NDJSON to `System.out` with [[DefaultProperties]]. */
  val default: StructuredDebugLog = new StructuredDebugLog(System.out)

  private def appendProperty(
    sb: java.lang.StringBuilder,
    property: StructuredProperty,
    event: DebugEvent,
    context: LogContext,
    clock: () => Long,
    renderer: DebugEventRenderer
  ): Unit = property match {
    case StructuredProperty.Timestamp =>
      sb.append("\"ts\":").append(clock())
    case StructuredProperty.Thread =>
      sb.append("\"thread\":")
      appendString(sb, context.threadName)
    case StructuredProperty.Event =>
      sb.append("\"event\":")
      appendString(sb, eventName(event))
    case StructuredProperty.Site =>
      sb.append("\"site\":{\"enclosing\":")
      appendString(sb, context.site.enclosing)
      sb.append(",\"file\":")
      appendString(sb, context.site.fileName)
      sb.append(",\"line\":").append(context.site.line)
      sb.append('}')
    case StructuredProperty.Message =>
      sb.append("\"message\":")
      appendString(sb, renderer.render(event, context))
    case StructuredProperty.Fields =>
      sb.append("\"fields\":")
      appendObject(sb, renderer.fields(event, context))
  }

  private def eventName(event: DebugEvent): String = event match {
    // Parameterless enum cases are instances of an anonymous subclass of
    // their enum class, so the class name alone is useless — derive the
    // family from the superclass and the case name from toString, which
    // the compiler pins to the case name.
    case e: scala.runtime.EnumValue => s"${className(e.getClass.getSuperclass)}.$e"
    case other                      => className(other.getClass)
  }

  private def className(cls: Class[?]): String = {
    // e.g. "org...events.Scheduling$NextTaskFailed" -> "Scheduling.NextTaskFailed"
    val raw = cls.getName
    val tail = raw.substring(raw.lastIndexOf('.') + 1)
    val trimmed = if (tail.endsWith("$")) tail.substring(0, tail.length - 1) else tail
    trimmed.replace('$', '.')
  }

  private def appendValue(sb: java.lang.StringBuilder, value: LogValue): Unit = value match {
    case LogValue.Null         => sb.append("null")
    case LogValue.Bool(b)      => sb.append(b)
    case LogValue.Num(n)       => sb.append(n)
    case LogValue.Real(n)      => appendFiniteOrString(sb, n)
    case LogValue.Str(s)       => appendString(sb, s)
    case LogValue.Arr(values)  => appendArray(sb, values)
    case LogValue.Obj(entries) => appendObject(sb, entries)
  }

  private def appendArray(sb: java.lang.StringBuilder, values: Seq[LogValue]): Unit = {
    sb.append('[')
    var first = true
    values.foreach { v =>
      if (!first) sb.append(',')
      appendValue(sb, v)
      first = false
    }
    sb.append(']')
  }

  private def appendObject(sb: java.lang.StringBuilder, entries: Seq[(String, LogValue)]): Unit = {
    sb.append('{')
    var first = true
    entries.foreach { case (k, v) =>
      if (!first) sb.append(',')
      appendString(sb, k)
      sb.append(':')
      appendValue(sb, v)
      first = false
    }
    sb.append('}')
  }

  /** JSON has no representation for NaN/Infinity — fall back to a string so output stays parseable. */
  private def appendFiniteOrString(sb: java.lang.StringBuilder, n: Double): Unit = {
    if (java.lang.Double.isFinite(n)) sb.append(n)
    else appendString(sb, n.toString)
  }

  private def appendString(sb: java.lang.StringBuilder, s: String): Unit = {
    sb.append('"')
    var i = 0
    val len = s.length
    while (i < len) {
      val c = s.charAt(i)
      c match {
        case '"'                           => sb.append("\\\"")
        case '\\'                          => sb.append("\\\\")
        case '\n'                          => sb.append("\\n")
        case '\r'                          => sb.append("\\r")
        case '\t'                          => sb.append("\\t")
        case '\b'                          => sb.append("\\b")
        case '\f'                          => sb.append("\\f")
        case ch if ch < 0x20 || ch == 0x7f => sb.append("\\u%04x".format(ch.toInt))
        case ch                            => sb.append(ch)
      }
      i += 1
    }
    sb.append('"')
  }
}
