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
 * Typed value model for structured log payloads. Events build a tree of
 * [[LogValue]]s in [[DebugCategory.fields]]; sinks render that tree
 * directly without reflecting over case-class shape. Authors get explicit
 * control over how each field encodes, and the sink can stay free of
 * type-dispatch logic.
 */
sealed trait LogValue

object LogValue {

  case object Null extends LogValue
  final case class Str(value: String) extends LogValue
  final case class Num(value: Long) extends LogValue
  final case class Real(value: Double) extends LogValue
  final case class Bool(value: Boolean) extends LogValue
  final case class Arr(values: Seq[LogValue]) extends LogValue
  final case class Obj(entries: Seq[(String, LogValue)]) extends LogValue

  /** Smart constructor: `null` strings collapse to [[Null]] so authors don't need to guard. */
  def str(value: String): LogValue = if (value eq null) Null else Str(value)

  def num(value: Long): LogValue = Num(value)
  def num(value: Int): LogValue = Num(value.toLong)
  def real(value: Double): LogValue = Real(value)
  def bool(value: Boolean): LogValue = Bool(value)

  def arr(values: Seq[LogValue]): LogValue = Arr(values)
  def strs(values: Seq[String]): LogValue = Arr(values.map(str))
  def ints(values: Seq[Int]): LogValue = Arr(values.map(n => Num(n.toLong)))
  def longs(values: Seq[Long]): LogValue = Arr(values.map(Num.apply))

  /**
   * Compact encoding for [[Throwable]] — class name and message only, no
   * stack trace. Stacks are rarely useful in structured search and explode
   * the line size; callers that want them can build a richer object
   * themselves.
   */
  def err(t: Throwable): LogValue =
    if (t eq null) Null
    else Obj(Seq("class" -> Str(t.getClass.getName), "message" -> str(t.getMessage)))

  /** Use sparingly — opaque values lose their type. Prefer named fields with concrete types. */
  def opaque(value: Any): LogValue = if (value == null) Null else Str(value.toString)
}
