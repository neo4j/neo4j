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

import scala.compiletime.summonInline

/**
 * Where a debug event was logged from. Captured at compile time by
 * [[CallSite.here]] via lihaoyi sourcecode — no stack walking at runtime.
 *
 * @param enclosing the enclosing definition path, e.g. `org.neo4j...MorselBuffer#put`.
 * @param fileName  the source file name, e.g. `MorselBuffer.scala`.
 * @param line      the 1-based source line of the log call.
 */
final case class CallSite(enclosing: String, fileName: String, line: Int) {

  /** Stack-frame-style rendering with the package path dropped, e.g. `MorselBuffer#put(MorselBuffer.scala:123)`. */
  def shortString: String = s"${enclosing.substring(enclosing.lastIndexOf('.') + 1)}($fileName:$line)"

  override def toString: String = s"$enclosing($fileName:$line)"
}

object CallSite {

  /**
   * Materialises the call site of the point where this call is inlined.
   * Resolution is deferred with [[summonInline]], so a use inside a
   * dropped `inline if` branch — like the one in `Debug.log` when
   * [[Debug.enabled]] is false — summons nothing at all.
   */
  inline def here: CallSite = CallSite(
    summonInline[sourcecode.Enclosing].value,
    summonInline[sourcecode.FileName].value,
    summonInline[sourcecode.Line].value
  )
}
