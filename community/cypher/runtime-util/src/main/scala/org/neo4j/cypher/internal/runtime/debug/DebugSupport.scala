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
package org.neo4j.cypher.internal.runtime.debug

object DebugSupport {

  /** DEBUG CONFIGURATION **/

  final val DEBUG_GENERATED_SOURCE_CODE = false
  final val DEBUG_GENERATED_IR_CODE = false

  /** COLORS AND FORMATTING **/

  final val Black = "\u001b[30m"
  final val Red = "\u001b[31m"
  final val Green = "\u001b[32m"
  final val Yellow = "\u001b[33m"
  final val BrightYellow = "\u001b[33;1m"
  final val Blue = "\u001b[34m"
  final val BrightBlue = "\u001b[34;1m"
  final val Magenta = "\u001b[35m"
  final val Cyan = "\u001b[36m"
  final val White = "\u001b[37m" // grey, really

  final val Bold = "\u001b[1m"
  final val Underline = "\u001b[4m"
  final val Reversed = "\u001b[7m"

  final val Reset = "\u001b[0m"
}
