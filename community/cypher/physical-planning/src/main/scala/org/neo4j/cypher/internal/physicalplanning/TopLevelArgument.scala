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
package org.neo4j.cypher.internal.physicalplanning

/**
  * The top level argument is the argument at the top level of a logical plan.
  * Here, there is no outer apply, meaning that only a single execution will happen,
  * started by a single argument row. This gives us opportunity to specialize handling
  * of arguments at the top level, and optimize storing of the logical argument.
  */
object TopLevelArgument {

  private class TopLevelArgumentException(argument: Long) extends RuntimeException(
        "The top level argument has to be 0, but got " + argument
      )

  def assertTopLevelArgument(argument: Long): Unit = {
    if (argument != _VALUE) {
      throw new TopLevelArgumentException(argument)
    }
  }

  final private[this] val _VALUE: Long = 0

  final val VALUE: Long = _VALUE
  final val SLOT_OFFSET: Int = -1
  final val UNDEFINED_SLOT_OFFSET: Int = -2
}
