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
package org.neo4j.cypher.internal

import java.util.Locale

sealed abstract class RuntimeName {
  def name: String
  def toTextOutput: String = name
}

case object InterpretedRuntimeName extends RuntimeName {
  override val name = "INTERPRETED"
}

case object SlottedRuntimeName extends RuntimeName {
  override val name = "SLOTTED"
}

case object PipelinedRuntimeName extends RuntimeName {
  override val name = "PIPELINED"
}

case object ParallelRuntimeName extends RuntimeName {
  override val name = "PARALLEL"
}

case object SchemaRuntimeName extends RuntimeName {
  override val name = "SCHEMA"
}

case object SystemCommandRuntimeName extends RuntimeName {
  override val name = "SYSTEM"
}

object RuntimeName {

  def apply(name: String): RuntimeName = name.toUpperCase(Locale.ROOT) match {
    case InterpretedRuntimeName.name   => InterpretedRuntimeName
    case SlottedRuntimeName.name       => SlottedRuntimeName
    case PipelinedRuntimeName.name     => PipelinedRuntimeName
    case ParallelRuntimeName.name      => ParallelRuntimeName
    case SchemaRuntimeName.name        => SchemaRuntimeName
    case SystemCommandRuntimeName.name => SystemCommandRuntimeName

    case n => throw new IllegalArgumentException(s"$n is not a valid runtime")
  }
}
