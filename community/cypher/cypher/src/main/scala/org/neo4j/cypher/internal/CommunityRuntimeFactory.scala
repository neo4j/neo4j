/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.{CypherRuntimeOption, InvalidArgumentException}
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.CommunityRuntimeContext
import org.neo4j.cypher.internal.compatibility.{CypherRuntime, FallbackRuntime, InterpretedRuntime, UnknownRuntime}

object CommunityRuntimeFactory {

  val interpreted = new FallbackRuntime[CommunityRuntimeContext](List(InterpretedRuntime), CypherRuntimeOption.interpreted)
  val default = new FallbackRuntime[CommunityRuntimeContext](List(InterpretedRuntime), CypherRuntimeOption.default)

  def getRuntime(cypherRuntime: CypherRuntimeOption, disallowFallback: Boolean): CypherRuntime[CommunityRuntimeContext] =
    cypherRuntime match {
      case CypherRuntimeOption.interpreted => interpreted

      case CypherRuntimeOption.default => default

      case unsupported if disallowFallback =>
        throw new InvalidArgumentException(s"This version of Neo4j does not support requested runtime: $unsupported")

      case unsupported => new FallbackRuntime[CommunityRuntimeContext](List(UnknownRuntime, InterpretedRuntime), unsupported)
    }
}
