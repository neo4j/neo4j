/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.CypherRuntimeOption
import org.neo4j.cypher.internal.compatibility.{FallbackRuntime, InterpretedRuntime, CypherRuntime}

object EnterpriseRuntimeFactory {

  val interpreted = new FallbackRuntime[EnterpriseRuntimeContext](List(InterpretedRuntime), CypherRuntimeOption.interpreted)
  val slotted = new FallbackRuntime[EnterpriseRuntimeContext](List(SlottedRuntime, InterpretedRuntime), CypherRuntimeOption.slotted)
  val compiledWithoutFallback = new FallbackRuntime[EnterpriseRuntimeContext](List(CompiledRuntime), CypherRuntimeOption.compiled)
  val compiled = new FallbackRuntime[EnterpriseRuntimeContext](List(CompiledRuntime, SlottedRuntime, InterpretedRuntime), CypherRuntimeOption.compiled)
  val morselWithoutFallback = new FallbackRuntime[EnterpriseRuntimeContext](List(MorselRuntime), CypherRuntimeOption.morsel)
  val morsel = new FallbackRuntime[EnterpriseRuntimeContext](List(MorselRuntime, CompiledRuntime, SlottedRuntime, InterpretedRuntime), CypherRuntimeOption.morsel)
  val default = new FallbackRuntime[EnterpriseRuntimeContext](List(CompiledRuntime, SlottedRuntime, InterpretedRuntime), CypherRuntimeOption.default)

  def getRuntime(cypherRuntime: CypherRuntimeOption, disallowFallback: Boolean): CypherRuntime[EnterpriseRuntimeContext] =
    cypherRuntime match {
      case CypherRuntimeOption.interpreted => interpreted

      case CypherRuntimeOption.slotted => slotted

      case CypherRuntimeOption.compiled if disallowFallback => compiledWithoutFallback

      case CypherRuntimeOption.compiled => compiled

      case CypherRuntimeOption.morsel if disallowFallback => morselWithoutFallback

      case CypherRuntimeOption.morsel => morsel

      case CypherRuntimeOption.default => default
    }
}
