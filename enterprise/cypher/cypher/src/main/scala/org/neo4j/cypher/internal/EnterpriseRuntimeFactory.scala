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
import org.neo4j.cypher.internal.compatibility.{FallbackRuntime, InterpretedRuntime, ProcedureCallOrSchemaCommandRuntime, TemporaryRuntime}
import org.neo4j.cypher.internal.runtime.compiled.EnterpriseRuntimeContext
import org.opencypher.v9_0.util.InvalidArgumentException

object EnterpriseRuntimeFactory {

  def getRuntime(cypherRuntime: CypherRuntimeOption, useErrorsOverWarnings: Boolean): TemporaryRuntime[EnterpriseRuntimeContext] = cypherRuntime match {
    case CypherRuntimeOption.interpreted =>
      new FallbackRuntime(
        List(new ProcedureCallOrSchemaCommandRuntime(), new InterpretedRuntime(true)),
        CypherRuntimeOption.interpreted)

    case CypherRuntimeOption.slotted =>
      new FallbackRuntime(
        List(new ProcedureCallOrSchemaCommandRuntime(), new SlottedRuntime()),
        CypherRuntimeOption.slotted)

    case CypherRuntimeOption.compiled =>
      new FallbackRuntime(
        List(new ProcedureCallOrSchemaCommandRuntime(), new CompiledRuntime(), new SlottedRuntime()),
        CypherRuntimeOption.compiled)

    case CypherRuntimeOption.morsel =>
      new FallbackRuntime(
        List(new ProcedureCallOrSchemaCommandRuntime(), new MorselRuntime(), new CompiledRuntime(), new SlottedRuntime()),
        CypherRuntimeOption.morsel)

    case CypherRuntimeOption.default =>
      new FallbackRuntime(
        List(new ProcedureCallOrSchemaCommandRuntime(), new CompiledRuntime(), new SlottedRuntime()),
        CypherRuntimeOption.default)

    case runtime if useErrorsOverWarnings =>
      throw new InvalidArgumentException(s"This version of Neo4j does not support requested runtime: $runtime")
  }
}
