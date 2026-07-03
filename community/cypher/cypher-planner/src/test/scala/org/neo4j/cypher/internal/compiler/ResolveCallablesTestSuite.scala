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
package org.neo4j.cypher.internal.compiler

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.frontend.phases.FieldSignature
import org.neo4j.cypher.internal.frontend.phases.InstrumentedProcedureSignatureResolver
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.planner.spi.DatabaseMode
import org.neo4j.cypher.internal.planner.spi.DatabaseMode.DatabaseMode
import org.neo4j.cypher.internal.planner.spi.NotImplementedPlanContext
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.ProcedureName
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.scalatest.Inside

/**
 * Common test infrastructure shared by [[ResolveCallablesTest]] and [[LocalCallableShadowNotificationTest]].
 */
abstract class ResolveCallablesTestSuite extends CypherPlannerTestSuite with AstConstructionTestSupport with Inside {

  protected val name: ProcedureName = procedureName("my", "proc", "foo")
  protected val signatureInputs: IndexedSeq[FieldSignature] = IndexedSeq(FieldSignature("a", CTInteger))

  protected val signatureOutputs: Option[IndexedSeq[FieldSignature]] =
    Some(IndexedSeq(FieldSignature("x", CTInteger), FieldSignature("y", CTList(CTNode))))

  protected val signature: ProcedureSignature =
    ProcedureSignature(name, signatureInputs, signatureOutputs, None, ProcedureReadOnlyAccess, id = 42)

  def makeResolver(
    procSignatureLookup: ProcedureName => ProcedureSignature = _ => signature,
    funcSignatureLookup: FunctionName => Option[UserFunctionSignature] = _ => None
  ): InstrumentedProcedureSignatureResolver =
    new InstrumentedProcedureSignatureResolver(new TestSignatureResolvingPlanContext(
      procSignatureLookup,
      funcSignatureLookup
    ))
}

class TestSignatureResolvingPlanContext(
  procSignatureLookup: ProcedureName => ProcedureSignature,
  funcSignatureLookup: FunctionName => Option[UserFunctionSignature]
) extends NotImplementedPlanContext {
  override def procedureSignature(name: ProcedureName): ProcedureSignature = procSignatureLookup(name)

  override def functionSignature(name: FunctionName): Option[UserFunctionSignature] = funcSignatureLookup(name)

  override def procedureSignatureVersion: Long = -1

  override def databaseMode: DatabaseMode = DatabaseMode.SINGLE
}
