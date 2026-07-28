/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.compiler

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.frontend.phases.FieldSignature
import org.neo4j.cypher.internal.frontend.phases.InstrumentedProcedureSignatureResolver
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.ProcedureName
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.Inside

/**
 * Common test infrastructure shared by [[ResolveCallablesTest]] and [[LocalCallableShadowNotificationTest]].
 */
abstract class ResolveCallablesTestSuite extends CypherFunSuite with AstConstructionTestSupport with Inside {

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
    new InstrumentedProcedureSignatureResolver(new TestSignatureResolver(
      procSignatureLookup,
      funcSignatureLookup
    ) {})
}
