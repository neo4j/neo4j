/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_0

import org.neo4j.cypher.internal.compiler.v3_0.ast.ResolvedCall
import org.neo4j.cypher.internal.compiler.v3_0.spi.{FieldSignature, ProcedureReadOnlyAccess, ProcedureSignature, QualifiedProcedureName}
import org.neo4j.cypher.internal.frontend.v3_0.ast._
import org.neo4j.cypher.internal.frontend.v3_0.symbols._
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite

class RewriteProcedureCallsTest extends CypherFunSuite with AstConstructionTestSupport {

  val ns = ProcedureNamespace(List("my", "proc"))(pos)
  val name = ProcedureName("foo")(pos)
  val qualifiedName = QualifiedProcedureName(ns.parts, name.name)
  val signatureInputs = Seq(FieldSignature("a", CTInteger))
  val signatureOutputs = Some(Seq(FieldSignature("x", CTInteger), FieldSignature("y", CTList(CTNode))))
  val signature = ProcedureSignature(qualifiedName, signatureInputs, signatureOutputs, ProcedureReadOnlyAccess)
  val lookup: (QualifiedProcedureName) => ProcedureSignature = _ => signature

  test("should resolve standalone procedure calls") {
    val unresolved = UnresolvedCall(ns, name, None, None)(pos)
    val original = Query(None, SingleQuery(Seq(unresolved))_)(pos)

    val rewritten = rewriteProcedureCalls(lookup)(original)

    rewritten should equal(
      Query(None, SingleQuery(Seq(ResolvedCall(lookup)(unresolved).coerceArguments.withFakedFullDeclarations))_)(pos)
    )
  }

  test("should resolve in-query procedure calls") {
    val unresolved = UnresolvedCall(ns, name, None, None)(pos)
    val headClause = Unwind(varFor("x"), varFor("y"))(pos)
    val original = Query(None, SingleQuery(Seq(headClause, unresolved))_)(pos)

    val rewritten = rewriteProcedureCalls(lookup)(original)

    rewritten should equal(
      Query(None, SingleQuery(Seq(headClause, ResolvedCall(lookup)(unresolved).coerceArguments))_)(pos)
    )
  }
}
