/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1

import org.neo4j.cypher.internal.frontend.v3_1.ast._
import org.neo4j.cypher.internal.frontend.v3_1.symbols._
import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_1.{FieldSignature, ProcedureReadOnlyAccess, ProcedureSignature, QualifiedName, ResolvedCall, UserFunctionSignature}

class RewriteProcedureCallsTest extends CypherFunSuite with AstConstructionTestSupport {

  val ns = Namespace(List("my", "proc"))(pos)
  val name = ProcedureName("foo")(pos)
  val qualifiedName = QualifiedName(ns.parts, name.name)
  val signatureInputs = IndexedSeq(FieldSignature("a", CTInteger))
  val signatureOutputs = Some(IndexedSeq(FieldSignature("x", CTInteger), FieldSignature("y", CTList(CTNode))))

  val signature = ProcedureSignature(qualifiedName, signatureInputs, signatureOutputs, None, ProcedureReadOnlyAccess(Array.empty[String]))
  val procLookup: (QualifiedName) => ProcedureSignature = _ => signature
  val fcnLookup: (QualifiedName) => Option[UserFunctionSignature] = _ => None

  test("should resolve standalone procedure calls") {
    val unresolved = UnresolvedCall(ns, name, None, None)(pos)
    val original = Query(None, SingleQuery(Seq(unresolved))_)(pos)

    val rewritten = rewriteProcedureCalls(procLookup, fcnLookup)(original)

    rewritten should equal(
      Query(None, SingleQuery(Seq(ResolvedCall(procLookup)(unresolved).coerceArguments.withFakedFullDeclarations))_)(pos)
    )
  }

  test("should resolve in-query procedure calls") {
    val unresolved = UnresolvedCall(ns, name, None, None)(pos)
    val headClause = Unwind(varFor("x"), varFor("y"))(pos)
    val original = Query(None, SingleQuery(Seq(headClause, unresolved))_)(pos)

    val rewritten = rewriteProcedureCalls(procLookup, fcnLookup)(original)

    rewritten should equal(
      Query(None, SingleQuery(Seq(headClause, ResolvedCall(procLookup)(unresolved).coerceArguments))_)(pos)
    )
  }
}
