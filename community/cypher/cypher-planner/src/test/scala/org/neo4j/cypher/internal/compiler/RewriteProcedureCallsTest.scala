/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler

import org.neo4j.cypher.internal.compiler.phases.RewriteProcedureCalls
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.expressions.{Namespace, ProcedureName}
import org.neo4j.cypher.internal.v4_0.util.symbols._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class RewriteProcedureCallsTest extends CypherFunSuite with AstConstructionTestSupport {

  private val ns = Namespace(List("my", "proc"))(pos)
  private val name = ProcedureName("foo")(pos)
  private val qualifiedName = QualifiedName(ns.parts, name.name)
  private val signatureInputs = IndexedSeq(FieldSignature("a", CTInteger))
  private val signatureOutputs = Some(IndexedSeq(FieldSignature("x", CTInteger), FieldSignature("y", CTList(CTNode))))

  private val signature = ProcedureSignature(qualifiedName, signatureInputs, signatureOutputs, None, ProcedureReadOnlyAccess(Array.empty[String]), id = 42)
  private val procLookup: QualifiedName => ProcedureSignature = _ => signature
  private val fcnLookup: QualifiedName => Option[UserFunctionSignature] = _ => None

  test("should resolve standalone procedure calls") {
    val unresolved = UnresolvedCall(ns, name, None, None)(pos)
    val original = Query(None, SingleQuery(Seq(unresolved))_)(pos)

    val rewritten = rewriteProcedureCalls(procLookup, fcnLookup, original)

    rewritten should equal(
      Query(None, SingleQuery(
        Seq(ResolvedCall(procLookup)(unresolved).coerceArguments.withFakedFullDeclarations,
            Return(distinct = false,
                   ReturnItems(includeExisting = false,
                               Seq(
                                 AliasedReturnItem(varFor("x"), varFor("x"))(pos),
                                 AliasedReturnItem(varFor("y"), varFor("y"))(pos)))(pos),
                   None, None, None)(pos)))(pos))(pos))
  }

  test("should resolve in-query procedure calls") {
    val unresolved = UnresolvedCall(ns, name, None, None)(pos)
    val headClause = Unwind(varFor("x"), varFor("y"))(pos)
    val original = Query(None, SingleQuery(Seq(headClause, unresolved))_)(pos)

    val rewritten = rewriteProcedureCalls(procLookup, fcnLookup, original)

    rewritten should equal(
      Query(None, SingleQuery(Seq(headClause, ResolvedCall(procLookup)(unresolved).coerceArguments))_)(pos)
    )
  }

  def rewriteProcedureCalls(procSignatureLookup: QualifiedName => ProcedureSignature,
                            funcSignatureLookup: QualifiedName => Option[UserFunctionSignature],
                            original: Query): Query = {
    original.endoRewrite(
      RewriteProcedureCalls.rewriter(new TestSignatureResolvingPlanContext(procSignatureLookup, funcSignatureLookup))
    )
  }
}

class TestSignatureResolvingPlanContext(procSignatureLookup: QualifiedName => ProcedureSignature,
                                        funcSignatureLookup: QualifiedName => Option[UserFunctionSignature])
  extends NotImplementedPlanContext {
  override def procedureSignature(name: QualifiedName): ProcedureSignature = procSignatureLookup(name)

  override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] = funcSignatureLookup(name)
}
