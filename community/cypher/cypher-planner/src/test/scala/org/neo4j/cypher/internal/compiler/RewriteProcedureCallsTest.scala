/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.Unwind
import org.neo4j.cypher.internal.compiler.phases.RewriteProcedureCalls
import org.neo4j.cypher.internal.compiler.phases.TryRewriteProcedureCalls
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.ProcedureName
import org.neo4j.cypher.internal.logical.plans.FieldSignature
import org.neo4j.cypher.internal.logical.plans.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.logical.plans.ResolvedCall
import org.neo4j.cypher.internal.logical.plans.UserFunctionSignature
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.Inside

import scala.util.Success
import scala.util.Try

class RewriteProcedureCallsTest extends CypherFunSuite with AstConstructionTestSupport with Inside {

  private val ns = Namespace(List("my", "proc"))(pos)
  private val name = ProcedureName("foo")(pos)
  private val qualifiedName = QualifiedName(ns.parts, name.name)
  private val signatureInputs = IndexedSeq(FieldSignature("a", CTInteger))
  private val signatureOutputs = Some(IndexedSeq(FieldSignature("x", CTInteger), FieldSignature("y", CTList(CTNode))))

  private val signature = ProcedureSignature(qualifiedName, signatureInputs, signatureOutputs, None, ProcedureReadOnlyAccess, id = 42)
  private val procLookup: QualifiedName => ProcedureSignature = _ => signature
  private val fcnLookup: QualifiedName => Option[UserFunctionSignature] = _ => None

  private val failingProcLookup: QualifiedName => ProcedureSignature = _ => throw new Exception("not found")
  private val failingFcnLookup: QualifiedName => Option[UserFunctionSignature] = _ => None

  test("should resolve standalone procedure calls") {
    val unresolved = UnresolvedCall(ns, name, None, None)(pos)
    val original = Query(None, SingleQuery(Seq(unresolved))_)(pos)

    val rewritten = rewriteProcedureCalls(procLookup, fcnLookup, original)
    val rewrittenTry = tryRewriteProcedureCalls(procLookup, fcnLookup, original)

    val expected = Query(None, SingleQuery(
      Seq(ResolvedCall(procLookup)(unresolved).coerceArguments.withFakedFullDeclarations,
        Return(distinct = false,
          ReturnItems(includeExisting = false,
            Seq(
              AliasedReturnItem(varFor("x"), varFor("x"))(pos, isAutoAliased = false),
              AliasedReturnItem(varFor("y"), varFor("y"))(pos, isAutoAliased = false)))(pos),
          None, None, None)(pos)))(pos))(pos)

    rewritten should equal(expected)
    rewrittenTry should equal(expected)
  }

  test("should resolve in-query procedure calls") {
    val unresolved = UnresolvedCall(ns, name, None, None)(pos)
    val headClause = Unwind(varFor("x"), varFor("y"))(pos)
    val original = Query(None, SingleQuery(Seq(headClause, unresolved))_)(pos)

    val rewritten = rewriteProcedureCalls(procLookup, fcnLookup, original)
    val rewrittenTry = tryRewriteProcedureCalls(procLookup, fcnLookup, original)

    val expected = Query(None, SingleQuery(Seq(headClause, ResolvedCall(procLookup)(unresolved).coerceArguments))_)(pos)

    rewritten should equal(expected)
    rewrittenTry should equal(expected)
  }

  test("TryRewriteProcedureCalls should return original for unresolved procedures") {
    val unresolved = UnresolvedCall(ns, name, None, None)(pos)
    val headClause = Unwind(varFor("x"), varFor("y"))(pos)
    val original = Query(None, SingleQuery(Seq(headClause, unresolved))_)(pos)

    val rewrittenTry = Try(tryRewriteProcedureCalls(failingProcLookup, failingFcnLookup, original))

    rewrittenTry should matchPattern { case Success(`original`) => }
  }

  test("TryRewriteProcedureCalls should return original for unresolved functions") {
    val headClause = Unwind(function("missing", varFor("x")), varFor("y"))(pos)
    val original = Query(None, SingleQuery(Seq(headClause))_)(pos)

    val rewrittenTry = tryRewriteProcedureCalls(failingProcLookup, failingFcnLookup, original)

    rewrittenTry should equal(original)
  }

  test("should not generate a Return clause when resolving a standalone procedure call with no output signature (aka unit procedure)") {
    val unresolved = UnresolvedCall(ns, name, None, None)(pos)
    val original = Query(None, SingleQuery(Seq(unresolved))(pos))(pos)

    val procLookupNoOutput: QualifiedName => ProcedureSignature = _ => signature.copy(outputSignature = None)

    val rewritten = rewriteProcedureCalls(procLookupNoOutput, fcnLookup, original)
    val rewrittenTry = tryRewriteProcedureCalls(procLookupNoOutput, fcnLookup, original)

    val resolved = ResolvedCall(procLookupNoOutput)(unresolved).coerceArguments.withFakedFullDeclarations
    val expected = Query(None, SingleQuery(Seq(resolved))(pos))(pos)

    rewritten should equal(expected)
    rewrittenTry should equal(expected)
  }

  def rewriteProcedureCalls(procSignatureLookup: QualifiedName => ProcedureSignature,
                            funcSignatureLookup: QualifiedName => Option[UserFunctionSignature],
                            original: Query): Query = {
    original.endoRewrite(
      RewriteProcedureCalls.rewriter(new TestSignatureResolvingPlanContext(procSignatureLookup, funcSignatureLookup))
    )
  }

  def tryRewriteProcedureCalls(procSignatureLookup: QualifiedName => ProcedureSignature,
                            funcSignatureLookup: QualifiedName => Option[UserFunctionSignature],
                            original: Query): Query = {
    val context = new TestSignatureResolvingPlanContext(procSignatureLookup, funcSignatureLookup)
    original.endoRewrite(
      TryRewriteProcedureCalls(context).rewriter
    )
  }
}

class TestSignatureResolvingPlanContext(procSignatureLookup: QualifiedName => ProcedureSignature,
                                        funcSignatureLookup: QualifiedName => Option[UserFunctionSignature])
  extends NotImplementedPlanContext {
  override def procedureSignature(name: QualifiedName): ProcedureSignature = procSignatureLookup(name)

  override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] = funcSignatureLookup(name)
}
