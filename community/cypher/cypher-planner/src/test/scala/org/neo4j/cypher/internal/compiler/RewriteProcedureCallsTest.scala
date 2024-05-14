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

import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.Unwind
import org.neo4j.cypher.internal.compiler.phases.RewriteProcedureCalls
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.ProcedureName
import org.neo4j.cypher.internal.frontend.phases.FieldSignature
import org.neo4j.cypher.internal.frontend.phases.InstrumentedProcedureSignatureResolver
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.QualifiedName
import org.neo4j.cypher.internal.frontend.phases.ResolvedCall
import org.neo4j.cypher.internal.frontend.phases.TryRewriteProcedureCalls
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.planner.spi.DatabaseMode
import org.neo4j.cypher.internal.planner.spi.DatabaseMode.DatabaseMode
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

  private val signature =
    ProcedureSignature(qualifiedName, signatureInputs, signatureOutputs, None, ProcedureReadOnlyAccess, id = 42)

  test("should resolve standalone procedure calls") {
    val unresolved = UnresolvedCall(ns, name, None, None)(pos)
    val original = SingleQuery(Seq(unresolved)) _

    val resolver = makeResolver()
    val rewritten = rewriteProcedureCalls(resolver, original)
    val rewrittenTry = tryRewriteProcedureCalls(resolver, original)

    val expected = SingleQuery(
      Seq(
        ResolvedCall(resolver.procedureSignature)(unresolved).coerceArguments.withFakedFullDeclarations,
        Return(
          distinct = false,
          ReturnItems(
            includeExisting = false,
            Seq(
              AliasedReturnItem(v"x", v"x")(pos),
              AliasedReturnItem(v"y", v"y")(pos)
            )
          )(pos),
          None,
          None,
          None
        )(pos)
      )
    )(pos)

    rewritten should equal(expected)
    rewrittenTry should equal(expected)
  }

  test("should resolve in-query procedure calls") {
    val unresolved = UnresolvedCall(ns, name, None, None)(pos)
    val headClause = Unwind(v"x", v"y")(pos)
    val original = SingleQuery(Seq(headClause, unresolved))(pos)

    val resolver = makeResolver()
    val rewritten = rewriteProcedureCalls(resolver, original)
    val rewrittenTry = tryRewriteProcedureCalls(resolver, original)

    val expected =
      SingleQuery(Seq(headClause, ResolvedCall(resolver.procedureSignature)(unresolved).coerceArguments))(pos)

    rewritten should equal(expected)
    rewrittenTry should equal(expected)
  }

  test("TryRewriteProcedureCalls should return original for unresolved procedures") {
    val unresolved = UnresolvedCall(ns, name, None, None)(pos)
    val headClause = Unwind(v"x", v"y")(pos)
    val original = SingleQuery(Seq(headClause, unresolved))(pos)

    val rewrittenTry =
      Try(tryRewriteProcedureCalls(makeResolver(procSignatureLookup = _ => throw new Exception("not found")), original))

    rewrittenTry should matchPattern { case Success(`original`) => }
  }

  test("TryRewriteProcedureCalls should return original for unresolved functions") {
    val headClause = Unwind(function("missing", v"x"), v"y")(pos)
    val original = SingleQuery(Seq(headClause))(pos)

    val rewrittenTry = tryRewriteProcedureCalls(makeResolver(), original)

    rewrittenTry should equal(original)
  }

  test(
    "should not generate a Return clause when resolving a standalone procedure call with no output signature (aka unit procedure)"
  ) {
    val unresolved = UnresolvedCall(ns, name, None, None)(pos)
    val original = SingleQuery(Seq(unresolved))(pos)

    val procLookupNoOutput: QualifiedName => ProcedureSignature = _ => signature.copy(outputSignature = None)

    val resolver = makeResolver(procSignatureLookup = procLookupNoOutput)
    val rewritten = rewriteProcedureCalls(resolver, original)
    val rewrittenTry = tryRewriteProcedureCalls(resolver, original)

    val resolved = ResolvedCall(procLookupNoOutput)(unresolved).coerceArguments.withFakedFullDeclarations
    val expected = SingleQuery(Seq(resolved))(pos)

    rewritten should equal(expected)
    rewrittenTry should equal(expected)
  }

  test(
    "should do nothing when no procedure call is present"
  ) {
    val headClause = Unwind(v"x", v"y")(pos)
    val original = SingleQuery(Seq(headClause))(pos)

    val evaluate = (callable: InstrumentedProcedureSignatureResolver => _) => {
      val resolver = makeResolver()
      callable(resolver)
      resolver.signatureVersionIfResolved
    }

    evaluate(resolver => rewriteProcedureCalls(resolver, original)) shouldBe None
    evaluate(resolver => tryRewriteProcedureCalls(resolver, original)) shouldBe None

  }

  test(
    "should include the procedureSignatureVersion when resolving a procedure call"
  ) {
    val unresolved = UnresolvedCall(ns, name, None, None)(pos)
    val original = SingleQuery(Seq(unresolved))(pos)

    val evaluate = (callable: InstrumentedProcedureSignatureResolver => _) => {
      val resolver = makeResolver()
      callable(resolver)
      resolver.signatureVersionIfResolved
    }

    evaluate(resolver => rewriteProcedureCalls(resolver, original)) shouldBe defined
    evaluate(resolver => tryRewriteProcedureCalls(resolver, original)) shouldBe defined
  }

  def makeResolver(
    procSignatureLookup: QualifiedName => ProcedureSignature = _ => signature,
    funcSignatureLookup: QualifiedName => Option[UserFunctionSignature] = _ => None
  ): InstrumentedProcedureSignatureResolver =
    new InstrumentedProcedureSignatureResolver(new TestSignatureResolvingPlanContext(
      procSignatureLookup,
      funcSignatureLookup
    ))

  def rewriteProcedureCalls(
    resolver: InstrumentedProcedureSignatureResolver,
    original: Query
  ): Query = {
    original.endoRewrite(
      RewriteProcedureCalls.rewriter(resolver)
    )
  }

  def tryRewriteProcedureCalls(
    resolver: InstrumentedProcedureSignatureResolver,
    original: Query
  ): Query = {
    original.endoRewrite(
      TryRewriteProcedureCalls(resolver).rewriter
    )
  }
}

class TestSignatureResolvingPlanContext(
  procSignatureLookup: QualifiedName => ProcedureSignature,
  funcSignatureLookup: QualifiedName => Option[UserFunctionSignature]
) extends NotImplementedPlanContext {
  override def procedureSignature(name: QualifiedName): ProcedureSignature = procSignatureLookup(name)

  override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] = funcSignatureLookup(name)

  override def procedureSignatureVersion: Long = -1

  override def databaseMode: DatabaseMode = DatabaseMode.SINGLE
}
