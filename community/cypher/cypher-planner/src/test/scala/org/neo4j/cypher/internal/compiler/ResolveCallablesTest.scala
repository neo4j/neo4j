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
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.FreeProjection
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.Unwind
import org.neo4j.cypher.internal.frontend.helpers.TestContext
import org.neo4j.cypher.internal.frontend.helpers.TestState
import org.neo4j.cypher.internal.frontend.phases.InstrumentedProcedureSignatureResolver
import org.neo4j.cypher.internal.frontend.phases.LocalDefinitionsDirectory
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.ResolvedNonLocalCall
import org.neo4j.cypher.internal.frontend.phases.StrictResolveCallables
import org.neo4j.cypher.internal.frontend.phases.TryResolveCallables
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ScopeSurveyor
import org.neo4j.cypher.internal.util.ProcedureName

import scala.util.Success
import scala.util.Try

class ResolveCallablesTest extends ResolveCallablesTestSuite {

  test("should resolve standalone procedure calls") {
    val unresolved = UnresolvedCall(name, None, None, isStandalone = true)(pos)
    val original = SingleQuery(Seq(unresolved))(pos)

    val resolver = makeResolver()
    val rewritten = strictResolveCallables(resolver, original)
    val rewrittenTry = tryResolveCallables(resolver, original)

    val expected = SingleQuery(
      Seq(
        ResolvedNonLocalCall(resolver.procedureSignature)(unresolved).coerceArguments.withFakedFullDeclarations,
        Return(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(v"x", v"x")(pos),
              AliasedReturnItem(v"y", v"y")(pos)
            )
          )(pos),
          None,
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
    val unresolved = UnresolvedCall(name, None, None, isStandalone = false)(pos)
    val headClause = Unwind(v"x", v"y")(pos)
    val original = SingleQuery(Seq(headClause, unresolved))(pos)

    val resolver = makeResolver()
    val rewritten = strictResolveCallables(resolver, original)
    val rewrittenTry = tryResolveCallables(resolver, original)

    val expected =
      SingleQuery(Seq(headClause, ResolvedNonLocalCall(resolver.procedureSignature)(unresolved).coerceArguments))(pos)

    rewritten should equal(expected)
    rewrittenTry should equal(expected)
  }

  test("TryResolveCallables should return original for unresolved procedures") {
    val unresolved = UnresolvedCall(name, None, None, isStandalone = false)(pos)
    val headClause = Unwind(v"x", v"y")(pos)
    val original = SingleQuery(Seq(headClause, unresolved))(pos)

    val rewrittenTry =
      Try(tryResolveCallables(makeResolver(procSignatureLookup = _ => throw new Exception("not found")), original))

    rewrittenTry should matchPattern { case Success(`original`) => }
  }

  test("TryResolveCallables should return original for unresolved functions") {
    val headClause = Unwind(function("missing", v"x"), v"y")(pos)
    val original = SingleQuery(Seq(headClause))(pos)

    val rewrittenTry = tryResolveCallables(makeResolver(), original)

    rewrittenTry should equal(original)
  }

  test(
    "should not generate a Return clause when resolving a standalone procedure call with no output signature (aka unit procedure)"
  ) {
    val unresolved = UnresolvedCall(name, None, None, isStandalone = true)(pos)
    val original = SingleQuery(Seq(unresolved))(pos)

    val procLookupNoOutput: ProcedureName => ProcedureSignature = _ => signature.copy(outputSignature = None)

    val resolver = makeResolver(procSignatureLookup = procLookupNoOutput)
    val rewritten = strictResolveCallables(resolver, original)
    val rewrittenTry = tryResolveCallables(resolver, original)

    val resolved = ResolvedNonLocalCall(procLookupNoOutput)(unresolved).coerceArguments.withFakedFullDeclarations
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

    evaluate(resolver => strictResolveCallables(resolver, original)) shouldBe None
    evaluate(resolver => tryResolveCallables(resolver, original)) shouldBe None

  }

  test(
    "should include the procedureSignatureVersion when resolving a procedure call"
  ) {
    val unresolved = UnresolvedCall(name, None, None, isStandalone = true)(pos)
    val original = SingleQuery(Seq(unresolved))(pos)

    val evaluate = (callable: InstrumentedProcedureSignatureResolver => _) => {
      val resolver = makeResolver()
      callable(resolver)
      resolver.signatureVersionIfResolved
    }

    evaluate(resolver => strictResolveCallables(resolver, original)) shouldBe defined
    evaluate(resolver => tryResolveCallables(resolver, original)) shouldBe defined
  }

  private val context = TestContext()

  def strictResolveCallables(
    resolver: InstrumentedProcedureSignatureResolver,
    original: Query
  ): Query = {
    val from = TestState(
      Some(original),
      maybeLocalDefinitions = Some(LocalDefinitionsDirectory.empty)
    )
    original.endoRewrite(
      StrictResolveCallables(resolver).rewriter(ScopeSurveyor.process(from, context), context)
    )
  }

  def tryResolveCallables(
    resolver: InstrumentedProcedureSignatureResolver,
    original: Query
  ): Query = {
    val from = TestState(
      Some(original),
      maybeLocalDefinitions = Some(LocalDefinitionsDirectory.empty)
    )
    original.endoRewrite(
      TryResolveCallables(resolver).rewriter(ScopeSurveyor.process(from, context), context)
    )
  }
}
