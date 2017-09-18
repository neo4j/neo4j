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
package org.neo4j.cypher.internal.compiler.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.symbols._
import org.neo4j.cypher.internal.frontend.v3_4.{SemanticCheckResult, SemanticState}
import org.neo4j.cypher.internal.v3_4.logical.plans._

class CallClauseTest extends CypherFunSuite with AstConstructionTestSupport {

  val ns = Namespace(List("my", "proc"))(pos)
  val name = ProcedureName("foo")(pos)
  val qualifiedName = QualifiedName(ns.parts, name.name)

  test("should resolve CALL my.proc.foo") {
    val unresolved = UnresolvedCall(ns, name, None, None)(pos)
    val signatureInputs = IndexedSeq(FieldSignature("a", CTInteger))
    val signatureOutputs = Some(IndexedSeq(FieldSignature("x", CTInteger), FieldSignature("y", CTList(CTNode))))
    val signature = ProcedureSignature(qualifiedName, signatureInputs, signatureOutputs, None, ProcedureReadOnlyAccess(Array.empty))
    val callArguments = IndexedSeq(Parameter("a", CTAny)(pos))
    val callResults = IndexedSeq(ProcedureResultItem(varFor("x"))(pos), ProcedureResultItem(varFor("y"))(pos))

    val resolved = ResolvedCall(_ => signature)(unresolved)

    resolved should equal(
      ResolvedCall(
        signature,
        callArguments,
        callResults,
        declaredArguments = false,
        declaredResults = false
      )(pos)
    )

    QualifiedName(unresolved) should equal(resolved.qualifiedName)
    resolved.callResultTypes should equal(Seq("x" -> CTInteger, "y" -> CTList(CTNode)))
    resolved.callResultIndices should equal(Seq(0 -> "x", 1 -> "y"))
  }

  test("should resolve void CALL my.proc.foo") {
    val unresolved = UnresolvedCall(ns, name, None, None)(pos)
    val signatureInputs = IndexedSeq(FieldSignature("a", CTInteger))
    val signatureOutputs = None
    val signature = ProcedureSignature(qualifiedName, signatureInputs, signatureOutputs, None, ProcedureReadOnlyAccess(Array.empty))
    val callArguments = Seq(Parameter("a", CTAny)(pos))
    val callResults = IndexedSeq.empty

    val resolved = ResolvedCall(_ => signature)(unresolved)

    resolved should equal(
      ResolvedCall(
        signature,
        callArguments,
        callResults,
        declaredArguments = false,
        declaredResults = false
      )(pos)
    )

    QualifiedName(unresolved) should equal(resolved.qualifiedName)
    resolved.callResultTypes should equal(Seq.empty)
    resolved.callResultIndices should equal(Seq.empty)
  }

  test("should resolve CALL my.proc.foo YIELD x, y") {
    val signatureInputs = IndexedSeq(FieldSignature("a", CTInteger))
    val signatureOutputs = Some(IndexedSeq(FieldSignature("x", CTInteger), FieldSignature("y", CTList(CTNode))))
    val signature = ProcedureSignature(qualifiedName, signatureInputs, signatureOutputs, None, ProcedureReadOnlyAccess(Array.empty))
    val callArguments = Seq(Parameter("a", CTAny)(pos))
    val callResults = IndexedSeq(ProcedureResultItem(varFor("x"))(pos), ProcedureResultItem(varFor("y"))(pos))
    val unresolved = UnresolvedCall(ns, name, None, Some(ProcedureResult(callResults)(pos)))(pos)

    val resolved = ResolvedCall(_ => signature)(unresolved)

    resolved should equal(
      ResolvedCall(
        signature,
        callArguments,
        callResults,
        declaredArguments = false,
        declaredResults = true
      )(pos)
    )

    QualifiedName(unresolved) should equal(resolved.qualifiedName)
    resolved.callResultTypes should equal(Seq("x" -> CTInteger, "y" -> CTList(CTNode)))
    resolved.callResultIndices should equal(Seq(0 -> "x", 1 -> "y"))
  }

  test("should resolve CALL my.proc.foo(a)") {
    val signatureInputs = IndexedSeq(FieldSignature("a", CTInteger))
    val signatureOutputs = Some(IndexedSeq(FieldSignature("x", CTInteger), FieldSignature("y", CTList(CTNode))))
    val signature = ProcedureSignature(qualifiedName, signatureInputs, signatureOutputs, None, ProcedureReadOnlyAccess(Array.empty))
    val callArguments = Seq(Parameter("a", CTAny)(pos))
    val callResults = IndexedSeq(ProcedureResultItem(varFor("x"))(pos), ProcedureResultItem(varFor("y"))(pos))
    val unresolved = UnresolvedCall(ns, name, Some(callArguments), None)(pos)

    val resolved = ResolvedCall(_ => signature)(unresolved)

    resolved should equal(
      ResolvedCall(
        signature,
        callArguments,
        callResults,
        declaredArguments = true,
        declaredResults = false
      )(pos)
    )

    QualifiedName(unresolved) should equal(resolved.qualifiedName)
    resolved.callResultTypes should equal(Seq("x" -> CTInteger, "y" -> CTList(CTNode)))
    resolved.callResultIndices should equal(Seq(0 -> "x", 1 -> "y"))
  }

  test("should resolve void CALL my.proc.foo(a)") {
    val signatureInputs = IndexedSeq(FieldSignature("a", CTInteger))
    val signatureOutputs = None
    val signature = ProcedureSignature(qualifiedName, signatureInputs, signatureOutputs, None, ProcedureReadOnlyAccess(Array.empty))
    val callArguments = Seq(Parameter("a", CTAny)(pos))
    val callResults = IndexedSeq.empty
    val unresolved = UnresolvedCall(ns, name, Some(callArguments), None)(pos)

    val resolved = ResolvedCall(_ => signature)(unresolved)

    resolved should equal(
      ResolvedCall(
        signature,
        callArguments,
        callResults,
        declaredArguments = true,
        declaredResults = false
      )(pos)
    )

    QualifiedName(unresolved) should equal(resolved.qualifiedName)
    resolved.callResultTypes should equal(Seq.empty)
    resolved.callResultIndices should equal(Seq.empty)
  }

  test("should resolve CALL my.proc.foo(a) YIELD x, y AS z") {
    val signatureInputs = IndexedSeq(FieldSignature("a", CTInteger))
    val signatureOutputs = Some(IndexedSeq(FieldSignature("x", CTInteger), FieldSignature("y", CTList(CTNode))))
    val signature = ProcedureSignature(qualifiedName, signatureInputs, signatureOutputs, None, ProcedureReadOnlyAccess(Array.empty))
    val callArguments = Seq(Parameter("a", CTAny)(pos))
    val callResults = IndexedSeq(
      ProcedureResultItem(varFor("x"))(pos),
      ProcedureResultItem(ProcedureOutput("y")(pos), varFor("z"))(pos)
    )
    val unresolved = UnresolvedCall(ns, name, Some(callArguments), Some(ProcedureResult(callResults)(pos)))(pos)

    val resolved = ResolvedCall(_ => signature)(unresolved)

    resolved should equal(
      ResolvedCall(
        signature,
        callArguments,
        callResults,
        declaredArguments = true,
        declaredResults = true
      )(pos)
    )
    resolved.callResultTypes should equal(Seq("x" -> CTInteger, "z" -> CTList(CTNode)))
    resolved.callResultIndices should equal(Seq(0 -> "x", 1 -> "z"))
  }

  test("pretends to be based on user-declared arguments and results upon request") {
    val signature = ProcedureSignature(qualifiedName, IndexedSeq.empty, Some(IndexedSeq.empty), None, ProcedureReadOnlyAccess(Array.empty))
    val call = ResolvedCall(signature, null, null, declaredArguments = false, declaredResults = false)(pos)

    call.withFakedFullDeclarations.declaredArguments should be(true)
    call.withFakedFullDeclarations.declaredResults should be(true)
  }

  test("adds coercion of arguments to signature types upon request") {
    val signatureInputs = IndexedSeq(FieldSignature("a", CTInteger))
    val signatureOutputs = Some(IndexedSeq(FieldSignature("x", CTInteger), FieldSignature("y", CTList(CTNode))))
    val signature = ProcedureSignature(qualifiedName, signatureInputs, signatureOutputs, None, ProcedureReadOnlyAccess(Array.empty))
    val callArguments = Seq(Parameter("a", CTAny)(pos))
    val callResults = IndexedSeq(
      ProcedureResultItem(varFor("x"))(pos),
      ProcedureResultItem(ProcedureOutput("y")(pos), varFor("z"))(pos)
    )
    val unresolved = UnresolvedCall(ns, name, Some(callArguments), Some(ProcedureResult(callResults)(pos)))(pos)
    val resolved = ResolvedCall(_ => signature)(unresolved)

    val coerced = resolved.coerceArguments

    coerced should equal(
      ResolvedCall(
        signature,
        Seq(CoerceTo(Parameter("a", CTAny)(pos), CTInteger)),
        callResults,
        declaredArguments = true,
        declaredResults = true
      )(pos)
    )
    coerced.callResultTypes should equal(Seq("x" -> CTInteger, "z" -> CTList(CTNode)))
    coerced.callResultIndices should equal(Seq(0 -> "x", 1 -> "z"))
  }

  test("should verify number of arguments during semantic checking of resolved calls") {
    val signatureInputs = IndexedSeq(FieldSignature("a", CTInteger))
    val signatureOutputs = Some(IndexedSeq(FieldSignature("x", CTInteger), FieldSignature("y", CTList(CTNode))))
    val signature = ProcedureSignature(qualifiedName, signatureInputs, signatureOutputs, None, ProcedureReadOnlyAccess(Array.empty))
    val callArguments = Seq.empty
    val callResults = IndexedSeq(
      ProcedureResultItem(varFor("x"))(pos),
      ProcedureResultItem(ProcedureOutput("y")(pos), varFor("z"))(pos)
    )
    val unresolved = UnresolvedCall(ns, name, Some(callArguments), Some(ProcedureResult(callResults)(pos)))(pos)
    val resolved = ResolvedCall(_ => signature)(unresolved)

    val toList: List[String] = errorTexts(resolved.semanticCheck(SemanticState.clean)).toList
    toList should equal(List(
      """Procedure call does not provide the required number of arguments: got 0 expected 1.
        |
        |Procedure my.proc.foo has signature: my.proc.foo(a :: INTEGER?) :: x :: INTEGER?, y :: LIST? OF NODE?
        |meaning that it expects 1 argument of type INTEGER? (line 1, column 0 (offset: 0))""".stripMargin
    ))
  }

  test("should verify that result variables are unique during semantic checking of resolved calls") {
    val signatureInputs = IndexedSeq(FieldSignature("a", CTInteger))
    val signatureOutputs = Some(IndexedSeq(FieldSignature("x", CTInteger), FieldSignature("y", CTList(CTNode))))
    val signature = ProcedureSignature(qualifiedName, signatureInputs, signatureOutputs, None, ProcedureReadOnlyAccess(Array.empty))
    val callArguments = Seq(Parameter("a", CTAny)(pos))
    val callResults = IndexedSeq(
      ProcedureResultItem(varFor("x"))(pos),
      ProcedureResultItem(ProcedureOutput("y")(pos), varFor("x"))(pos)
    )
    val unresolved = UnresolvedCall(ns, name, Some(callArguments), Some(ProcedureResult(callResults)(pos)))(pos)
    val resolved = ResolvedCall(_ => signature)(unresolved)

    errorTexts(resolved.semanticCheck(SemanticState.clean)) should equal(Seq(
      "Variable `x` already declared (line 1, column 0 (offset: 0))"
    ))
  }

  test("should verify that output field names are correct during semantic checking of resolved calls") {
    val signatureInputs = IndexedSeq(FieldSignature("a", CTInteger))
    val signatureOutputs = Some(IndexedSeq(FieldSignature("x", CTInteger), FieldSignature("y", CTList(CTNode))))
    val signature = ProcedureSignature(qualifiedName, signatureInputs, signatureOutputs, None, ProcedureReadOnlyAccess(Array.empty))
    val callArguments = Seq(Parameter("a", CTAny)(pos))
    val callResults = IndexedSeq(
      ProcedureResultItem(varFor("x"))(pos),
      ProcedureResultItem(ProcedureOutput("p")(pos), varFor("y"))(pos)
    )
    val unresolved = UnresolvedCall(ns, name, Some(callArguments), Some(ProcedureResult(callResults)(pos)))(pos)
    val resolved = ResolvedCall(_ => signature)(unresolved)

    errorTexts(resolved.semanticCheck(SemanticState.clean)) should equal(Seq(
      "Unknown procedure output: `p` (line 1, column 0 (offset: 0))"
    ))
  }

  test("should verify result types during semantic checking of resolved calls") {
    val signatureInputs = IndexedSeq(FieldSignature("a", CTInteger))
    val signatureOutputs = Some(IndexedSeq(FieldSignature("x", CTInteger), FieldSignature("y", CTList(CTNode))))
    val signature = ProcedureSignature(qualifiedName, signatureInputs, signatureOutputs, None, ProcedureReadOnlyAccess(Array.empty))
    val callArguments = Seq(StringLiteral("nope")(pos))
    val callResults = IndexedSeq(
      ProcedureResultItem(varFor("x"))(pos),
      ProcedureResultItem(ProcedureOutput("y")(pos), varFor("z"))(pos)
    )
    val unresolved = UnresolvedCall(ns, name, Some(callArguments), Some(ProcedureResult(callResults)(pos)))(pos)
    val resolved = ResolvedCall(_ => signature)(unresolved)

    errorTexts(resolved.semanticCheck(SemanticState.clean)) should equal(Seq(
      "Type mismatch: expected Integer but was String (line 1, column 0 (offset: 0))"
    ))
  }

  private def errorTexts(result: SemanticCheckResult) =
    result.errors.map { e => s"${e.msg} (${e.position})" }
}
