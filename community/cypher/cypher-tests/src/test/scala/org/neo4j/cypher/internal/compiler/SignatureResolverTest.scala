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

import org.neo4j.cypher.CommunityCypherTestSuite
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.helpers.ProcedureLookup
import org.neo4j.cypher.internal.compiler.helpers.SignatureResolver
import org.neo4j.cypher.internal.frontend.phases.QueryLanguage
import org.neo4j.internal.kernel.api.procs.Neo4jTypes
import org.neo4j.internal.kernel.api.procs.ProcedureHandle
import org.neo4j.internal.kernel.api.procs.QualifiedName
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature

class SignatureResolverTest extends CommunityCypherTestSuite with AstConstructionTestSupport {

  private val funcName = functionName("my", "func")
  private val aggFuncName = functionName("my", "aggFunc")
  private val missingName = functionName("no", "such", "func")

  private def mkHandle(
    name: Seq[String],
    id: Int,
    queryLanguage: org.neo4j.kernel.api.QueryLanguage
  ): UserFunctionHandle = {
    val sig = new UserFunctionSignature(
      new QualifiedName(name.init.toArray, name.last),
      java.util.List.of(),
      Neo4jTypes.NTAny,
      false,
      null,
      name.last,
      "Test",
      true,
      false,
      false,
      false,
      java.util.Set.of(queryLanguage)
    )
    new UserFunctionHandle(sig, id)
  }

  private def regularHandle(s: org.neo4j.kernel.api.QueryLanguage) = mkHandle(Seq("my", "func"), 1, s)
  private def aggHandle(s: org.neo4j.kernel.api.QueryLanguage) = mkHandle(Seq("my", "aggFunc"), 2, s)

  private def mkResolver(): SignatureResolver = new SignatureResolver(new ProcedureLookup {
    override def function(n: QualifiedName, s: org.neo4j.kernel.api.QueryLanguage): UserFunctionHandle =
      if (n.toString == "my.func") regularHandle(s) else null
    override def aggregationFunction(n: QualifiedName, s: org.neo4j.kernel.api.QueryLanguage): UserFunctionHandle =
      if (n.toString == "my.aggFunc") aggHandle(s) else null
    override def procedure(n: QualifiedName, s: org.neo4j.kernel.api.QueryLanguage): ProcedureHandle =
      throw new RuntimeException("not implemented")
    override def signatureVersion: Long = 42
  })

  for (language <- Seq(QueryLanguage.Cypher5, QueryLanguage.Cypher25)) {

    test(s"should resolve regular function with isAggregate=false ($language)") {
      val resolver = mkResolver()
      val result = resolver.functionSignature(funcName, language)
      result shouldBe defined
      result.get.isAggregate shouldBe false
    }

    test(s"should resolve aggregation function via fallback with isAggregate=true ($language)") {
      val resolver = mkResolver()
      val result = resolver.functionSignature(aggFuncName, language)
      result shouldBe defined
      result.get.isAggregate shouldBe true
    }

    test(s"should return None when neither function nor aggregation function exists ($language)") {
      val resolver = mkResolver()
      val result = resolver.functionSignature(missingName, language)
      result shouldBe None
    }
  }
}
