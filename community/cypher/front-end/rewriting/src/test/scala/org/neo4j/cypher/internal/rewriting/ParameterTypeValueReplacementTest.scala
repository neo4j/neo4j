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
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.rewriting.rewriters.parameterValueTypeReplacement
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo.BOOL
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo.INT
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ParameterTypeValueReplacementTest extends CypherFunSuite with AstRewritingTestSupport {

  test("single integer parameter should be rewritten") {
    val params = Map("param" -> INT)
    assertRewrite("MATCH (n) WHERE n.foo > $param RETURN n", params)
  }

  test("multiple integer parameters should be rewritten") {
    val params = Map("param1" -> INT, "param2" -> INT, "param3" -> INT)
    assertRewrite("MATCH (n) WHERE n.foo > $param1 AND n.bar < $param2 AND n.baz = $param3 RETURN n", params)
  }

  test("single string parameter should be rewritten") {
    val params = Map("param" -> ParameterTypeInfo.info(CTString, 11))
    assertRewrite("MATCH (n) WHERE n.foo > $param RETURN n", params)
  }

  test("multiple string parameters should be rewritten") {
    val params = Map(
      "param1" -> ParameterTypeInfo.info(CTString, 11),
      "param2" -> ParameterTypeInfo.info(CTString, 111),
      "param3" -> ParameterTypeInfo.info(CTString, 1111)
    )
    assertRewrite(
      "MATCH (n) WHERE n.foo STARTS WITH $param1 AND n.bar ENDS WITH $param2 AND n.baz = $param3 RETURN n",
      params
    )
  }

  test("mixed parameters should be rewritten") {
    val params = Map("param1" -> ParameterTypeInfo.info(CTString, 11), "param2" -> BOOL, "param3" -> INT)
    assertRewrite("MATCH (n) WHERE n.foo STARTS WITH $param1 AND n.bar = $param2 AND n.baz = $param3 RETURN n", params)
  }

  private def assertRewrite(originalQuery: String, parameterTypes: Map[String, ParameterTypeInfo]): Unit = {
    val exceptionFactory = OpenCypherExceptionFactory(None)
    val original: Statement = parse(originalQuery, exceptionFactory)

    original.folder.findAllByClass[Parameter].size should equal(
      parameterTypes.size
    ) // make sure we use all given parameters in the query

    val rewriter = parameterValueTypeReplacement(parameterTypes)
    val result = original.endoRewrite(rewriter)

    val rewrittenParameters: Seq[Parameter] = result.folder.findAllByClass[Parameter]
    val rewrittenParameterTypes =
      rewrittenParameters.map(p => p.name -> parameterTypes.getOrElse(p.name, fail("something went wrong"))).toMap
    rewrittenParameterTypes should equal(parameterTypes)
  }
}
