/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.neo4j.cypher.internal.parser.ParserFixture.parser
import org.neo4j.cypher.internal.rewriting.rewriters.parameterValueTypeReplacement
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ParameterTypeValueReplacementTest extends CypherFunSuite  {
  // This class does not work with the old parameter syntax
  // make sure to not reference a parameter more than once (or fix the test to support that)

  test("single integer parameter should be rewritten") {
    val params = Map("param" -> CTInteger)
    assertRewrite("MATCH (n) WHERE n.foo > $param RETURN n",params)
  }

  test("multiple integer parameters should be rewritten") {
    val params = Map("param1" -> CTInteger, "param2" -> CTInteger, "param3" -> CTInteger)
    assertRewrite("MATCH (n) WHERE n.foo > $param1 AND n.bar < $param2 AND n.baz = $param3 RETURN n",params)
  }

  test("single string parameter should be rewritten") {
    val params = Map("param" -> CTString)
    assertRewrite("MATCH (n) WHERE n.foo > $param RETURN n",params)
  }

  test("multiple string parameters should be rewritten") {
    val params = Map("param1" -> CTString, "param2" -> CTString, "param3" -> CTString)
    assertRewrite("MATCH (n) WHERE n.foo STARTS WITH $param1 AND n.bar ENDS WITH $param2 AND n.baz = $param3 RETURN n",params)
  }

  test("mixed parameters should be rewritten") {
    val params = Map("param1" -> CTString, "param2" -> CTBoolean, "param3" -> CTInteger)
    assertRewrite("MATCH (n) WHERE n.foo STARTS WITH $param1 AND n.bar = $param2 AND n.baz = $param3 RETURN n",params)
  }

  private def assertRewrite(originalQuery: String, parameterTypes: Map[String,CypherType]) {
    val exceptionFactory = OpenCypherExceptionFactory(None)
    val original: Statement = parser.parse(originalQuery, exceptionFactory) // Do not use the old parameter syntax here

    original.findByAllClass[Parameter].size should equal(parameterTypes.size) // make sure we use all given parameters in the query

    val rewriter = parameterValueTypeReplacement(original, parameterTypes)
    val result = original.rewrite(rewriter).asInstanceOf[Statement]

    val rewrittenParameters = result.findByAllClass[Parameter]
    parameterTypes.size should equal(rewrittenParameters.size)

    rewrittenParameters.forall( p => {
      val correctType: CypherType = parameterTypes.getOrElse(p.name, fail("something went wrong"))
      p.parameterType equals correctType
    })
  }
}
