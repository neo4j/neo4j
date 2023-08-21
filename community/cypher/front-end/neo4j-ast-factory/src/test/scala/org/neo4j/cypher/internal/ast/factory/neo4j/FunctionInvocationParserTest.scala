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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.cst.factory.neo4j.AntlrRule
import org.neo4j.cypher.internal.cst.factory.neo4j.Cst
import org.neo4j.cypher.internal.expressions.Expression

class FunctionInvocationParserTest extends ParserSyntaxTreeBase[Cst.FunctionInvocation, Expression] {

  implicit private val javaccRule: JavaccRule[Expression] = JavaccRule.FunctionInvocation
  implicit private val antlrRule: AntlrRule[Cst.FunctionInvocation] = AntlrRule.FunctionInvocation

  test("foo()") {
    gives(function("foo"))
  }

  test("foo('test', 1 + 2)") {
    gives(function("foo", literalString("test"), add(literalInt(1), literalInt(2))))
  }

  test("my.namespace.foo()") {
    gives(function(List("my", "namespace"), "foo"))
  }

  test("my.namespace.foo('test', 1 + 2)") {
    gives(function(List("my", "namespace"), "foo", literalString("test"), add(literalInt(1), literalInt(2))))
  }
}
