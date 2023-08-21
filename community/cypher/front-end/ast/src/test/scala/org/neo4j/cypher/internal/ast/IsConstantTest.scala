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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class IsConstantTest extends CypherFunSuite with AstConstructionTestSupport {

  Map(
    literalInt(5) -> true,
    literal(null) -> true,
    literal("false") -> true,
    literal(false) -> true,
    parameter("param", CTString) -> true,
    autoParameter("param", CTList(CTInteger), Some(4)) -> true,
    varFor("n") -> false,
    listOf(literal(null), literal(false)) -> true,
    listOf(literal(null), varFor("n"), literal(false)) -> false,
    mapOf(("k1", literal("a")), ("k2", literal("b")), ("k3", literal("c"))) -> true,
    mapOf(("k1", literal("a")), ("k2", varFor("b")), ("k3", literal("c"))) -> false,
    prop(mapOfInt(("k1", 1), ("k2", 2)), "k2") -> true,
    prop(varFor("n"), "prop") -> false,
    max(nullLiteral) -> true,
    function("sin", literal(Math.PI)) -> true,
    function("sin", prop(varFor("n"), "prop")) -> false,
    add(literalInt(1), literalInt(1)) -> true,
    add(literalInt(1), varFor("n")) -> false,
    subtract(literalInt(1), literalInt(1)) -> true,
    subtract(literalInt(1), varFor("n")) -> false,
    multiply(literalInt(1), literalInt(1)) -> true,
    multiply(literalInt(1), varFor("n")) -> false,
    divide(literalInt(1), literalInt(1)) -> true,
    divide(literalInt(1), varFor("n")) -> false,
    pow(literalInt(1), literalInt(1)) -> true,
    pow(literalInt(1), varFor("n")) -> false,
    and(literal(true), literal(true)) -> true,
    and(literal(true), varFor("n")) -> false,
    or(literal(true), literal(true)) -> true,
    or(literal(true), varFor("n")) -> false
  ).foreach {
    case (expression, isConstant) =>
      test(s"$expression should ${if (isConstant) "" else "not"} be constant") {
        expression.isConstantForQuery shouldBe isConstant
      }
  }
}
