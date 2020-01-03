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
package org.neo4j.cypher.internal.v4_0.rewriting.conditions

import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class NormalizedEqualsArgumentsTest extends CypherFunSuite with AstConstructionTestSupport {

  private val condition: Any => Seq[String] = normalizedEqualsArguments

  test("happy if the property in equals is normalized") {
    val ast = equals(prop("a", "prop"), literalInt(12))

    condition(ast) shouldBe empty
  }

  test("unhappy if the property in equals is not normalized") {
    val ast = equals(literalInt(12), prop("a", "prop"))

    condition(ast) should equal(Seq(s"Equals at ${ast.position} is not normalized: $ast"))
  }

  test("happy if the Id-function in equals is normalized") {
    val ast = equals(id(varFor("a")), literalInt(12))

    condition(ast) shouldBe empty
  }

  test("unhappy if the Id-function in equals is not normalized") {
    val ast = equals(literalInt(12), id(varFor("a")))

    condition(ast) should equal(Seq(s"Equals at ${ast.position} is not normalized: $ast"))
  }

  test("happy if the Id-function and the property in equals are normalized") {
    val ast = equals(id(varFor("a")), prop("a", "prop"))

    condition(ast) shouldBe empty
  }

  test("unhappy if the Id-function and the property in equals are not normalized") {
    val ast = equals(prop("a", "prop"), id(varFor("a")))

    condition(ast) should equal(Seq(s"Equals at ${ast.position} is not normalized: $ast"))
  }
}
