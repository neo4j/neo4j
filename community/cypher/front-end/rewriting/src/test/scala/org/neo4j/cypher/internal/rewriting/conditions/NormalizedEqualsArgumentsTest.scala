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
package org.neo4j.cypher.internal.rewriting.conditions

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class NormalizedEqualsArgumentsTest extends CypherFunSuite with AstConstructionTestSupport {

  private val condition: Any => Seq[String] = normalizedEqualsArguments(_)(CancellationChecker.NeverCancelled)

  test("happy if the property in equals is normalized") {
    val ast = equals(prop("a", "prop"), literalInt(12))

    condition(ast) shouldBe empty
  }

  test("unhappy if the property in equals is not normalized") {
    val ast = equals(literalInt(12), prop("a", "prop"))

    condition(ast) should equal(Seq(s"Equals at ${ast.position} is not normalized: $ast"))
  }
}

trait NormalizedEqualsArgumentsIdTestBase extends CypherFunSuite with AstConstructionTestSupport {
  protected def makeId(e: Expression): FunctionInvocation

  private val condition: Any => Seq[String] = normalizedEqualsArguments(_)(CancellationChecker.NeverCancelled)

  test("happy if the Id-function in equals is normalized") {
    val ast = equals(makeId(varFor("a")), literalInt(12))

    condition(ast) shouldBe empty
  }

  test("unhappy if the Id-function in equals is not normalized") {
    val ast = equals(literalInt(12), makeId(varFor("a")))

    condition(ast) should equal(Seq(s"Equals at ${ast.position} is not normalized: $ast"))
  }

  test("happy if the Id-function and the property in equals are normalized") {
    val ast = equals(makeId(varFor("a")), prop("a", "prop"))

    condition(ast) shouldBe empty
  }

  test("unhappy if the Id-function and the property in equals are not normalized") {
    val ast = equals(prop("a", "prop"), makeId(varFor("a")))

    condition(ast) should equal(Seq(s"Equals at ${ast.position} is not normalized: $ast"))
  }
}

class NormalizedEqualsArgumentsIdTest extends NormalizedEqualsArgumentsIdTestBase {
  override protected def makeId(e: Expression): FunctionInvocation = id(e)
}

class NormalizedEqualsArgumentsElementIdTest extends NormalizedEqualsArgumentsIdTestBase {
  override protected def makeId(e: Expression): FunctionInvocation = elementId(e)
}
