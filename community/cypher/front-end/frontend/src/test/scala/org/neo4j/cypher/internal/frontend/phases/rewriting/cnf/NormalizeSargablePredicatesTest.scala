/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.frontend.phases.rewriting.cnf

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class NormalizeSargablePredicatesTest extends CypherFunSuite with AstConstructionTestSupport {

  test("NOT x < y rewritten to: x >= y") {
    val input = not(lessThan(varFor("x"), varFor("y")))
    val output = greaterThanOrEqual(varFor("x"), varFor("y"))

    normalizeSargablePredicates(input) should equal(output)
  }

  test("NOT x <= y rewritten to: x > y") {
    val input = not(lessThanOrEqual(varFor("x"), varFor("y")))
    val output = greaterThan(varFor("x"), varFor("y"))

    normalizeSargablePredicates(input) should equal(output)
  }

  test("NOT x > y rewritten to: x <= y") {
    val input = not(greaterThan(varFor("x"), varFor("y")))
    val output = lessThanOrEqual(varFor("x"), varFor("y"))

    normalizeSargablePredicates(input) should equal(output)
  }

  test("NOT x >= y rewritten to: x < y") {
    val input = not(greaterThanOrEqual(varFor("x"), varFor("y")))
    val output = lessThan(varFor("x"), varFor("y"))

    normalizeSargablePredicates(input) should equal(output)
  }
}
