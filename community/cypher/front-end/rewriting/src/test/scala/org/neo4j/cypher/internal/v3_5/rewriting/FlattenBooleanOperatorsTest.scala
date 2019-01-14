/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.v3_5.rewriting

import org.neo4j.cypher.internal.v3_5.rewriting.rewriters.flattenBooleanOperators
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class FlattenBooleanOperatorsTest extends CypherFunSuite with PredicateTestSupport {

  val rewriter = flattenBooleanOperators

  test("Should be able to flatten a simple and") {
    and(P, Q) <=> ands(P, Q)
  }

  test("Should be able to flatten more than 2 ands on the left") {
    and(and(P, Q), R) <=> ands(P, Q, R)
  }

  test("Should be able to flatten more than 2 ands on the right") {
    and(R, and(P, Q)) <=> ands(R, P, Q)
  }

  test("Should be able to flatten more than 3 ands on the right") {
    and(R, and(S, and(P, Q))) <=> ands(R, S, P, Q)
  }

  test("Should be able to flatten more than 3 ands") {
    and(and(R, S), and(P, Q)) <=> ands(R, S, P, Q)
  }

  test("Should be able to flatten a simple or") {
    or(P, Q) <=> ors(P, Q)
  }

  test("Should be able to flatten more than 2 ors on the left") {
    or(or(P, Q), R) <=> ors(P, Q, R)
  }

  test("Should be able to flatten more than 2 ors on the right") {
    or(R, or(P, Q)) <=> ors(R, P, Q)
  }

  test("Should be able to flatten more than 3 ors on the right") {
    or(R, or(S, or(P, Q))) <=> ors(R, S, P, Q)
  }

  test("Should be able to flatten more than 3 ors") {
    or(or(R, S), or(P, Q)) <=> ors(R, S, P, Q)
  }

  test("Should be able to flatten an expression in cnf") {
    and(or(R, S), and(P, or(P, Q))) <=> ands(ors(R, S), P, ors(P, Q))
  }

  test("Should be able to flatten an expression in cnf 2") {
    and(or(R, S), and(P, or(or(P, Q), Q))) <=> ands(ors(R, S), P, ors(P, Q, Q))
  }

}
