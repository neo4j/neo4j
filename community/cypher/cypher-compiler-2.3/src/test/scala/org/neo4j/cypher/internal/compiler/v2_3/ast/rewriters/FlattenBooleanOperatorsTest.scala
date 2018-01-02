/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters

import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

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
