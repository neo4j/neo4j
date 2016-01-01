/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.Rewriter

class CNFNormalizerTest extends CypherFunSuite with PredicateTestSupport {
  def rewriter: Rewriter = CNFNormalizer

  test("should flatten multiple ANDs in a ANDS") {
    and(P, and(R, S)) <=> ands(P, R, S)
    and(and(R, S), P) <=> ands(R, S, P)
  }

  test("should be able to convert a dnf to cnf") {
    or(and(P, Q), and(R, S)) <=> ands(ors(P, R), ors(Q, R), ors(P, S), ors(Q, S))
  }

  test("should be able to convert a dnf with nested ands to cnf") {
    or(and(P, and(Q, V)), and(R, S)) <=> ands(ors(P, R), ors(Q, R), ors(V, R), ors(P, S), ors(Q, S), ors(V, S))
  }

  test("should be able to convert a complex formula to cnf") {
    or(xor(P, Q), xor(R, S)) <=> ands(
      ors(P, Q, R, S),
      ors(not(P), not(Q), R, S),
      ors(P, Q, not(R), not(S)),
      ors(not(P), not(Q), not(R), not(S))
    )
  }
}
