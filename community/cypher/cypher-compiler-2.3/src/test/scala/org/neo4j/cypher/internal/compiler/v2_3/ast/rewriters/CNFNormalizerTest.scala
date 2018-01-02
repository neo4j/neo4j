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

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_3.AstRewritingMonitor
import org.neo4j.cypher.internal.frontend.v2_3.Rewriter
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class CNFNormalizerTest extends CypherFunSuite with PredicateTestSupport {
  def rewriter: Rewriter = CNFNormalizer()(mock[AstRewritingMonitor])

  test("should not touch a simple predicate") {
    P <=> P
  }

  test("should lift plain AND") {
    and(P, Q) <=> ands(P, Q)
  }

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

  test("aborts cnf-rewriting for the worst case scenarios") {
    /* GIVEN A PATHOLOGICAL CASE FOR CNF
    When we get predicates in certain shapes, the normalized form for the predicate is so large it becomes
    intractable to plan on, and the better alternative is to simply give up on normalizing and let the planner
    produces a sub-optimal plan instead.
     */

    val p1 = anExp("p1")
    val p2 = anExp("p2")
    val p3 = anExp("p3")
    val p4 = anExp("p4")
    val p5 = anExp("p5")
    val p6 = anExp("p6")
    val p7 = anExp("p7")
    val p8 = anExp("p8")
    val p9 = anExp("p9")
    val p10 = anExp("p10")
    val p11 = anExp("p11")
    val p12 = anExp("p12")
    val p13 = anExp("p13")
    val p14 = anExp("p14")
    val p15 = anExp("p15")
    val p16 = anExp("p16")
    val p17 = anExp("p17")
    val p18 = anExp("p18")
    val p19 = anExp("p19")
    val p20 = anExp("p20")

    val bigPredicate =
      or(
        or(
          or(
            or(
              and(p1, p2),
              and(p3, p4)), or(
              and(p5, p6),
              and(p7, p8))), or(
            or(
              and(p9, p10),
              and(p11, p12)), or(
              and(p13, p14),
              and(p15, p16)))), or(
          and(p17, p18),
          and(p19, p20)))
    val monitor = mock[AstRewritingMonitor]

    // When
    bigPredicate.rewrite(CNFNormalizer()(monitor))

    // Then the rewriting was aborted
    verify(monitor, times(1)).abortedRewriting(any())
  }
}
