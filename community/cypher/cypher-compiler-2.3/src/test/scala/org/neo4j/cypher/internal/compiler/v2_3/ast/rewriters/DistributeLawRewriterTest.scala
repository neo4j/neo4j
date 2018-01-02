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

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_3.AstRewritingMonitor
import org.neo4j.cypher.internal.frontend.v2_3.Rewriter
import org.neo4j.cypher.internal.frontend.v2_3.ast.Or
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class DistributeLawRewriterTest extends CypherFunSuite with PredicateTestSupport {

  val monitor = mock[AstRewritingMonitor]
  val rewriter: Rewriter = distributeLawsRewriter()(monitor)

  test("(P or (Q and R))  iff  (P or Q) and (P or R)") {
    or(P, and(Q, R)) <=> and(or(P, Q), or(P, R))
  }

  test("((Q and R) or P)  iff  (Q or P) and (R or P)") {
    or(and(Q, R), P) <=> and(or(Q, P), or(R, P))
  }

  test("((Q and R and S) or P)  iff  (Q or P) and (R or P) and (S or P)") {
    or(and(Q, and(R, S)), P) <=> and(or(Q, P), and(or(R, P), or(S, P)))
  }

  test("should not rewrite DNF predicates larger than the limit") {
    // given
    val start = or(and(P, Q), and(Q, R))
    val fullOr = combineUntilLimit(start, distributeLawsRewriter.DNF_CONVERSION_LIMIT - 2)

    // when
    val result = rewriter.apply(fullOr)

    // then result should be the same and aborted due to DNF-limit
    if (result == fullOr) verify(monitor).abortedRewritingDueToLargeDNF(fullOr)
    else fail(s"result was different: $result")
  }

  test("should rewrite DNF predicates smaller than the limit") {
    // given
    val start = or(and(P, Q), and(Q, R))
    val fullOr = combineUntilLimit(start, distributeLawsRewriter.DNF_CONVERSION_LIMIT - 3)

    // when
    val result = rewriter.apply(fullOr)

    // then result should be different, or equal but aborted due to repeatWithSizeLimit
    if (result == fullOr) verify(monitor).abortedRewriting(fullOr)
  }

  private def combineUntilLimit(start: Or, limit: Int): Or =
    if (limit > 0)
      combineUntilLimit(or(start, and(P, Q)), limit - 1)
    else
      start
}
