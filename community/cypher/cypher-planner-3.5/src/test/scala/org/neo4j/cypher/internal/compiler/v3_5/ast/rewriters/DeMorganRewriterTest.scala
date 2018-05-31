/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.v3_5.ast.rewriters

import org.opencypher.v9_0.util.Rewriter
import org.opencypher.v9_0.rewriting.AstRewritingMonitor
import org.opencypher.v9_0.rewriting.rewriters.deMorganRewriter
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

class DeMorganRewriterTest extends CypherFunSuite with PredicateTestSupport {

  val rewriter: Rewriter = deMorganRewriter()(mock[AstRewritingMonitor])

  test("not (P and Q)  iff  (not P) or (not Q)") {
    not(and(P, Q)) <=> or(not(P), not(Q))
  }

  test("not (P or Q)  iff  (not P) and (not Q)") {
    not(or(P, Q)) <=> and(not(P), not(Q))
  }

  test("P xor Q  iff  (P or Q) and (not P or not Q)") {
    xor(P, Q) <=> and(or(P, Q), or(not(P), not(Q)))
  }

  test("not (P xor Q)  iff  (not P and not Q) or (P and Q)") {
    not(xor(P, Q)) <=>
      or(
        and(
          not(P),
          not(Q)),
        and(
          not(not(P)),
          not(not(Q))))
  }
}
