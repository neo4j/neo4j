/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.{AstRewritingMonitor, Rewriter}

class DistributeLawRewriterTest extends CypherFunSuite with PredicateTestSupport {

  val rewriter: Rewriter = distributeLawsRewriter()(mock[AstRewritingMonitor])

  test("(P or (Q and R))  iff  (P or Q) and (P or R)") {
    or(P, and(Q, R)) <=> and(or(P, Q), or(P, R))
  }

  test("((Q and R) or P)  iff  (Q or P) and (R or P)") {
    or(and(Q, R), P) <=> and(or(Q, P), or(R, P))
  }

  test("((Q and R and S) or P)  iff  (Q or P) and (R or P) and (S or P)") {
    or(and(Q, and(R, S)), P) <=> and(or(Q, P), and(or(R, P), or(S, P)))
  }
}
