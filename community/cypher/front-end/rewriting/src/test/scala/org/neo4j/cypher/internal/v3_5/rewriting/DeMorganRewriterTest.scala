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

import org.neo4j.cypher.internal.v3_5.rewriting.rewriters.deMorganRewriter
import org.neo4j.cypher.internal.v3_5.util.Rewriter
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

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
