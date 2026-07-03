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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.verify
import org.neo4j.cypher.internal.rewriting.AstRewritingMonitor
import org.neo4j.cypher.internal.rewriting.PredicateTestSupport
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class repeatWithSizeLimitTest extends CypherFunSuite with PredicateTestSupport {

  override def rewriter: Rewriter = getRewriterAndMonitor._1

  private val inner = bottomUp(Rewriter.lift {
    case `P` => not(P)
    case `S` => and(T, T)
  })

  private def getRewriterAndMonitor: (Rewriter, AstRewritingMonitor) = {
    val monitor: AstRewritingMonitor = mock[AstRewritingMonitor]
    val rewriter: Rewriter = repeatWithSizeLimit(inner)(monitor)
    (rewriter, monitor)
  }

  test("should apply rewrites if final size < limit") {
    or(S, T) <=> or(and(T, T), T)
  }

  test("should abort rewriting if size rewritten would be > limit.") {
    // given
    val exp = P
    val (rewriter, monitor) = getRewriterAndMonitor

    // when
    val result = rewriter.apply(exp) // would grow indefinitely

    // then
    verify(monitor).abortedRewriting(any[AnyRef])
    // and the returned result should not be the partially rewritten but the original
    result should be(P)
  }
}
