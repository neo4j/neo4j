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
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.rewriting.rewriters.foldConstants
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.helpers.fixedPoint
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class FoldConstantsTest extends CypherFunSuite with RewriteTest {
  val exceptionFactory = OpenCypherExceptionFactory(None)
  val rewriterUnderTest: Rewriter = fixedPoint(CancellationChecker.neverCancelled())(foldConstants(exceptionFactory))

  test("solve literal expressions") {
    assertRewrite("RETURN 1+1 AS r", "RETURN 2 AS r")
    assertRewrite("RETURN 1+5*4-3 AS r", "RETURN 18 AS r")
    assertRewrite("RETURN 1+(5*4)/(3*4) AS r", "RETURN 2 AS r")
    assertRewrite("RETURN 1+(5*4)/(2.0*4) AS r", "RETURN 3.5 AS r")
  }

  test("solve multiplication regardless of order") {
    assertRewrite("MATCH (n) RETURN 5 * 3 * n.prop AS r", "MATCH (n) RETURN n.prop * 15 AS r")
    assertRewrite("MATCH (n) RETURN n.prop * 4 * 2 AS r", "MATCH (n) RETURN n.prop * 8 AS r")
    assertRewrite("MATCH (n) RETURN 12 * n.prop * 5 AS r", "MATCH (n) RETURN n.prop * 60 AS r")
    assertRewrite("MATCH (n) RETURN (12 * n.prop) * 5 AS r", "MATCH (n) RETURN n.prop * 60 AS r")
    assertRewrite("MATCH (n) RETURN 12 * (n.prop * 5) AS r", "MATCH (n) RETURN n.prop * 60 AS r")
  }

  test("solve equality comparisons between literals") {
    assertRewrite("MATCH (n) WHERE 1=1 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1=8 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1.2=1.2 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1.0=1.0 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1.0=8.0 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1=1.0 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1=8.0 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1=1.2 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1.0=1 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1.0=8 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1.2=1 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1+(5*4)/(3*4)=2 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertIsNotRewritten("MATCH (n) WHERE 1=null RETURN n AS r")
  }

  test("solve greater than comparisons between literals") {
    assertRewrite("MATCH (n) WHERE 2>1 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1>2 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1.2>2.4 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 2.0>1.0 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1.0>8.0 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 2>1.0 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1>8.0 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 2.0>1 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1.0>7 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1.2>1 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
  }

  test("solve less than comparisons between literals") {
    assertRewrite("MATCH (n) WHERE 2<1 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1<2 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1.2<2.4 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 2.0<1.0 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1.0<8.0 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 2<1.0 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1<8.0 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 2.0<1 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1.0<7 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1.2<1 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
  }

  test("doesn't fail on / by zero") {
    assertRewrite("RETURN 1/0 AS r", "RETURN 1/0 AS r")
  }

  test("does fold as far as possible on / by zero") {
    assertRewrite("RETURN (1+1)/0 AS r", "RETURN 2/0 AS r")
    assertRewrite("RETURN (1+1)/(1-1) AS r", "RETURN 2/0 AS r")
    assertRewrite("RETURN (1/0) + (1+1) AS r", "RETURN (1/0) + 2 AS r")
  }
}
