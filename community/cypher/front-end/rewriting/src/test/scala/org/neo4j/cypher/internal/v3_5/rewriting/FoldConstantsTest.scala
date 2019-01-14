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

import org.neo4j.cypher.internal.v3_5.rewriting.rewriters.foldConstants
import org.neo4j.cypher.internal.v3_5.util.Rewriter
import org.neo4j.cypher.internal.v3_5.util.helpers.fixedPoint
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class FoldConstantsTest extends CypherFunSuite with RewriteTest {

  val rewriterUnderTest: Rewriter = fixedPoint(foldConstants)

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
}
