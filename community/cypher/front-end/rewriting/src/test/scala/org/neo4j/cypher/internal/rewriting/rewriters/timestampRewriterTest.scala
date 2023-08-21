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

import org.neo4j.cypher.internal.rewriting.RewriteTest
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class timestampRewriterTest extends CypherFunSuite with RewriteTest {

  override val rewriterUnderTest: Rewriter = timestampRewriter.instance

  test("Rewrites timestamp to datetime.epochMillis") {
    assertRewrite("RETURN timestamp() as t", "RETURN datetime().epochMillis as t")
    assertRewrite("WITH timestamp() as t RETURN t", "WITH datetime().epochMillis as t RETURN t")
    assertRewrite(
      "RETURN timestamp() as t, timestamp() as d",
      "RETURN datetime().epochMillis as t, datetime().epochMillis as d"
    )
    assertRewrite("RETURN TiMeStAmP() AS t", "RETURN datetime().epochMillis AS t")
    assertIsNotRewritten("RETURN test.timestamp() AS t")
  }
}
