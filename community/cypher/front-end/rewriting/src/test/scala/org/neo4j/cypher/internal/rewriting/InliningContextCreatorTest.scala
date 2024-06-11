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

import org.neo4j.cypher.internal.rewriting.rewriters.inliningContextCreator
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class InliningContextCreatorTest extends CypherFunSuite with AstRewritingTestSupport {

  private val identA = varFor("a")
  private val identB = varFor("b")
  private val exceptionFactory = OpenCypherExceptionFactory(None)

  test("should not spoil aliased node variables") {
    val ast = parse("match (a) with a as b match (b) return b", exceptionFactory)

    val context = inliningContextCreator(ast)

    context.projections should equal(Map(identB -> identA))
    context.alias(identB) should equal(Some(identA))
  }

  test("should ignore named shortest paths") {
    val ast = parse("match p = shortestPath((a)-[r]->(b)) return p", exceptionFactory)

    val context = inliningContextCreator(ast)

    context.projections should equal(Map.empty)
  }

  test("should not spoil aliased relationship variables") {
    val ast = parse("match ()-[a]->() with a as b match ()-[b]->() return b", exceptionFactory)

    val context = inliningContextCreator(ast)

    context.projections should equal(Map(identB -> identA))
    context.alias(identB) should equal(Some(identA))
  }

  test("should spoil all the variables when WITH has aggregations") {
    val ast =
      parse("match (a)-[r]->(b) with a as `x1`, count(r) as `x2` return x1, x2", exceptionFactory)

    val context = inliningContextCreator(ast)

    context.projections should equal(Map.empty)
  }
}
