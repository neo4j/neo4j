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

import org.neo4j.cypher.internal.v3_5.rewriting.rewriters.inliningContextCreator
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite


class InliningContextCreatorTest extends CypherFunSuite with AstRewritingTestSupport {

  val identA  = varFor("a")
  val identB  = varFor("b")
  val identR  = varFor("r")
  val identP  = varFor("p")
  val identX1 = varFor("x1")
  val identX2 = varFor("x2")

  test("should not spoil aliased node variables") {
    val ast = parser.parse("match (a) with a as b match (b) return b")

    val context = inliningContextCreator(ast)

    context.projections should equal(Map(identB -> identA))
    context.alias(identB) should equal(Some(identA))
  }

  test("should ignore named shortest paths") {
    val ast = parser.parse("match p = shortestPath((a)-[r]->(b)) return p")

    val context = inliningContextCreator(ast)

    context.projections should equal(Map.empty)
  }

  test("should not spoil aliased relationship variables") {
    val ast = parser.parse("match ()-[a]->() with a as b match ()-[b]->() return b")

    val context = inliningContextCreator(ast)

    context.projections should equal(Map(identB -> identA))
    context.alias(identB) should equal(Some(identA))
  }

  test("should spoil all the variables when WITH has aggregations") {
    val ast = parser.parse("match (a)-[r]->(b) with a as `x1`, count(r) as `x2` return x1, x2")

    val context = inliningContextCreator(ast)

    context.projections should equal(Map.empty)
  }
}
