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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.rewriting.rewriters.foldConstants
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class extractSensitiveLiteralsTest extends CypherFunSuite with AstConstructionTestSupport with RewritePhaseTest {

  override def rewriterPhaseUnderTest: Phase[BaseContext, BaseState, BaseState] = extractSensitiveLiterals

  val queries = Seq(
    "MATCH (n) WHERE n.salary = 10000000.76 RETURN n",
    "MATCH (n) WHERE n.salary = 10000000.76 RETURN $p",
    "MATCH (n) WHERE n.isEvil = true RETURN n",
    "MATCH (n) WHERE n.creditCard = 123123 RETURN n",
    "MATCH (n) WHERE n.secret = 'There \\\'is\\\' \\\"no\\\" god!' RETURN n",
    "MATCH (n) WHERE n.secret = \"There \\\"is\\\" 'no' god!\" RETURN n",
    "RETURN ['the', 'answer', 'is',  42]",
    "RETURN ['there', 'is', ' no', 'god', $p, '!']",
    "RETURN {answer: 42}",
    "RETURN {     answer: 42 \t }",
    "RETURN {     answers: [ 42,       {key: false } ], accurate: false }",
    "MATCH (n) WHERE n.secret = NULL RETURN n",
    "MATCH (n {creditCard:123456}) RETURN n",
    "WITH 1 AS one, true AS notFalse RETURN one, notFalse",
    "CREATE ()-[r:Type {answer: 42}]->()",
    "MATCH (p)-[:IS_PARENT*1..2]->() RETURN p.name, p.age"
  )

  for (q <- queries) {
    test(q) {
      assertRewritten(q, q)
    }
  }

  test("should not fold sensitive literals") {
    assertSensitiveNotRewritten("RETURN 1+(5*4)/(2.0*4) AS r")
    assertSensitiveNotRewritten("MATCH (n) WHERE 1=1 RETURN n AS r")
    assertSensitiveNotRewritten("MATCH (n) WHERE 1+(5*4)/(3*4)=2 RETURN n AS r")
    assertSensitiveNotRewritten("MATCH (n) WHERE 2>1 RETURN n AS r")
    assertSensitiveNotRewritten("MATCH (n) WHERE 2<1 RETURN n AS r")
  }

  def assertSensitiveNotRewritten(query: String): Unit = {
    val unfolded: BaseState = prepareFrom(query, rewriterPhaseUnderTest)
    val folded = unfolded.statement().endoRewrite(foldConstants(OpenCypherExceptionFactory(None)))
    unfolded.statement() should equal(folded)
  }
}
