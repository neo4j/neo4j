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
package org.neo4j.cypher.internal.frontend.phases.rewriting

import org.neo4j.cypher.internal.ast.semantics.SemanticCheckContext
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.simplifyPredicates
import org.neo4j.cypher.internal.rewriting.AstRewritingTestSupport
import org.neo4j.cypher.internal.rewriting.rewriters.computeDependenciesForExpressions
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeExistsPatternExpressions
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class normalizeExistsPatternExpressionsTest extends CypherFunSuite with AstRewritingTestSupport {

  testRewrite(
    "MATCH (n) WHERE (n)--(m) RETURN n",
    "MATCH (n) WHERE EXISTS { (n)--(m) } RETURN n"
  )

  testRewrite(
    "MATCH (n) RETURN NOT (n)--(m) AS foo",
    "MATCH (n) RETURN NOT EXISTS { (n)--(m) } AS foo"
  )

  testRewrite(
    "MATCH (n) WHERE (n)--(m) AND (n)--(p) RETURN n",
    "MATCH (n) WHERE EXISTS { (n)--(m) } AND EXISTS { (n)--(p) } RETURN n"
  )

  testRewrite(
    "MATCH (n) WHERE NOT (n)--(m) AND (n)--(p) RETURN n",
    "MATCH (n) WHERE NOT EXISTS { (n)--(m) } AND EXISTS { (n)--(p) } RETURN n"
  )

  testRewrite(
    "MATCH (n) WHERE NOT (n)--(m) RETURN n",
    "MATCH (n) WHERE NOT EXISTS { (n)--(m) } RETURN n"
  )

  testRewrite(
    "MATCH (f) WITH collect(f) AS friends RETURN [f IN friends WHERE (f)-[:WORKS_AT]->(:ComedyClub)] AS r",
    "MATCH (f) WITH collect(f) AS friends RETURN [f IN friends WHERE EXISTS { (f)-[:WORKS_AT]->(:ComedyClub) }] AS r"
  )

  testRewrite(
    "RETURN COLLECT { MATCH (x)-[]->(y) WHERE NOT (y)-[]->(x) RETURN id(x) } AS ids",
    "RETURN COLLECT { MATCH (x)-[]->(y) WHERE NOT EXISTS { (y)-[]->(x) } RETURN id(x) } AS ids"
  )

  testRewrite(
    "RETURN all(x IN [1,2,3] WHERE ()--()) AS y",
    "RETURN all(x IN [1,2,3] WHERE EXISTS { ()--() }) AS y"
  )

  testRewrite(
    "RETURN any(x IN [1,2,3] WHERE ()--()) AS y",
    "RETURN any(x IN [1,2,3] WHERE EXISTS { ()--() }) AS y"
  )

  testRewrite(
    "RETURN none(x IN [1,2,3] WHERE ()--()) AS y",
    "RETURN none(x IN [1,2,3] WHERE EXISTS { ()--() }) AS y"
  )

  testRewrite(
    "RETURN single(x IN [1,2,3] WHERE ()--()) AS y",
    "RETURN single(x IN [1,2,3] WHERE EXISTS { ()--() }) AS y"
  )

  testRewrite(
    "MATCH (n) WHERE EXISTS {MATCH ()--() WHERE ()--()--()} RETURN n",
    "MATCH (n) WHERE EXISTS { MATCH ()--() WHERE EXISTS { ()--()--() } } RETURN n"
  )

  testRewrite(
    "MATCH (n) WHERE ALL (n in[1, 2, 3] WHERE NOT ()<-[]-()) RETURN *",
    "MATCH (n) WHERE ALL (n in[1, 2, 3] WHERE NOT EXISTS { ()<-[]-() }) RETURN *"
  )

  testRewrite(
    "MATCH (n) WHERE COUNT { (n)-->() } > 0 RETURN *",
    "MATCH (n) WHERE EXISTS { (n)-->() } RETURN *"
  )

  testRewrite(
    "MATCH (n) WHERE 0 < COUNT { (n)-->() } RETURN *",
    "MATCH (n) WHERE EXISTS { (n)-->() } RETURN *"
  )

  testRewrite(
    "MATCH (n) WHERE COUNT { (n)-->() } = 0 RETURN *",
    "MATCH (n) WHERE NOT EXISTS { (n)-->() } RETURN *"
  )

  testRewrite(
    "MATCH (n) WHERE 0 = COUNT { (n)-->() } RETURN *",
    "MATCH (n) WHERE NOT EXISTS { (n)-->() } RETURN *"
  )

  testRewrite(
    "MATCH (n) WHERE COUNT { (n) } > 0 RETURN *",
    "MATCH (n) WHERE EXISTS { (n) } RETURN *"
  )

  testRewrite(
    "MATCH (n) WHERE COUNT { ((n)-[r1]->(m))+ ((x)-[r2]->(y)){,3} } > 0 RETURN *",
    "MATCH (n) WHERE EXISTS { ((n)-[r1]->(m))+ ((x)-[r2]->(y)){,3} } RETURN *"
  )

  testRewrite(
    "MATCH (n) WHERE COUNT { (n)-->() WHERE n.prop > 123 } > 0 RETURN *",
    "MATCH (n) WHERE EXISTS { (n)-->() WHERE n.prop > 123 } RETURN *"
  )

  testNoRewrite("RETURN (n)--(m)")

  testNoRewrite("RETURN COLLECT { MATCH (n)--(m) RETURN n.prop }")

  testNoRewrite("MATCH (n) WHERE 2 IN COLLECT { MATCH (n)--(m) RETURN n.prop } RETURN n")

  testNoRewrite("MATCH (n) WHERE size(COLLECT { MATCH p = (n)--(m) RETURN p }) > 1 RETURN n")

  private def testNoRewrite(query: String): Unit = {
    test(query + " is not rewritten") {
      assertRewrite(query, query)
    }
  }

  private def testRewrite(originalQuery: String, expectedQuery: String): Unit = {
    test(originalQuery + " is rewritten to " + expectedQuery) {
      assertRewrite(originalQuery, expectedQuery)
    }
  }

  private def assertRewrite(originalQuery: String, expectedQuery: String): Unit = {
    val original = parse(originalQuery, OpenCypherExceptionFactory(None))
    val expected = parse(expectedQuery, OpenCypherExceptionFactory(None))

    val checkResult = original.semanticCheck.run(SemanticState.clean, SemanticCheckContext.default)
    val rewriter =
      inSequence(
        computeDependenciesForExpressions(checkResult.state),
        normalizeExistsPatternExpressions(checkResult.state),
        simplifyPredicates(checkResult.state, CancellationChecker.neverCancelled())
      )

    val result = original.rewrite(rewriter)
    assert(result === expected)
  }
}
