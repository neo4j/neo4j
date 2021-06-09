/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.frontend.phases.rewriting

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaCCParser
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.simplifyPredicates
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeExistsPatternExpressions
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class normalizeExistsPatternExpressionsTest extends CypherFunSuite with AstConstructionTestSupport {

  testRewrite(
    "MATCH (n) WHERE (n)--(m) RETURN n",
    "MATCH (n) WHERE EXISTS((n)--(m)) RETURN n")

  testRewrite(
    "MATCH (n) WHERE (n)--(m) AND (n)--(p) RETURN n",
    "MATCH (n) WHERE EXISTS((n)--(m)) AND EXISTS((n)--(p)) RETURN n")

  testRewrite(
    "MATCH (n) WHERE NOT (n)--(m) AND (n)--(p) RETURN n",
    "MATCH (n) WHERE NOT EXISTS((n)--(m)) AND EXISTS((n)--(p)) RETURN n")

  testRewrite(
    "MATCH (n) WHERE size((n)--(m))>0 RETURN n",
    "MATCH (n) WHERE EXISTS((n)--(m)) RETURN n")
  testRewrite(
    "MATCH (n) WHERE 0<size((n)--(m)) RETURN n",
    "MATCH (n) WHERE EXISTS((n)--(m)) RETURN n")

  testRewrite(
    "MATCH (n) WHERE NOT size((n)--(m))=0 RETURN n",
    "MATCH (n) WHERE EXISTS((n)--(m)) RETURN n")

  testRewrite(
    "MATCH (n) WHERE NOT 0=size((n)--(m)) RETURN n",
    "MATCH (n) WHERE EXISTS((n)--(m)) RETURN n")

  testRewrite(
    "MATCH (n) WHERE NOT (n)--(m) RETURN n",
    "MATCH (n) WHERE NOT EXISTS((n)--(m)) RETURN n")

  testRewrite(
    "MATCH (n) WHERE size((n)--(m))=0 RETURN n",
    "MATCH (n) WHERE NOT EXISTS((n)--(m)) RETURN n")

  testRewrite(
    "MATCH (n) WHERE 0=size((n)--(m)) RETURN n",
    "MATCH (n) WHERE NOT EXISTS((n)--(m)) RETURN n")

  testRewrite(
    "MATCH (n) WHERE NOT size((n)--(m))>0 RETURN n",
    "MATCH (n) WHERE NOT EXISTS((n)--(m)) RETURN n")

  testRewrite(
    "MATCH (n) WHERE NOT 0<size((n)--(m)) RETURN n",
    "MATCH (n) WHERE NOT EXISTS((n)--(m)) RETURN n")

  testRewrite(
    "MATCH (n) RETURN size((n)--(m)) > 0 AS b",
    "MATCH (n) RETURN exists((n)--(m)) AS b")

  testRewrite(
    "MATCH (n) WITH size((n)--(m)) > 0 AS b RETURN b",
    "MATCH (n) WITH exists((n)--(m)) AS b RETURN b")

  testRewrite(
    "MATCH (f) WITH collect(f) AS friends RETURN [f IN friends WHERE (f)-[:WORKS_AT]->(:ComedyClub)] AS r",
    "MATCH (f) WITH collect(f) AS friends RETURN [f IN friends WHERE exists((f)-[:WORKS_AT]->(:ComedyClub))] AS r"
  )

  testRewrite(
    "RETURN [(x)-[]->(y) WHERE NOT (y)-[]->(x) | id(x)] AS ids",
    "RETURN [(x)-[]->(y) WHERE NOT exists((y)-[]->(x)) | id(x)] AS ids")

  testRewrite(
    "RETURN all(x IN [1,2,3] WHERE ()--()) AS y",
    "RETURN all(x IN [1,2,3] WHERE exists(()--())) AS y")

  testRewrite(
    "RETURN any(x IN [1,2,3] WHERE ()--()) AS y",
    "RETURN any(x IN [1,2,3] WHERE exists(()--())) AS y"
  )

  testRewrite(
    "RETURN none(x IN [1,2,3] WHERE ()--()) AS y",
    "RETURN none(x IN [1,2,3] WHERE exists(()--())) AS y")

  testRewrite(
    "RETURN single(x IN [1,2,3] WHERE ()--()) AS y",
    "RETURN single(x IN [1,2,3] WHERE exists(()--())) AS y")

  testRewrite(
    "MATCH (n) WHERE EXISTS {MATCH ()--() WHERE ()--()--()} RETURN n",
    "MATCH (n) WHERE EXISTS {MATCH ()--() WHERE exists(()--()--())} RETURN n")

  testRewrite(
    "MATCH (n) WHERE ALL (n in[1, 2, 3] WHERE NOT ()<-[]-()) RETURN *",
    "MATCH (n) WHERE ALL (n in[1, 2, 3] WHERE NOT exists(()<-[]-())) RETURN *")

  testNoRewrite("RETURN (n)--(m)")

  testNoRewrite("RETURN [(n)--(m) | n.prop]")

  testNoRewrite("MATCH (n) WHERE 2 IN [(n)--(m) | n.prop] RETURN n")

  testNoRewrite("MATCH (n) WHERE size((n)--(m))>1 RETURN n")

  testNoRewrite("MATCH (n) WHERE NOT size((n)--(m))>1 RETURN n")

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
    val original = JavaCCParser.parse(originalQuery, OpenCypherExceptionFactory(None), new AnonymousVariableNameGenerator)
    val expected = JavaCCParser.parse(expectedQuery, OpenCypherExceptionFactory(None), new AnonymousVariableNameGenerator)

    val checkResult = original.semanticCheck(SemanticState.clean)
    val rewriter = inSequence(normalizeExistsPatternExpressions(checkResult.state), simplifyPredicates(checkResult.state))

    val result = original.rewrite(rewriter)
    assert(result === expected)
  }
}
