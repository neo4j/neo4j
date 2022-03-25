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
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaCCParser
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.ast.semantics.SemanticChecker
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.rewriting.rewriters.LabelExpressionPredicateNormalizer
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeHasLabelsAndHasType
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeMatchPredicates
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

class normalizeMatchPredicatesTest extends CypherFunSuite with TestName {

  private val prettifier = Prettifier(ExpressionStringifier(_.asCanonicalStringVal))

  def rewriter(semanticState: SemanticState): Rewriter = inSequence(
    LabelExpressionPredicateNormalizer,
    normalizeHasLabelsAndHasType(semanticState),
    normalizeMatchPredicates.getRewriter(
      semanticState,
      Map.empty,
      OpenCypherExceptionFactory(None),
      new AnonymousVariableNameGenerator
    )
  )

  def rewriterWithoutNormalizeMatchPredicates(semanticState: SemanticState): Rewriter = inSequence(
    LabelExpressionPredicateNormalizer,
    normalizeHasLabelsAndHasType(semanticState)
  )

  def parseForRewriting(queryText: String): Statement = JavaCCParser.parse(
    queryText.replace("\r\n", "\n"),
    OpenCypherExceptionFactory(None),
    new AnonymousVariableNameGenerator
  )

  private def assertRewrite(expectedQuery: String): Unit = {
    def rewrite(query: String, rewriter: SemanticState => Rewriter): Statement = {
      val ast = parseForRewriting(query)
      ast.endoRewrite(rewriter(SemanticChecker.check(ast).state))
    }

    val result = rewrite(testName, rewriter)
    // For the test sake we need to rewrite away HasLabelsOrTypes to HasLabels also on the expected
    val expectedResult = rewrite(expectedQuery, rewriterWithoutNormalizeMatchPredicates)
    assert(
      result === expectedResult,
      s"\n$testName\nshould be rewritten to:\n$expectedQuery\nbut was rewritten to:${prettifier.asString(result.asInstanceOf[Statement])}"
    )
  }

  test("MATCH (n {foo: 'bar'}) RETURN n") {
    assertRewrite("MATCH (n) WHERE n.foo = 'bar' RETURN n")
  }

  test("MATCH (n)-[r:Foo {foo: 1}]->() RETURN n") {
    assertRewrite("MATCH (n)-[r:Foo]->() WHERE r.foo = 1 RETURN n")
  }

  test("MATCH (n {foo: 'bar', bar: 4}) RETURN n") {
    assertRewrite("MATCH (n) WHERE n.foo = 'bar' AND n.bar = 4 RETURN n")
  }

  test("MATCH (n)-[r:Foo {foo: 1, bar: 'baz'}]->() RETURN n") {
    assertRewrite("MATCH (n)-[r:Foo]->() WHERE r.foo = 1 AND r.bar = 'baz' RETURN n")
  }

  test("MATCH (n {foo: 'bar', bar: 4})-[r:Foo {foo: 1, bar: 'baz'}]->() RETURN n") {
    assertRewrite("MATCH (n)-[r:Foo]->() WHERE n.foo = 'bar' AND n.bar = 4 AND r.foo = 1 AND r.bar = 'baz' RETURN n")
  }

  test("MATCH (n { }) RETURN n") {
    assertRewrite("MATCH (n) RETURN n")
  }

  test("MATCH (n)-[r:Foo { }]->() RETURN n") {
    assertRewrite("MATCH (n)-[r:Foo]->() RETURN n")
  }

  test("MATCH (n { })-[r:Foo { }]->() RETURN n") {
    assertRewrite("MATCH (n)-[r:Foo]->() RETURN n")
  }

  test("MATCH (n { })-[r:Foo { }]->() WHERE n.baz = true OR r.baz = false RETURN n") {
    assertRewrite("MATCH (n)-[r:Foo]->() WHERE n.baz = true OR r.baz = false RETURN n")
  }

  test("MATCH (n {foo: 'bar', bar: 4})-[r:Foo {foo: 1, bar: 'baz'}]->() WHERE n.baz = true OR r.baz = false RETURN n") {
    assertRewrite(
      "MATCH (n)-[r:Foo]->() WHERE n.foo = 'bar' AND n.bar = 4 AND r.foo = 1 AND r.bar = 'baz' AND (n.baz = true OR r.baz = false) RETURN n"
    )
  }

  test("MATCH ({foo: 'bar', bar: 4})-[r:Foo {foo: 1, bar: 'baz'}]->() RETURN r") {
    assertRewrite("MATCH ({foo: 'bar', bar: 4})-[r:Foo]->() WHERE r.foo = 1 AND r.bar = 'baz' RETURN r")
  }

  test("MATCH (n {foo: 'bar', bar: 4})-[:Foo {foo: 1, bar: 'baz'}]->() RETURN n") {
    assertRewrite("MATCH (n)-[:Foo {foo: 1, bar: 'baz'}]->() WHERE n.foo = 'bar' AND n.bar = 4 RETURN n")
  }

  test("MATCH (n:LABEL) RETURN n") {
    assertRewrite("MATCH (n) WHERE n:LABEL RETURN n")
  }

  test("MATCH (n:L1:L2) RETURN n") {
    assertRewrite("MATCH (n) WHERE n:L1:L2 RETURN n")
  }

  test("MATCH (n:Foo)-[r]->(b) RETURN n") {
    assertRewrite("MATCH (n)-[r]->(b) WHERE n:Foo RETURN n")
  }

  test("MATCH (n)-[r]->(b:Foo) RETURN n") {
    assertRewrite("MATCH (n)-[r]->(b) WHERE b:Foo RETURN n")
  }

  test("MATCH (a:A {foo:'v1', bar:'v2'})-[r:R {baz: 'v1'}]->(b:B {foo:'v2', baz:'v2'}) RETURN *") {
    assertRewrite(
      "MATCH (a)-[r:R]->(b) WHERE a.foo = 'v1' AND a.bar = 'v2' AND a:A AND r.baz = 'v1' AND b.foo = 'v2' AND b.baz = 'v2' AND b:B RETURN *"
    )
  }

  test("MATCH (n)-[r* {prop: 42}]->(b) RETURN n") {
    assertRewrite("MATCH (n)-[r*]->(b) WHERE ALL(`  UNNAMED0` in r where `  UNNAMED0`.prop = 42) RETURN n")
  }

  test("MATCH (n)-[r* {prop: 42, p: 'aaa'}]->(b) RETURN n") {
    assertRewrite(
      "MATCH (n)-[r*]->(b) WHERE ALL(`  UNNAMED0` in r where `  UNNAMED0`.prop = 42 AND `  UNNAMED0`.p = 'aaa') RETURN n"
    )
  }

  test("MATCH (a:Artist)-[r:WORKED_WITH* { year: 1988 }]->(b:Artist) RETURN *") {
    assertRewrite(
      "MATCH (a)-[r:WORKED_WITH*]->(b) WHERE a:Artist AND ALL(`  UNNAMED0` in r where `  UNNAMED0`.year = 1988) AND b:Artist  RETURN *"
    )
  }

  test("MATCH (a:Artist)-[r:WORKED_WITH* { year: $foo }]->(b:Artist) RETURN *") {
    assertRewrite(
      "MATCH (a)-[r:WORKED_WITH*]->(b) WHERE a:Artist AND ALL(`  UNNAMED0` in r where `  UNNAMED0`.year = $foo)  AND b:Artist RETURN *"
    )
  }

  test("MATCH (n WHERE n.prop > 123) RETURN n") {
    assertRewrite("MATCH (n) WHERE n.prop > 123 RETURN n")
  }

  test("MATCH (n WHERE n.prop > 123)-->(m WHERE m.prop < 42 AND m.otherProp = 'hello') RETURN n") {
    assertRewrite("MATCH (n)-->(m) WHERE n.prop > 123 AND (m.prop < 42 AND m.otherProp = 'hello') RETURN n")
  }

  test(
    "MATCH (n WHERE n.prop > 123)-->(m WHERE m.prop < 42 AND m.otherProp = 'hello') WHERE n.prop <> m.prop RETURN n"
  ) {
    assertRewrite(
      "MATCH (n)-->(m) WHERE n.prop > 123 AND (m.prop < 42 AND m.otherProp = 'hello') AND n.prop <> m.prop RETURN n"
    )
  }

  test("MATCH (n)-[r WHERE r.prop > 123]->() RETURN r") {
    assertRewrite("MATCH (n)-[r]->() WHERE r.prop > 123 RETURN r")
  }

  test("MATCH (n)-[r WHERE r.prop > 123]->()-[s WHERE s.otherProp = \"ok\"]-() RETURN r") {
    assertRewrite(
      "MATCH (n)-[r]->()-[s]-() WHERE r.prop > 123 AND s.otherProp = \"ok\" RETURN r"
    )
  }

  test("MATCH (n:A&B) RETURN n") {
    assertRewrite("MATCH (n) WHERE n:A AND n:B RETURN n")
  }

  test("MATCH (n:A|B) RETURN n") {
    assertRewrite("MATCH (n) WHERE n:A OR n:B RETURN n")
  }

  test("MATCH (n:!A) RETURN n") {
    assertRewrite("MATCH (n) WHERE NOT n:A RETURN n")
  }

  test("MATCH (n:A&(B|C)) RETURN n") {
    assertRewrite("MATCH (n) WHERE n:A AND (n:B OR n:C) RETURN n")
  }

  test("MATCH (n:%&(B|!%)) RETURN n") {
    assertRewrite("MATCH (n) WHERE size(labels(n)) > 0 AND (n:B OR NOT(size(labels(n)) > 0)) RETURN n")
  }

  test("MATCH ()-[r:!A&!B]->() RETURN r") {
    assertRewrite("MATCH ()-[r]->() WHERE NOT(r:A) AND NOT(r:B) RETURN r")
  }

  test("MATCH ()-[r:A|B|C]->() RETURN r") {
    assertRewrite("MATCH ()-[r:A|B|C]->() RETURN r")
  }
}
