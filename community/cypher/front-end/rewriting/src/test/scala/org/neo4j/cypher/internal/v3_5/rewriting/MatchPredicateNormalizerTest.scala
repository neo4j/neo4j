/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.v3_5.ast._
import org.neo4j.cypher.internal.v3_5.expressions.{GetDegree, GreaterThan}
import org.neo4j.cypher.internal.v3_5.parser.ParserFixture.parser
import org.neo4j.cypher.internal.v3_5.rewriting.rewriters.{LabelPredicateNormalizer, MatchPredicateNormalization, PropertyPredicateNormalizer}
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_5.util.{Rewriter, inSequence}

class MatchPredicateNormalizerTest extends CypherFunSuite with RewriteTest {

  object PropertyPredicateNormalization extends MatchPredicateNormalization(PropertyPredicateNormalizer, getDegreeRewriting = true)

  object LabelPredicateNormalization extends MatchPredicateNormalization(LabelPredicateNormalizer, getDegreeRewriting = true)

  def rewriterUnderTest: Rewriter = inSequence(
    PropertyPredicateNormalization,
    LabelPredicateNormalization
  )

  test("move single predicate from node to WHERE") {
    assertRewrite(
      "MATCH (n {foo: 'bar'}) RETURN n",
      "MATCH (n) WHERE n.foo = 'bar' RETURN n")
  }

  test("move single predicates from rel to WHERE") {
    assertRewrite(
      "MATCH (n)-[r:Foo {foo: 1}]->() RETURN n",
      "MATCH (n)-[r:Foo]->() WHERE r.foo = 1 RETURN n")
  }

  test("move multiple predicates from nodes to WHERE") {
    assertRewrite(
      "MATCH (n {foo: 'bar', bar: 4}) RETURN n",
      "MATCH (n) WHERE n.foo = 'bar' AND n.bar = 4 RETURN n")
  }

  test("move multiple predicates from rels to WHERE") {
    assertRewrite(
      "MATCH (n)-[r:Foo {foo: 1, bar: 'baz'}]->() RETURN n",
      "MATCH (n)-[r:Foo]->() WHERE r.foo = 1 AND r.bar = 'baz' RETURN n")
  }

  test("move multiple predicates to WHERE") {
    assertRewrite(
      "MATCH (n {foo: 'bar', bar: 4})-[r:Foo {foo: 1, bar: 'baz'}]->() RETURN n",
      "MATCH (n)-[r:Foo]->() WHERE n.foo = 'bar' AND n.bar = 4 AND r.foo = 1 AND r.bar = 'baz' RETURN n")
  }

  test("remove empty predicate from node") {
    assertRewrite(
      "MATCH (n { }) RETURN n",
      "MATCH (n) RETURN n")
  }

  test("remove empty predicate from rel") {
    assertRewrite(
      "MATCH (n)-[r:Foo { }]->() RETURN n",
      "MATCH (n)-[r:Foo]->() RETURN n")
  }

  test("remove empty predicates") {
    assertRewrite(
      "MATCH (n { })-[r:Foo { }]->() RETURN n",
      "MATCH (n)-[r:Foo]->() RETURN n")
  }

  test("remove empty predicates and keep existing WHERE") {
    assertRewrite(
      "MATCH (n { })-[r:Foo { }]->() WHERE n.baz = true OR r.baz = false RETURN n",
      "MATCH (n)-[r:Foo]->() WHERE n.baz = true OR r.baz = false RETURN n")
  }

  test("prepend predicates to existing WHERE") {
    assertRewrite(
      "MATCH (n {foo: 'bar', bar: 4})-[r:Foo {foo: 1, bar: 'baz'}]->() WHERE n.baz = true OR r.baz = false RETURN n",
      "MATCH (n)-[r:Foo]->() WHERE n.foo = 'bar' AND n.bar = 4 AND r.foo = 1 AND r.bar = 'baz' AND (n.baz = true OR r.baz = false) RETURN n")
  }

  test("ignore unnamed node pattern elements") {
    assertRewrite(
      "MATCH ({foo: 'bar', bar: 4})-[r:Foo {foo: 1, bar: 'baz'}]->() RETURN r",
      "MATCH ({foo: 'bar', bar: 4})-[r:Foo]->() WHERE r.foo = 1 AND r.bar = 'baz' RETURN r")
  }

  test("ignore unnamed rel pattern elements") {
    assertRewrite(
      "MATCH (n {foo: 'bar', bar: 4})-[:Foo {foo: 1, bar: 'baz'}]->() RETURN n",
      "MATCH (n)-[:Foo {foo: 1, bar: 'baz'}]->() WHERE n.foo = 'bar' AND n.bar = 4 RETURN n")
  }

  test("move single label from nodes to WHERE") {
    assertRewrite(
      "MATCH (n:LABEL) RETURN n",
      "MATCH (n) WHERE n:LABEL RETURN n")
  }

  test("move multiple labels from nodes to WHERE") {
    assertRewrite(
      "MATCH (n:L1:L2) RETURN n",
      "MATCH (n) WHERE n:L1:L2 RETURN n")
  }

  test("move single label from start node to WHERE when pattern contains relationship") {
    assertRewrite(
      "MATCH (n:Foo)-[r]->(b) RETURN n",
      "MATCH (n)-[r]->(b) WHERE n:Foo RETURN n")
  }

  test("move single label from end node to WHERE when pattern contains relationship") {
    assertRewrite(
      "MATCH (n)-[r]->(b:Foo) RETURN n",
      "MATCH (n)-[r]->(b) WHERE b:Foo RETURN n")
  }

  test("move properties and labels from nodes and relationships to WHERE clause") {
    assertRewrite(
      "MATCH (a:A {foo:'v1', bar:'v2'})-[r:R {baz: 'v1'}]->(b:B {foo:'v2', baz:'v2'}) RETURN *",
      "MATCH (a)-[r:R]->(b) WHERE (a:A AND b:B) AND (a.foo = 'v1' AND a.bar = 'v2' AND r.baz = 'v1' AND b.foo = 'v2' AND b.baz = 'v2') RETURN *")
  }

  test("move single property from var length relationship to the where clause") {
    assertRewrite(
      "MATCH (n)-[r* {prop: 42}]->(b) RETURN n",
      "MATCH (n)-[r*]->(b) WHERE ALL(`  FRESHID9` in r where `  FRESHID9`.prop = 42) RETURN n")
  }

  test("move multiple properties from var length relationship to the where clause") {
    assertRewrite(
      "MATCH (n)-[r* {prop: 42, p: 'aaa'}]->(b) RETURN n",
      "MATCH (n)-[r*]->(b) WHERE ALL(`  FRESHID9` in r where `  FRESHID9`.prop = 42 AND `  FRESHID9`.p = 'aaa') RETURN n")
  }

  test("varlength with labels") {
    assertRewrite(
      "MATCH (a:Artist)-[r:WORKED_WITH* { year: 1988 }]->(b:Artist) RETURN *",
      "MATCH (a)-[r:WORKED_WITH*]->(b) WHERE a:Artist AND b:Artist AND ALL(`  FRESHID16` in r where `  FRESHID16`.year = 1988)  RETURN *")
  }

  test("varlength with labels and parameters") {
    assertRewrite(
      "MATCH (a:Artist)-[r:WORKED_WITH* { year: $foo }]->(b:Artist) RETURN *",
      "MATCH (a)-[r:WORKED_WITH*]->(b) WHERE a:Artist AND b:Artist AND ALL(`  FRESHID16` in r where `  FRESHID16`.year = $foo)  RETURN *")
  }

  test("rewrite outgoing pattern to getDegree call") {
    rewrite(parseForRewriting("MATCH (a) WHERE (a)-[:R]->() RETURN a.prop")) should matchPattern {
      case Query(_, SingleQuery(Seq(Match(_, _, _, Some(Where(GreaterThan(_: GetDegree, _)))), _: Return))) =>
    }
  }

  test("rewrite incoming pattern to getDegree call") {
    val rewrite1 = rewrite(parseForRewriting("MATCH (a) WHERE ()-[:R]->(a) RETURN a.prop"))
    rewrite1 should matchPattern {
      case Query(_, SingleQuery(Seq(Match(_, _, _, Some(Where(GreaterThan(_: GetDegree, _)))), _: Return))) =>
    }
  }

  test("does not rewrite getDegree if turned off") {
    val rewriter1 = new MatchPredicateNormalization(PropertyPredicateNormalizer, getDegreeRewriting = false) {}
    val rewriter2 = new MatchPredicateNormalization(LabelPredicateNormalizer, getDegreeRewriting = false) {}

    val rewriterUnderTest: Rewriter = inSequence(
      rewriter1,
      rewriter2
    )

    val query = "MATCH (a) WHERE ()-[:R]->(a) RETURN a.prop"
    val original = parser.parse(query)
    val result = original.rewrite(rewriterUnderTest)
    assert(result === original, "\n" + query)
  }
}
