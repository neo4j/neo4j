/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters

import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{Rewriter, inSequence}

class MatchPredicateNormalizerTest extends CypherFunSuite with RewriteTest {

  object PropertyPredicateNormalization extends MatchPredicateNormalization(PropertyPredicateNormalizer)

  object LabelPredicateNormalization extends MatchPredicateNormalization(LabelPredicateNormalizer)

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
      "MATCH (a:Artist)-[r:WORKED_WITH* { year: {foo} }]->(b:Artist) RETURN *",
      "MATCH (a)-[r:WORKED_WITH*]->(b) WHERE a:Artist AND b:Artist AND ALL(`  FRESHID16` in r where `  FRESHID16`.year = {foo})  RETURN *")
  }
}
