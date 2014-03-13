/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.commons.CypherFunSuite

class MatchPredicateNormalizerTest extends CypherFunSuite {
  import parser.ParserFixture._

  object PropertyPredicateNormalization extends MatchPredicateNormalization(PropertyPredicateNormalizer)
  object LabelPredicateNormalization extends MatchPredicateNormalization(LabelPredicateNormalizer)

  test("move single predicate from node to WHERE") {
    val original = parser.parse("MATCH (n {foo: 'bar'}) RETURN n")
    val expected = parser.parse("MATCH (n) WHERE n.foo = 'bar' RETURN n")

    val result = original.rewrite(topDown(PropertyPredicateNormalization))
    result should equal(expected)
  }

  test("move single predicates from rel to WHERE") {
    val original = parser.parse("MATCH (n)-[r:Foo {foo: 1}]->() RETURN n")
    val expected = parser.parse("MATCH (n)-[r:Foo]->() WHERE r.foo = 1 RETURN n")

    val result = original.rewrite(bottomUp(PropertyPredicateNormalization))
    assert(result.toString === expected.toString)
  }

  test("move multiple predicates from nodes to WHERE") {
    val original = parser.parse("MATCH (n {foo: 'bar', bar: 4}) RETURN n")
    val expected = parser.parse("MATCH (n) WHERE n.foo = 'bar' AND n.bar = 4 RETURN n")

    val result = original.rewrite(topDown(PropertyPredicateNormalization))
    result should equal(expected)
  }

  test("move multiple predicates from rels to WHERE") {
    val original = parser.parse("MATCH (n)-[r:Foo {foo: 1, bar: 'baz'}]->() RETURN n")
    val expected = parser.parse("MATCH (n)-[r:Foo]->() WHERE r.foo = 1 AND r.bar = 'baz' RETURN n")

    val result = original.rewrite(bottomUp(PropertyPredicateNormalization))
    result should equal(expected)
  }

  test("move multiple predicates to WHERE") {
    val original = parser.parse("MATCH (n {foo: 'bar', bar: 4})-[r:Foo {foo: 1, bar: 'baz'}]->() RETURN n")
    val expected = parser.parse("MATCH (n)-[r:Foo]->() WHERE n.foo = 'bar' AND n.bar = 4 AND r.foo = 1 AND r.bar = 'baz' RETURN n")

    val result = original.rewrite(bottomUp(PropertyPredicateNormalization))
    result should equal(expected)
  }

  test("prepend predicates to existing WHERE") {
    val original = parser.parse("MATCH (n {foo: 'bar', bar: 4})-[r:Foo {foo: 1, bar: 'baz'}]->() WHERE n.baz = true OR r.baz = false RETURN n")
    val expected = parser.parse("MATCH (n)-[r:Foo]->() WHERE n.foo = 'bar' AND n.bar = 4 AND r.foo = 1 AND r.bar = 'baz' AND (n.baz = true OR r.baz = false) RETURN n")

    val result = original.rewrite(bottomUp(PropertyPredicateNormalization))
    result should equal(expected)
  }

  test("ignore unnamed node pattern elements") {
    val original = parser.parse("MATCH ({foo: 'bar', bar: 4})-[r:Foo {foo: 1, bar: 'baz'}]->() RETURN n")
    val expected = parser.parse("MATCH ({foo: 'bar', bar: 4})-[r:Foo]->() WHERE r.foo = 1 AND r.bar = 'baz' RETURN n")

    val result = original.rewrite(bottomUp(PropertyPredicateNormalization))
    result should equal(expected)
  }

  test("ignore unnamed rel pattern elements") {
    val original = parser.parse("MATCH (n {foo: 'bar', bar: 4})-[:Foo {foo: 1, bar: 'baz'}]->() RETURN n")
    val expected = parser.parse("MATCH (n)-[:Foo {foo: 1, bar: 'baz'}]->() WHERE n.foo = 'bar' AND n.bar = 4 RETURN n")

    val result = original.rewrite(bottomUp(PropertyPredicateNormalization))
    result should equal(expected)
  }

  test("move single label from nodes to WHERE") {
    val original = parser.parse("MATCH (n:LABEL) RETURN n")
    val expected = parser.parse("MATCH (n) WHERE n:LABEL RETURN n")
    val result = original.rewrite(topDown(LabelPredicateNormalization))

    result should equal(expected)
  }

  test("move multiple labels from nodes to WHERE") {
    val original = parser.parse("MATCH (n:L1:L2) RETURN n")
    val expected = parser.parse("MATCH (n) WHERE n:L1:L2 RETURN n")
    val result = original.rewrite(topDown(LabelPredicateNormalization))

    result should equal(expected)
  }

  test("move single label from start node to WHERE when pattern contains relationship") {
    val original = parser.parse("MATCH (n:Foo)-[r]->(b) RETURN n")
    val expected = parser.parse("MATCH (n)-[r]->(b) WHERE n:Foo RETURN n")
    val result = original.rewrite(topDown(LabelPredicateNormalization))

    result should equal(expected)
  }

  test("move single label from end node to WHERE when pattern contains relationship") {
    val original = parser.parse("MATCH (n)-[r]->(b:Foo) RETURN n")
    val expected = parser.parse("MATCH (n)-[r]->(b) WHERE b:Foo RETURN n")
    val result = original.rewrite(topDown(LabelPredicateNormalization))

    result should equal(expected)
  }

  test("move properties and labels from nodes and relationships to WHERE clause") {
    val original = parser.parse("MATCH (a:A {foo:'v1', bar:'v2'})-[r:R {baz: 'v1'}]->(b:B {foo:'v2', baz:'v2'}) RETURN *")
    val expected = parser.parse("MATCH (a)-[r:R]->(b) WHERE (a:A AND b:B) AND (a.foo = 'v1' AND a.bar = 'v2' AND r.baz = 'v1' AND b.foo = 'v2' AND b.baz = 'v2') RETURN *")

    val step1 = Rewritable.RewritableAny(original).rewrite(topDown(PropertyPredicateNormalization))
    val step2 = Rewritable.RewritableAny(step1).rewrite(topDown(LabelPredicateNormalization))

    step2 should equal(expected)
  }
}
