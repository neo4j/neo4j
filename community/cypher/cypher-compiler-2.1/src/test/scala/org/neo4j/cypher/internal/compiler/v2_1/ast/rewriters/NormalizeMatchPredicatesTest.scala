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
import org.scalatest.FlatSpec

class NormalizeMatchPredicatesTest extends FlatSpec {
  import parser.ParserFixture._

  "normalizeMatchPredicates" should "move single predicate from nodes to WHERE" in {
    val original = parser.parse("MATCH (n {foo: 'bar'}) RETURN n")
    val expected = parser.parse("MATCH (n) WHERE n.foo = 'bar' RETURN n")

    val result = original.rewrite(topDown(normalizeMatchPredicates))
    assert(result === expected)
  }

  it should "move single predicates from rels to WHERE" in {
    val original = parser.parse("MATCH (n)-[r:Foo {foo: 1}]->() RETURN n")
    val expected = parser.parse("MATCH (n)-[r:Foo]->() WHERE r.foo = 1 RETURN n")

    val result = original.rewrite(bottomUp(normalizeMatchPredicates))
    assert(result.toString === expected.toString)
  }

  it should "move multiple predicates from nodes to WHERE" in {
    val original = parser.parse("MATCH (n {foo: 'bar', bar: 4}) RETURN n")
    val expected = parser.parse("MATCH (n) WHERE n.foo = 'bar' AND n.bar = 4 RETURN n")

    val result = original.rewrite(topDown(normalizeMatchPredicates))
    assert(result === expected)
  }

  it should "move multiple predicates from rels to WHERE" in {
    val original = parser.parse("MATCH (n)-[r:Foo {foo: 1, bar: 'baz'}]->() RETURN n")
    val expected = parser.parse("MATCH (n)-[r:Foo]->() WHERE r.foo = 1 AND r.bar = 'baz' RETURN n")

    val result = original.rewrite(bottomUp(normalizeMatchPredicates))
    assert(result === expected)
  }

  it should "move multiple predicates to WHERE" in {
    val original = parser.parse("MATCH (n {foo: 'bar', bar: 4})-[r:Foo {foo: 1, bar: 'baz'}]->() RETURN n")
    val expected = parser.parse("MATCH (n)-[r:Foo]->() WHERE n.foo = 'bar' AND n.bar = 4 AND r.foo = 1 AND r.bar = 'baz' RETURN n")

    val result = original.rewrite(bottomUp(normalizeMatchPredicates))
    assert(result === expected)
  }

  it should "prepend predicates to existing WHERE" in {
    val original = parser.parse("MATCH (n {foo: 'bar', bar: 4})-[r:Foo {foo: 1, bar: 'baz'}]->() WHERE n.baz = true OR r.baz = false RETURN n")
    val expected = parser.parse("MATCH (n)-[r:Foo]->() WHERE n.foo = 'bar' AND n.bar = 4 AND r.foo = 1 AND r.bar = 'baz' AND (n.baz = true OR r.baz = false) RETURN n")

    val result = original.rewrite(bottomUp(normalizeMatchPredicates))
    assert(result === expected)
  }

  it should "ignore unnamed node pattern elements" in {
    val original = parser.parse("MATCH ({foo: 'bar', bar: 4})-[r:Foo {foo: 1, bar: 'baz'}]->() RETURN n")
    val expected = parser.parse("MATCH ({foo: 'bar', bar: 4})-[r:Foo]->() WHERE r.foo = 1 AND r.bar = 'baz' RETURN n")

    val result = original.rewrite(bottomUp(normalizeMatchPredicates))
    assert(result === expected)
  }

  it should "ignore unnamed rel pattern elements" in {
    val original = parser.parse("MATCH (n {foo: 'bar', bar: 4})-[:Foo {foo: 1, bar: 'baz'}]->() RETURN n")
    val expected = parser.parse("MATCH (n)-[:Foo {foo: 1, bar: 'baz'}]->() WHERE n.foo = 'bar' AND n.bar = 4 RETURN n")

    val result = original.rewrite(bottomUp(normalizeMatchPredicates))
    assert(result === expected)
  }
}
