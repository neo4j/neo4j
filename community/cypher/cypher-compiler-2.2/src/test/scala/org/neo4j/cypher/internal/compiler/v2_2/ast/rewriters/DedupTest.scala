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
package org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters

import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.{SemanticCheckMonitor, SemanticChecker}

class DedupTest extends CypherFunSuite {
  import org.neo4j.cypher.internal.compiler.v2_2.parser.ParserFixture._

  test("even without WITH") {
    assertRewrite(
      "match n return n as FOO",
      "match n6 return n6 as FOO")
  }

  test("is not easily fooled") {
    assertRewrite(
      "match n-->b with n as n match n-->b return n as FOO, b as BAR",
      "match n6-->b10 with n6 as n22 match n22-->b34 return n22 as FOO, b34 as BAR")
  }

  test("start and return") {
    assertRewrite(
      "start r=rel(0) return r as s",
      "start r6=rel(0) return r6 as s")
  }

  test("does not replace identifiers in order by that are shadowed by a preceding return") {
    assertRewrite(
      "match n return n as n order by n.name",
      "match n6 return n6 as n order by n.name"
    )
  }

  test("introducing aliases expressions in WITH is handled correctly") {
    assertRewrite(
      "match n with n as n, count(n) as c return n as n, c as c",
      "match n6 with n6 as n18, count(n6) as c33 return n18 as n, c33 as c"
    )
  }

  test("introducing aliases expressions in WITH is handled correctly (simple case)") {
    assertRewrite(
      "match n with n as n return n as n",
      "match n6 with n6 as n18 return n18 as n"
    )
  }

  test("sorting is deduped") {
    assertRewrite(
      "match n return n as x order by n.prop",
      "match n6 return n6 as x order by n6.prop"
    )
  }

  test("sorting is deduped 2") {
    assertRewrite(
      "match n return n as x order by x.prop",
      "match n6 return n6 as x order by x.prop"
    )
  }

  test("sorting is deduped 3") {
    assertRewrite(
      "match a, b return a as x order by b.prop",
      "match a6, b9 return a6 as x order by b9.prop"
    )
  }

  test("sorting after aggregation is deduped") {
    assertRewrite(
      "match n with n as n, count(n) as c return n.prop + c as X order by n.prop + c",
      "match n6 with n6 as n18, count(n6) as c33 return n18.prop + c33 as X order by n18.prop + c33"
    )
  }

  test("match two nodes and compare a property between them") {
    assertRewrite(
      "match (a), (b) where a:Label and b:Label and a.property = b.property return a as a, b as b",
      "match (a7), (b12) where a7:Label and b12:Label and a7.property = b12.property return a7 as a, b12 as b"
    )
  }

  test("passing node on via WITH is deduped") {
    assertRewrite(
      "MATCH a WITH a AS a MATCH a-->b RETURN a AS a",
      "MATCH a6 WITH a6 AS a18 MATCH a18-->b30 RETURN a18 AS a"
    )
  }

  test("should reject return *") {
    val (original, table) = parseAndCheck("MATCH a RETURN *")
    evaluating {
      dedup(table)(original)
    } should produce[InternalException]
  }

  test("should reject return a") {
    val (original, table) = parseAndCheck("MATCH a RETURN a")
    evaluating {
      dedup(table)(original)
    } should produce[InternalException]
  }

  test("should accept return a as b") {
    val (original, table) = parseAndCheck("MATCH a RETURN a AS b")
    dedup(table)(original)
  }

  val semantickChecker = new SemanticChecker(mock[SemanticCheckMonitor])

  def parseAndCheck(query: String) = {
    val original = parser.parse(query)
    val table = semantickChecker.check(query, original)
    (original, table)
  }

  def assertRewrite(originalQuery: String, expectedQuery: String) {
    val (original, table) = parseAndCheck(originalQuery)
    println(original)
    val expected = parser.parse(expectedQuery)
    println(expected)

    val result = dedup(table)(original).getOrElse(fail("Rewriter did not accept query"))
    assert(result === expected, s"\n$originalQuery")
  }
}
