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
      "match n with n as n, count(n) as c return n.prop + c as X order by n.prop + c",
      "match n6 with n6 as n18, count(n6) as c33 return n18.prop + c33 as X order by n18.prop + c33"
    )
  }

  val semantickChecker = new SemanticChecker(mock[SemanticCheckMonitor])

  def assertRewrite(originalQuery: String, expectedQuery: String) {
    val original = parser.parse(originalQuery)
    val expected = parser.parse(expectedQuery)
    val table = semantickChecker.check(originalQuery, original)

    val result = dedup(table)(original).getOrElse(fail("Rewriter did not accept query"))
    assert(result === expected, s"\n$originalQuery")
  }
}
