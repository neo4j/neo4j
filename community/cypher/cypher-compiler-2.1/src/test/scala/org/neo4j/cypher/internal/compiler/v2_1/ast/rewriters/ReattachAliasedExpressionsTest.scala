/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1
import org.neo4j.cypher.internal.compiler.v2_1.bottomUp

class ReattachAliasedExpressionsTest extends CypherFunSuite {

  import v2_1.parser.ParserFixture._

  test("MATCH a RETURN a.x AS newAlias ORDER BY newAlias" ) {
    val original = parser.parse("MATCH a RETURN a.x AS newAlias ORDER BY newAlias")
    val expected = parser.parse("MATCH a RETURN a.x AS newAlias ORDER BY a.x")

    val result = original.rewrite(reattachAliasedExpressions)
    assert(result === expected)
  }

  test("MATCH a RETURN count(*) AS foo ORDER BY foo") {
    val original = parser.parse("MATCH a RETURN count(*) AS foo ORDER BY foo")
    val expected = parser.parse("MATCH a RETURN count(*) AS foo ORDER BY count(*)")

    val result = original.rewrite(reattachAliasedExpressions)
    assert(result === expected)
  }

  test("MATCH x WITH x AS x RETURN count(x) AS foo ORDER BY foo") {
    val original = parser.parse("MATCH x WITH x AS x RETURN count(x) AS foo ORDER BY foo")
    val expected = parser.parse("MATCH x WITH x AS x RETURN count(x) AS foo ORDER BY count(x)")

    val result = original.rewrite(reattachAliasedExpressions)
    assert(result === expected)
  }

  test("MATCH a WITH a.x AS newAlias ORDER BY newAlias RETURN *") {
    val original = parser.parse("MATCH a WITH a.x AS newAlias ORDER BY newAlias RETURN *")
    val expected = parser.parse("MATCH a WITH a.x AS newAlias ORDER BY a.x RETURN *")

    val result = original.rewrite(reattachAliasedExpressions)
    assert(result === expected)
  }

  test("MATCH a WITH count(*) AS foo ORDER BY foo RETURN *") {
    val original = parser.parse("MATCH a WITH count(*) AS foo ORDER BY foo RETURN *")
    val expected = parser.parse("MATCH a WITH count(*) AS foo ORDER BY count(*) RETURN *")

    val result = original.rewrite(reattachAliasedExpressions)
    assert(result === expected)
  }

  test("MATCH x WITH x AS x WITH count(x) AS foo ORDER BY foo RETURN *") {
    val original = parser.parse("MATCH x WITH x AS x WITH count(x) AS foo ORDER BY foo RETURN *")
    val expected = parser.parse("MATCH x WITH x AS x WITH count(x) AS foo ORDER BY count(x) RETURN *")

    val result = original.rewrite(reattachAliasedExpressions)
    assert(result === expected)
  }
}
