/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.cucumber.synthesise.generator

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CompositeWrapTest extends CypherFunSuite {

  private val parser = new CachingParser(CypherVersion.Cypher25)
  private val usePrefix = "USE comp.data\n"

  // The wrapper's contract: route to the constituent with `expectedUses` occurrences of `USE comp.data` (one per
  // top-level leaf for plain/UNION; one enclosing `USE ... { ... }` for WHEN/NEXT/braces), and the result must
  // remain valid, parseable Cypher (semantic/fabric validity is exercised by the composite feature-test run).
  private def assertRoutes(query: String, expectedUses: Int): Unit = {
    val result = CompositeWrap.route(parser, usePrefix, query)
    withClue(s"wrapped query:\n$result\n") {
      assert(
        result.split("USE comp.data", -1).length - 1 == expectedUses,
        s"expected $expectedUses USE clause(s)"
      )
      try parser.parse(result)
      catch { case e: Exception => fail(s"wrapped query did not parse:\n$result", e) }
    }
  }

  test("plain query with an unaliased RETURN is distributed") {
    assertRoutes("MATCH (n) RETURN n.name", 1)
  }

  test("top-level UNION distributes USE into every branch") {
    assertRoutes("RETURN 1 AS x UNION RETURN 2 AS x", 2)
  }

  test("standalone procedure call is distributed") {
    assertRoutes("CALL db.labels()", 1)
  }

  test("leading FILTER is distributed") {
    assertRoutes("FILTER true RETURN 1 AS x", 1)
  }

  test("nested CALL subquery is not routed (it inherits the graph selection)") {
    assertRoutes("MATCH (n) CALL { MATCH (n)-->(m) RETURN m } RETURN n, m", 1)
  }

  test("WHEN is wrapped in a single USE { ... } (inherits the selection)") {
    assertRoutes("WHEN true THEN RETURN 1 AS x ELSE RETURN 2 AS x", 1)
  }

  test("NEXT with aliased returns is wrapped in a single USE { ... }") {
    assertRoutes("RETURN 1 AS a NEXT RETURN 2 AS b", 1)
  }

  test("top-level braces get the selection attached, not a second wrap") {
    assertRoutes("{ RETURN 1 AS x }", 1)
  }

  test("a braces/WHEN UNION branch gets USE attached, not distributed into its bodies") {
    assertRoutes("{ WHEN true THEN RETURN 1 AS x ELSE RETURN 2 AS x } UNION MATCH (n) RETURN n.p AS x", 2)
  }
}
