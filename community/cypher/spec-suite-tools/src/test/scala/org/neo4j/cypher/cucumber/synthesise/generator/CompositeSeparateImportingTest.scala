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

class CompositeSeparateImportingTest extends CypherFunSuite {

  private val parser = new CachingParser(CypherVersion.Cypher25)
  private def wrap(cypher: String): String = CompositeSeparateImporting.wrap(parser, cypher)

  test("Mode A: correlated leading UNWIND becomes a scope-clause CALL importing the used producer var") {
    val result = wrap("UNWIND [1, 2, 3] AS n MATCH (m) WHERE m.p = n RETURN m.q AS y")
    result should startWith("UNWIND")
    result should include("CALL (`n`) {")
    result should include("USE comp.data")
    result.trim should endWith("RETURN `y`")
    result should not include "WITH true AS" // took Mode A, not the generic fallback
  }

  test("Mode A declines an aggregating body (would change grouping) and falls back to Mode B") {
    val result = wrap("UNWIND [1, 2, 3] AS n MATCH (m) WHERE m.p = n RETURN count(*) AS c")
    result should include("WITH true AS")
    result should include("CALL (")
  }

  test("Mode A declines when the producer variable is also returned (outer collision) and falls back to Mode B") {
    val result = wrap("UNWIND [1, 2, 3] AS n RETURN n AS n")
    result should include("WITH true AS")
  }

  test("Mode A declines an updating body under a multi-row driver (rows would see earlier writes) and falls back") {
    val result = wrap("UNWIND [1, 2] AS i CREATE (n:N {id: i}) RETURN n")
    result should include("WITH true AS") // Mode B keeps the UNWIND inside one CALL
    result should not include "CALL (`i`)" // did not relocate the UNWIND as a per-row Mode A driver
  }

  test("Mode A declines a user-defined aggregation (not flagged aggregating pre-resolution) and falls back") {
    val result = wrap("UNWIND [1, 2, null, 4] AS x RETURN test.aggCount(x) AS cnt")
    result should include("WITH true AS") // Mode B aggregates over all rows inside one CALL
    result should not include "CALL (`x`)"
  }

  test("Mode A declines when the body re-declares an imported producer var (would shadow the scope-clause import)") {
    val result = wrap(
      "WITH [1, 2, 3] AS newlist WITH reverse(newlist) AS newlist WITH size(newlist) AS result RETURN result AS out"
    )
    result should include(
      "WITH true AS"
    ) // Mode B keeps newlist as an ordinary linear re-projection, not a scope import
    result should not include "CALL (`newlist`)"
  }

  test("Mode A allows a pass-through re-projection of an imported var (`x AS x` is not a shadow)") {
    val result = wrap("WITH [1, 2, 3] AS newlist UNWIND newlist AS x WITH newlist AS newlist, x AS x RETURN x AS out")
    result should include("CALL (`newlist`)")
    result should not include "WITH true AS"
  }

  test("Mode B: a MATCH-led query gets the generic single-row import wrap") {
    val result = wrap("MATCH (n) RETURN n.p AS x")
    result should include("WITH true AS `outerImport`")
    result should include("CALL (`outerImport`) {")
    result should include("USE comp.data")
    result should include("WITH * WHERE `outerImport`")
    result.trim should endWith("RETURN `x`")
  }

  test("Mode B: a non-returning write ends in the unit subquery call (no outer tail, no FINISH)") {
    val result = wrap("MATCH (n) SET n.p = 1")
    result should include("WITH true AS `outerImport`")
    result should include("CALL (`outerImport`) {")
    result.trim should endWith("}")
    result should not include "FINISH"
  }

  test("Mode B: the injected import variable avoids colliding with a source variable") {
    val result = wrap("MATCH (outerImport) RETURN outerImport.p AS x")
    result should include("WITH true AS `outerImport1`")
    result should include("CALL (`outerImport1`) {")
  }

  test("Mode B: a FINISH-terminated (non-returning) source gets no ghost outer RETURN") {
    val result = wrap("UNWIND [] AS x FINISH")
    withClue(result) {
      result should not include "RETURN"
      result.trim should endWith("}")
    }
  }

  test("wrap strips a trailing semicolon") {
    val result = wrap("MATCH (n) RETURN n AS n;")
    result should not include ";"
  }

  test("isImportWrappable accepts a plain single query with an explicit terminal") {
    assert(CompositeSeparateImporting.isImportWrappable(parser.parse("MATCH (n) RETURN n AS n")))
  }

  test("isImportWrappable accepts a non-returning single query") {
    assert(CompositeSeparateImporting.isImportWrappable(parser.parse("MATCH (n) SET n.p = 1")))
  }

  test("isImportWrappable rejects RETURN * (any leaf)") {
    assert(!CompositeSeparateImporting.isImportWrappable(parser.parse("MATCH (n) RETURN *")))
    assert(!CompositeSeparateImporting.isImportWrappable(parser.parse("RETURN 1 AS a UNION MATCH (n) RETURN *")))
  }

  test("isImportWrappable accepts UNION with explicit terminals in every branch") {
    assert(CompositeSeparateImporting.isImportWrappable(parser.parse("RETURN 1 AS a UNION RETURN 2 AS a")))
  }

  test("Mode compound: UNION imports the driver into every branch inside one CALL") {
    val result = wrap("MATCH (a) RETURN a.p AS x UNION MATCH (b) RETURN b.p AS x")
    withClue(result) {
      result should include("WITH true AS `outerImport`")
      result should include("CALL (`outerImport`) {")
      result should include("UNION")
      "WHERE `outerImport`".r.findAllMatchIn(result).size shouldBe 2
      result.trim should endWith("RETURN `x`")
    }
  }

  test("Mode compound: WHEN imports the driver into each branch (THEN/ELSE-body placement is legal)") {
    val result = wrap("WHEN true THEN MATCH (a) RETURN a.p AS x ELSE MATCH (b) RETURN b.p AS x")
    withClue(result) {
      result should include("CALL (`outerImport`) {")
      "WHERE `outerImport`".r.findAllMatchIn(result).size shouldBe 2
    }
  }

  test("Mode A preserves a `|` that is the leading character of a source line (not mistaken for stripMargin)") {
    val result = wrap(
      """WITH 1 AS x
        |RETURN {
        |  k IN [1, x]
        |  | k: k
        |} AS result""".stripMargin
    )
    result should include("CALL (`x`)")
    result should include("| k: k")
  }

  test("Mode B preserves a `|` that is the leading character of a source line (not mistaken for stripMargin)") {
    val result = wrap(
      """MATCH (n)
        |RETURN {
        |  k IN [1, 2]
        |  | k: k
        |} AS result""".stripMargin
    )
    result should include("CALL (`outerImport`) {")
    result should include("| k: k")
  }

  test("Mode compound preserves a `|` that is the leading character of a source line (not mistaken for stripMargin)") {
    val result = wrap(
      "MATCH (a) RETURN {k IN [1, 2] | k: k} AS result UNION MATCH (b) RETURN {k IN [1, 2] | k: k} AS result"
    )
    withClue(result) {
      result should include("WITH true AS")
      result.count(_ == '|') shouldBe 2
    }
  }

  test("isImportWrappable rejects NEXT (needs a dedicated wrap)") {
    assert(!CompositeSeparateImporting.isImportWrappable(
      parser.parse("MATCH (a) RETURN a AS x NEXT MATCH (b) RETURN b AS y")
    ))
  }
}
