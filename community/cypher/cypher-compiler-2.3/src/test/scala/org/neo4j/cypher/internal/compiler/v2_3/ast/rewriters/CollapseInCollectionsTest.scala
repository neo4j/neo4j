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

import org.neo4j.cypher.internal.compiler.v2_3.AstRewritingMonitor
import org.neo4j.cypher.internal.compiler.v2_3.planner.AstRewritingTestSupport
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class CollapseInCollectionsTest extends CypherFunSuite with AstRewritingTestSupport {

  test("should collapse collection containing ConstValues for id function") {
    val original = parse("MATCH (a) WHERE id(a) IN [42] OR id(a) IN [13]")
    val expected = parse("MATCH (a) WHERE id(a) IN [42, 13]")

    val result = original.rewrite(collapseInCollections)

    result should equal(expected)
  }

  test("should not collapse collections containing ConstValues and nonConstValues for id function") {
    val original = parse("MATCH (a) WHERE id(a) IN [42] OR id(a) IN [rand()]")
    val expected = parse("MATCH (a) WHERE id(a) IN [42, rand()]")

    val result = original.rewrite(collapseInCollections)

    result should equal(expected)
  }

  test("should collapse collection containing ConstValues for property") {
    val original = parse("MATCH (a) WHERE a.prop IN [42] OR a.prop IN [13]")
    val expected = parse("MATCH (a) WHERE a.prop IN [42, 13]")

    val result = original.rewrite(collapseInCollections)

    result should equal(expected)
  }

  test("should not collapse collections containing ConstValues and nonConstValues for property") {
    val original = parse("MATCH (a) WHERE a.prop IN [42] OR a.prop IN [rand()]")
    val expected = parse("MATCH (a) WHERE a.prop IN [42, rand()]")

    val result = original.rewrite(collapseInCollections)

    result should equal(expected)
  }

  private def parse(query: String) = {
    val parsed = parser.parse(query)
    parsed.endoRewrite(CNFNormalizer()(mock[AstRewritingMonitor]))
  }
}
