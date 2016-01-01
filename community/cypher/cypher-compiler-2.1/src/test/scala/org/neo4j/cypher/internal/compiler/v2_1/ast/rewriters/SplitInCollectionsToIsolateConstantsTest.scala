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
import org.neo4j.cypher.internal.compiler.v2_1.bottomUp
import org.neo4j.cypher.internal.compiler.v2_1.planner.AstRewritingTestSupport

class SplitInCollectionsToIsolateConstantsTest extends CypherFunSuite with AstRewritingTestSupport  {

  test("should split ConstValue and NonConstValue value in Collection with id(a) IN [...]") {
    val original = parser.parse("MATCH (a) WHERE id(a) IN [42, rand()]")
    val expected = parser.parse("MATCH (a) WHERE id(a) IN [42] OR id(a) IN [rand()]")

    val result = original.rewrite(splitInCollectionsToIsolateConstants)

    result should equal(expected)
  }

  test("should not split collections containing only ConstValues for id function") {
    val original = parser.parse("MATCH (a) WHERE id(a) IN [42, 21]")

    val result = original.rewrite(splitInCollectionsToIsolateConstants)

    result should equal(original)
  }

  test("should not split collections containing only NonConstValues for id function") {
    val original = parser.parse("MATCH (a) WHERE id(a) IN [rand(), rand()]")

    val result = original.rewrite(splitInCollectionsToIsolateConstants)

    result should equal(original)
  }

  test("should split ConstValue and NonConstValue value in Collection with a.prop IN [...]") {
    val original = parser.parse("MATCH (a) WHERE a.prop IN [42, rand()]")
    val expected = parser.parse("MATCH (a) WHERE a.prop IN [42] OR a.prop IN [rand()]")

    val result = original.rewrite(splitInCollectionsToIsolateConstants)

    result should equal(expected)
  }

  test("should not split collections containing only ConstValues for property") {
    val original = parser.parse("MATCH (a) WHERE a.prop IN [42, 21]")

    val result = original.rewrite(splitInCollectionsToIsolateConstants)

    result should equal(original)
  }

  test("should not split collections containing only NonConstValues for property") {
    val original = parser.parse("MATCH (a) WHERE a.prop IN [rand(), rand()]")

    val result = original.rewrite(splitInCollectionsToIsolateConstants)

    result should equal(original)
  }
}
