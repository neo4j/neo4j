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
import org.neo4j.cypher.internal.compiler.v2_1.planner.AstRewritingTestSupport
import org.neo4j.cypher.internal.compiler.v2_1.bottomUp

class RewriteEqualityToInCollectionTest extends CypherFunSuite with AstRewritingTestSupport {

  test("should transform id(a) = ConstValue to id(a) IN [ConstValue]") {
    val original = parser.parse("MATCH (a) WHERE id(a) = 42")
    val expected = parser.parse("MATCH (a) WHERE id(a) IN [42]")

    val result = original.rewrite(rewriteEqualityToInCollection)

    result should equal(expected)
  }

  test("should not transform id(a) = NonConstValue") {
    val original = parser.parse("MATCH (a) WHERE id(a) = rand()")

    val result = original.rewrite(rewriteEqualityToInCollection)

    result should equal(original)
  }

  test("should transform a.prop = ConstValue to a.prop IN [ConstValue]") {
    val original = parser.parse("MATCH (a) WHERE a.prop = 42")
    val expected = parser.parse("MATCH (a) WHERE a.prop IN [42]")

    val result = original.rewrite(rewriteEqualityToInCollection)

    result should equal(expected)
  }

  test("should not transform a.prop = NonConstValue") {
    val original = parser.parse("MATCH (a) WHERE a.prop = rand()")

    val result = original.rewrite(rewriteEqualityToInCollection)

    result should equal(original)
  }
}
