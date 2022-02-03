/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.expressions.Expression

class ComparisonParserTest extends JavaccParserAstTestBase[Expression] {

  implicit private val parser: JavaccRule[Expression] = JavaccRule.Expression

  test("a < b") {
    gives(lt(id("a"), id("b")))
  }

  test("a > b") {
    gives(gt(id("a"), id("b")))
  }

  test("a > b AND b > c") {
    gives(and(gt(id("a"), id("b")), gt(id("b"), id("c"))))
  }

  test("a > b > c") {
    gives(ands(gt(id("a"), id("b")), gt(id("b"), id("c"))))
  }

  test("a > b > c > d") {
    gives(ands(gt(id("a"), id("b")), gt(id("b"), id("c")), gt(id("c"), id("d"))))
  }

  test("a < b > c = d <= e >= f") {
    gives(ands(lt(id("a"), id("b")), gt(id("b"), id("c")), eq(id("c"), id("d")), lte(id("d"), id("e")), gte(id("e"), id("f"))))
  }
}
