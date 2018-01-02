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
package org.neo4j.cypher.internal.frontend.v2_3.parser

import org.neo4j.cypher.internal.frontend.v2_3.ast

class ComparisonTest extends ParserAstTest[ast.Expression] with Expressions {
  implicit val parser = Expression

  test("a < b") {
    yields(lt(id("a"), id("b")))
  }

  test("a > b") {
    yields(gt(id("a"), id("b")))
  }

  test("a > b AND b > c") {
    yields(and(gt(id("a"), id("b")), gt(id("b"), id("c"))))
  }

  test("a > b > c") {
    yields(ands(gt(id("a"), id("b")), gt(id("b"), id("c"))))
  }

  test("a > b > c > d") {
    yields(ands(gt(id("a"), id("b")), gt(id("b"), id("c")), gt(id("c"), id("d"))))
  }
}
