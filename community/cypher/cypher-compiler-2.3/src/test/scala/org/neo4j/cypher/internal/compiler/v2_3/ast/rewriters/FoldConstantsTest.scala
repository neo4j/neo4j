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

import org.neo4j.cypher.internal.frontend.v2_3.Rewriter
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class FoldConstantsTest extends CypherFunSuite with RewriteTest {

  val rewriterUnderTest: Rewriter = foldConstants

  test("solve literal expressions") {
    assertRewrite("RETURN 1+1 AS r", "RETURN 2 AS r")
    assertRewrite("RETURN 1+5*4-3 AS r", "RETURN 18 AS r")
    assertRewrite("RETURN 1+(5*4)/(3*4) AS r", "RETURN 2 AS r")
    assertRewrite("RETURN 1+(5*4)/(2.0*4) AS r", "RETURN 3.5 AS r")
  }

  test("solve multiplication regardless of order") {
    assertRewrite("MATCH n RETURN 5 * 3 * n.prop AS r", "MATCH n RETURN n.prop * 15 AS r")
    assertRewrite("MATCH n RETURN n.prop * 4 * 2 AS r", "MATCH n RETURN n.prop * 8 AS r")
    assertRewrite("MATCH n RETURN 12 * n.prop * 5 AS r", "MATCH n RETURN n.prop * 60 AS r")
    assertRewrite("MATCH n RETURN (12 * n.prop) * 5 AS r", "MATCH n RETURN n.prop * 60 AS r")
    assertRewrite("MATCH n RETURN 12 * (n.prop * 5) AS r", "MATCH n RETURN n.prop * 60 AS r")
  }
}
