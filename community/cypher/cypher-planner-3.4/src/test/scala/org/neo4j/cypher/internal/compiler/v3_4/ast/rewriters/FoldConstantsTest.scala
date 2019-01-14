/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_4.ast.rewriters

import org.neo4j.cypher.internal.util.v3_4.Rewriter
import org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters.foldConstants
import org.neo4j.cypher.internal.frontend.v3_4.helpers.fixedPoint
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite

class FoldConstantsTest extends CypherFunSuite with RewriteTest {

  val rewriterUnderTest: Rewriter = fixedPoint(foldConstants)

  test("solve literal expressions") {
    assertRewrite("RETURN 1+1 AS r", "RETURN 2 AS r")
    assertRewrite("RETURN 1+5*4-3 AS r", "RETURN 18 AS r")
    assertRewrite("RETURN 1+(5*4)/(3*4) AS r", "RETURN 2 AS r")
    assertRewrite("RETURN 1+(5*4)/(2.0*4) AS r", "RETURN 3.5 AS r")
  }

  test("solve multiplication regardless of order") {
    assertRewrite("MATCH (n) RETURN 5 * 3 * n.prop AS r", "MATCH (n) RETURN n.prop * 15 AS r")
    assertRewrite("MATCH (n) RETURN n.prop * 4 * 2 AS r", "MATCH (n) RETURN n.prop * 8 AS r")
    assertRewrite("MATCH (n) RETURN 12 * n.prop * 5 AS r", "MATCH (n) RETURN n.prop * 60 AS r")
    assertRewrite("MATCH (n) RETURN (12 * n.prop) * 5 AS r", "MATCH (n) RETURN n.prop * 60 AS r")
    assertRewrite("MATCH (n) RETURN 12 * (n.prop * 5) AS r", "MATCH (n) RETURN n.prop * 60 AS r")
  }

  test("solve equality comparisons between literals") {
    assertRewrite("MATCH (n) WHERE 1=1 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1=8 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1.2=1.2 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1.0=1.0 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1.0=8.0 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1=1.0 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1=8.0 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1=1.2 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1.0=1 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1.0=8 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1.2=1 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1+(5*4)/(3*4)=2 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertIsNotRewritten("MATCH (n) WHERE 1=null RETURN n AS r")
  }

  test("solve greater than comparisons between literals") {
    assertRewrite("MATCH (n) WHERE 2>1 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1>2 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1.2>2.4 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 2.0>1.0 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1.0>8.0 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 2>1.0 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1>8.0 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 2.0>1 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1.0>7 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1.2>1 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
  }

  test("solve less than comparisons between literals") {
    assertRewrite("MATCH (n) WHERE 2<1 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1<2 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1.2<2.4 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 2.0<1.0 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1.0<8.0 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 2<1.0 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1<8.0 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 2.0<1 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1.0<7 RETURN n AS r", "MATCH (n) WHERE true RETURN n AS r")
    assertRewrite("MATCH (n) WHERE 1.2<1 RETURN n AS r", "MATCH (n) WHERE false RETURN n AS r")
  }
}
