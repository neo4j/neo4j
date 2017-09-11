/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_4.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_4.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters.createGraphIntroducesHorizon
import org.neo4j.cypher.internal.apa.v3_4.test_helpers.CypherFunSuite

class createGraphIntroducesHorizonTest extends CypherFunSuite with RewriteTest with AstConstructionTestSupport {

  override val rewriterUnderTest = createGraphIntroducesHorizon

  test("create x >> is rewritten") {
    assertRewrite(
      "CREATE GRAPH AT 'url' AS foo >>",
      "CREATE GRAPH AT 'url' AS foo WITH * GRAPHS *, foo >>"
    )
  }

  test("create << x is rewritten") {
    assertRewrite(
      "CREATE >> GRAPH AT 'url' AS foo",
      "CREATE GRAPH AT 'url' AS foo WITH * GRAPHS *, >> foo"
    )
  }

  test("create x is not rewritten without fish op") {
    assertIsNotRewritten("CREATE GRAPH AT 'url' AS foo")
  }

}
