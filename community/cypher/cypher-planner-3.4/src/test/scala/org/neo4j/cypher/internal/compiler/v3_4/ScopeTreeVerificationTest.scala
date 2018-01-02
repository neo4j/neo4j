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
package org.neo4j.cypher.internal.compiler.v3_4

import org.neo4j.cypher.internal.frontend.v3_4.helpers.StringHelper._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_4.ScopeTreeVerifier
import org.neo4j.cypher.internal.frontend.v3_4.semantics.Scope

class ScopeTreeVerificationTest extends CypherFunSuite {

  import org.neo4j.cypher.internal.compiler.v3_4.helpers.ScopeTestHelper._

  test("should reject scopes mapping the wrong name to a symbol") {
    val given = Scope(Map("a" -> intSymbol("a", 3), "b" -> intSymbol("x", 5)), Seq())

    val result = ScopeTreeVerifier.verify(given).map(_.fixNewLines)

    result should equal(Seq(s"""'b' points to symbol with different name 'x@5(5): Integer' in scope ${given.toIdString}. Scope tree:
                               |${given.toIdString} {
                               |  a: 3
                               |  b: 5
                               |}
                               |""".stripMargin.fixNewLines))
  }
}
