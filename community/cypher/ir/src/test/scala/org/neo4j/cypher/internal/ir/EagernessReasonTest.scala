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
package org.neo4j.cypher.internal.ir

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.topDown

class EagernessReasonTest extends CypherFunSuite with AstConstructionTestSupport {

  private val rewriter: Rewriter = topDown(identity)

  test("can rewrite Conflict with small ids") {
    val original = EagernessReason.Conflict(Id(1), Id(2))
    val rewritten = original.endoRewrite(rewriter)
    (original eq rewritten) shouldBe true
  }

  test("can rewrite Conflict with large ids") {
    val original = EagernessReason.Conflict(Id(1234), Id(5678))
    val rewritten = original.endoRewrite(rewriter)
    (original eq rewritten) shouldBe true
  }
}
