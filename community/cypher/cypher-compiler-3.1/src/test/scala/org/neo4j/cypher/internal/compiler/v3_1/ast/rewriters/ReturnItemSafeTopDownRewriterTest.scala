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
package org.neo4j.cypher.internal.compiler.v3_1.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_1.Rewriter
import org.neo4j.cypher.internal.frontend.v3_1.ast._
import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite

class ReturnItemSafeTopDownRewriterTest extends CypherFunSuite with AstConstructionTestSupport {

  val rewriter = ReturnItemSafeTopDownRewriter(Rewriter.lift { case v@Variable("foo") => Variable("bar")(v.position) })

  test("works with where") {
    val original = Where(Equals(varFor("foo"), literalInt(42))(pos))(pos)
    val result = original.endoRewrite(rewriter)

    result should equal(Where(Equals(varFor("bar"), literalInt(42))(pos))(pos))
  }

  test("does not rewrite return item alias") {

    def createWith(item: ReturnItem) = {
      val returnItems = ReturnItems(includeExisting = false, Seq(item))(pos)
      With(distinct = false, returnItems, None, None, None, None)(pos)
    }


    val originalReturnItem = AliasedReturnItem(Equals(varFor("foo"), literalInt(42))(pos), varFor("foo"))(pos)
    val expectedReturnItem = AliasedReturnItem(Equals(varFor("bar"), literalInt(42))(pos), varFor("foo"))(pos)

    createWith(originalReturnItem).endoRewrite(rewriter) should equal(createWith(expectedReturnItem))
  }
}
