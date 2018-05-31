/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.ast.rewriters

import org.opencypher.v9_0.util.Rewriter
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.opencypher.v9_0.ast._
import org.opencypher.v9_0.rewriting.rewriters.ReturnItemSafeTopDownRewriter
import org.opencypher.v9_0.expressions.{Equals, Variable}

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
      With(distinct = false, returnItems, PassAllGraphReturnItems(pos), None, None, None, None)(pos)
    }

    val originalReturnItem = AliasedReturnItem(Equals(varFor("foo"), literalInt(42))(pos), varFor("foo"))(pos)
    val expectedReturnItem = AliasedReturnItem(Equals(varFor("bar"), literalInt(42))(pos), varFor("foo"))(pos)

    createWith(originalReturnItem).endoRewrite(rewriter) should equal(createWith(expectedReturnItem))
  }
}
