/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.ast

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.SemanticState
import org.neo4j.cypher.internal.compiler.v2_2.symbols._

class WithTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should introduce identifiers into scope") {
    // GIVEN WITH "a" as n
    val returnItem = AliasedReturnItem(StringLiteral("a")_, ident("n"))_
    val listedReturnItems = ListedReturnItems(Seq(returnItem))_
    val withObj = With(distinct = false, listedReturnItems, None, None, None, None)_

    // WHEN
    val result = withObj.semanticCheck(SemanticState.clean)

    // THEN
    result.errors shouldBe empty
    result.state.symbolTypes("n") should equal(CTString.invariant)
  }

  test("should remove identifiers from scope") {
    // GIVEN MATCH n WITH "a" as X
    val returnItem = AliasedReturnItem(StringLiteral("a")_, ident("X"))_
    val listedReturnItems = ListedReturnItems(Seq(returnItem))_
    val withObj = With(distinct = false, listedReturnItems, None, None, None, None)_

    val beforeState = SemanticState.clean.declareIdentifier(ident("n"), CTNode).right.get
    // WHEN
    val result = withObj.semanticCheck(beforeState)

    // THEN the n identifier is no longer accessible
    result.errors shouldBe empty
    result.state.symbol("n") shouldBe empty
  }
}
