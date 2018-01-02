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
package org.neo4j.cypher.internal.frontend.v2_3.ast

import org.neo4j.cypher.internal.frontend.v2_3.SemanticState
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class ReturnItemsTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should forbid aliased projections collisions, e.g., projecting more than one value to the same id") {
    val item1 = AliasedReturnItem(StringLiteral("a")_, ident("n"))_
    val item2 = AliasedReturnItem(StringLiteral("b")_, ident("n"))_

    val items = ReturnItems(includeExisting = false, Seq(item1, item2))_

    val result = items.semanticCheck(SemanticState.clean)

    result.errors should have size 1
    result.errors.head.msg should startWith("Multiple result columns with the same name are not supported")
  }

  test("should forbid unaliased projections collisions, e.g., projecting more than one value to the same id") {
    val item1 = UnaliasedReturnItem(StringLiteral("a")_, "a")_
    val item2 = UnaliasedReturnItem(StringLiteral("a")_, "a")_

    val items = ReturnItems(includeExisting = false, Seq(item1, item2))_

    val result = items.semanticCheck(SemanticState.clean)

    result.errors should have size 1
    result.errors.head.msg should startWith("Multiple result columns with the same name are not supported")
  }

  test("should not forbid aliased projections of the same expression with different names") {
    val item1 = AliasedReturnItem(StringLiteral("a")_, ident("n"))_
    val item2 = AliasedReturnItem(StringLiteral("a")_, ident("m"))_

    val items = ReturnItems(includeExisting = false, Seq(item1, item2))_

    val result = items.semanticCheck(SemanticState.clean)

    result.errors shouldBe empty
  }
}
