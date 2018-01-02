/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_4.ast

import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticState
import org.neo4j.cypher.internal.v3_4.expressions.StringLiteral

class ReturnItemsTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should forbid aliased projections collisions, e.g., projecting more than one value to the same id") {
    val item1 = AliasedReturnItem(StringLiteral("a")_, varFor("n"))_
    val item2 = AliasedReturnItem(StringLiteral("b")_, varFor("n"))_

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
    val item1 = AliasedReturnItem(StringLiteral("a")_, varFor("n"))_
    val item2 = AliasedReturnItem(StringLiteral("a")_, varFor("m"))_

    val items = ReturnItems(includeExisting = false, Seq(item1, item2))_

    val result = items.semanticCheck(SemanticState.clean)

    result.errors shouldBe empty
  }
}
