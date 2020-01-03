/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.v4_0.ast

import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticState

class ReturnItemsTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should forbid aliased projections collisions, e.g., projecting more than one value to the same id") {
    val item1 = AliasedReturnItem(literalString("a"), varFor("n"))_
    val item2 = AliasedReturnItem(literalString("b"), varFor("n"))_

    val items = ReturnItems(includeExisting = false, Seq(item1, item2))_

    val result = items.semanticCheck(SemanticState.clean)

    result.errors should have size 1
    result.errors.head.msg should startWith("Multiple result columns with the same name are not supported")
  }

  test("should forbid unaliased projections collisions, e.g., projecting more than one value to the same id") {
    val item1 = UnaliasedReturnItem(literalString("a"), "a")_
    val item2 = UnaliasedReturnItem(literalString("a"), "a")_

    val items = ReturnItems(includeExisting = false, Seq(item1, item2))_

    val result = items.semanticCheck(SemanticState.clean)

    result.errors should have size 1
    result.errors.head.msg should startWith("Multiple result columns with the same name are not supported")
  }

  test("should not forbid aliased projections of the same expression with different names") {
    val item1 = AliasedReturnItem(literalString("a"), varFor("n"))_
    val item2 = AliasedReturnItem(literalString("a"), varFor("m"))_

    val items = ReturnItems(includeExisting = false, Seq(item1, item2))_

    val result = items.semanticCheck(SemanticState.clean)

    result.errors shouldBe empty
  }
}
