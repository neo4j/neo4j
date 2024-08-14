/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.RemoveDynamicPropertyItem
import org.neo4j.cypher.internal.ast.SetDynamicPropertyItem
import org.neo4j.cypher.internal.rewriting.rewriters.replaceLiteralDynamicPropertyLookups
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ReplaceLiteralDynamicPropertyLookupsTest extends CypherFunSuite with AstConstructionTestSupport {

  test("Replaces literal dynamic property lookups") {
    val input: ASTNode = containerIndex(varFor("a"), literalString("name"))
    val output: ASTNode = prop("a", "name")

    replaceLiteralDynamicPropertyLookups.instance(input) should equal(output)
  }

  test("Replaces SET dynamic property lookups") {
    val input: ASTNode = SetDynamicPropertyItem(containerIndex(varFor("a"), literalString("name")), literalInt(1))(pos)
    val output: ASTNode = setPropertyItem("a", "name", literalInt(1))

    replaceLiteralDynamicPropertyLookups.instance(input) should equal(output)
  }

  test("Replaces REMOVE dynamic property lookups") {
    val input: ASTNode = RemoveDynamicPropertyItem(containerIndex(varFor("a"), literalString("name")))
    val output: ASTNode = removePropertyItem("a", "name")

    replaceLiteralDynamicPropertyLookups.instance(input) should equal(output)
  }

  test("Does not replaces non-literal dynamic property lookups") {
    val input: ASTNode = containerIndex(varFor("a"), varFor("b"))

    replaceLiteralDynamicPropertyLookups.instance(input) should equal(input)
  }

  test("Does not replaces non-literal SET dynamic property lookups") {
    val input: ASTNode = SetDynamicPropertyItem(containerIndex(varFor("a"), varFor("prop")), literalInt(1))(pos)

    replaceLiteralDynamicPropertyLookups.instance(input) should equal(input)
  }

  test("Does not replaces non-literal REMOVE dynamic property lookups") {
    val input: ASTNode = RemoveDynamicPropertyItem(containerIndex(varFor("a"), varFor("prop")))

    replaceLiteralDynamicPropertyLookups.instance(input) should equal(input)
  }
}
