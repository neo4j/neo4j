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
package org.neo4j.cypher.internal.v4_0.rewriting

import org.neo4j.cypher.internal.v4_0.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v4_0.util.ASTNode
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v4_0.rewriting.rewriters.replaceLiteralDynamicPropertyLookups

class ReplaceLiteralDynamicPropertyLookupsTest extends CypherFunSuite with AstConstructionTestSupport {

  test("Replaces literal dynamic property lookups") {
    val input: ASTNode = containerIndex(varFor("a"), literalString("name"))
    val output: ASTNode = prop("a", "name")

    replaceLiteralDynamicPropertyLookups(input) should equal(output)
  }

  test("Does not replaces non-literal dynamic property lookups") {
    val input: ASTNode = containerIndex(varFor("a"), varFor("b"))

    replaceLiteralDynamicPropertyLookups(input) should equal(input)
  }
}
