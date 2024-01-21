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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.LegacyAstParsingTestSupport
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Variable

class EscapedSymbolicNameParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  test("escaped variable name") {
    parsing[Variable]("`This isn\\'t a common variable`") shouldGive varFor("This isn\\'t a common variable")
    parsing[Variable]("`a``b`") shouldGive varFor("a`b")
  }

  test("escaped label name") {
    parsing[NodePattern]("(n:`Label`)") shouldGive nodePat(Some("n"), Some(labelLeaf("Label")))
    parsing[NodePattern]("(n:`Label``123`)") shouldGive nodePat(Some("n"), Some(labelLeaf("Label`123")))
    parsing[NodePattern]("(n:`````Label```)") shouldGive nodePat(Some("n"), Some(labelLeaf("``Label`")))

    assertFails[NodePattern]("(n:`L`abel`)")
  }
}
