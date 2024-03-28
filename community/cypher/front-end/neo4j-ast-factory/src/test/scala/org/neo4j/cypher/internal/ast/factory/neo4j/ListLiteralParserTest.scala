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
import org.neo4j.cypher.internal.expressions.Expression

class ListLiteralParserTest extends AstParsingTestBase {

  test("valid list with single element should parse") {
    "['value']" should parseTo[Expression](listOf(literalString("value")))
    "[42]" should parseTo[Expression](listOf(literalInt(42)))
    "[0.42]" should parseTo[Expression](listOf(literalFloat(0.42)))
    "[false]" should parseTo[Expression](listOf(falseLiteral))
  }

  test("empty list should parse") {
    "[]" should parseTo[Expression](listOf())
  }

  test("list with mixed element types should parse") {
    "['value', false, 42]" should parseTo[Expression](
      listOf(literalString("value"), falseLiteral, literalInt(42))
    )
  }

  test("list without comma separation should not parse") {
    "['value' 42]" should notParse[Expression]
  }

  test("list with invalid start comma should not parse") {
    "[, 'value']" should notParse[Expression]
  }
}
