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
package org.neo4j.cypher.internal.ast.factory.expression

import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.test.util.LegacyAstParsingTestSupport
import org.neo4j.cypher.internal.expressions.Expression

class ComparisonParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  test("a < b") {
    parsesTo[Expression](lt(id("a"), id("b")))
  }

  test("a > b") {
    parsesTo[Expression](gt(id("a"), id("b")))
  }

  test("a > b AND b > c") {
    parsesTo[Expression](and(gt(id("a"), id("b")), gt(id("b"), id("c"))))
  }

  test("a > b > c") {
    parsesTo[Expression](ands(gt(id("a"), id("b")), gt(id("b"), id("c"))))
  }

  test("a > b > c > d") {
    parsesTo[Expression](ands(gt(id("a"), id("b")), gt(id("b"), id("c")), gt(id("c"), id("d"))))
  }

  test("a < b > c = d <= e >= f") {
    parsesTo[Expression](ands(
      lt(id("a"), id("b")),
      gt(id("b"), id("c")),
      eq(id("c"), id("d")),
      lte(id("d"), id("e")),
      gte(id("e"), id("f"))
    ))
  }
}
