/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_4.prettifier

import org.neo4j.cypher.internal.frontend.v3_4.ast.Statement
import org.neo4j.cypher.internal.frontend.v3_4.parser.CypherParser
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite

class PrettifierTest extends CypherFunSuite {
  val stringifier: Prettifier = Prettifier(ExpressionStringifier())

  val parser = new CypherParser
  val tests: Seq[(String, String)] =
    Seq[(String, String)](
      "return 42" -> "RETURN 42",
      "return 42 as x" -> "RETURN 42 AS x",
      "return 42 as `43`" -> "RETURN 42 AS `43`",
      "return distinct 42" -> "RETURN DISTINCT 42",

      "return distinct a, b as X, 3+3 as six order by b.prop, b.foo descending skip 1 limit 2" ->
        """RETURN DISTINCT a, b AS X, 3 + 3 AS six
          |  ORDER BY b.prop ASCENDING, b.foo DESCENDING
          |  SKIP 1
          |  LIMIT 2""".stripMargin,

      "match (a) return a" ->
        """MATCH (a)
          |RETURN a""".stripMargin,

      "match (a) where a.prop = 42 return a" ->
        """MATCH (a)
          |  WHERE a.prop = 42
          |RETURN a""".stripMargin
    )

  tests foreach {
    case (inputString, expected) =>
      test(inputString) {
        val parsingResults: Statement = parser.parse(inputString)
        val str = stringifier.asString(parsingResults)
        str should equal(expected)
      }
  }

}
