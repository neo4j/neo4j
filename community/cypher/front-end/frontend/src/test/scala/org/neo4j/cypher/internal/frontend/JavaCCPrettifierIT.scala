/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.frontend

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaCCParser
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory.SyntaxException
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.WindowsStringSafe

class JavaCCPrettifierIT extends CypherFunSuite {
  private implicit val windowsSafe: WindowsStringSafe.type = WindowsStringSafe

  val prettifier: Prettifier = Prettifier(ExpressionStringifier())
  val parboiledPrettifier = new ParboiledPrettifierIT()

  val javaCcOnlyTests: Seq[(String, String)] = Seq[(String, String)](
    "CALL nsp.proc() yield *" ->
      """CALL nsp.proc()
        |  YIELD *""".stripMargin,
    "MATCH (n WHERE n:N)" -> "MATCH (n WHERE n:N)",
    "MATCH (n:N WHERE n.prop > 0)" -> "MATCH (n:N WHERE n.prop > 0)",
    "MATCH (n:N {foo: 5} WHERE n.prop > 0)" -> "MATCH (n:N {foo: 5} WHERE n.prop > 0)",
  )

  (parboiledPrettifier.tests ++ javaCcOnlyTests) foreach {
    case (inputString, expected) =>
      test(inputString) {
        try {
          val parsingResults: Statement = JavaCCParser.parse(inputString, OpenCypherExceptionFactory(None), new AnonymousVariableNameGenerator())
          val str = prettifier.asString(parsingResults)
          str should equal(expected)
        } catch {
          case _: SyntaxException if JavaCCParser.shouldFallback(inputString) =>
          // Should not succeed in new parser so this is correct
        }
      }
  }

  test("Ensure tests don't include fallback triggers") {
    // Sanity check
    (parboiledPrettifier.queryTests() ++ javaCcOnlyTests) foreach {
      case (inputString, _) if JavaCCParser.shouldFallback(inputString) =>
        fail(s"should not use fallback strings in tests: $inputString")
      case _ =>
    }
  }
}
