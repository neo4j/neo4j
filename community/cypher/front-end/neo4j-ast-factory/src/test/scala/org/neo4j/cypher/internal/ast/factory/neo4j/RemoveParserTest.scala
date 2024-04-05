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

import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.LegacyAstParsingTestSupport

class RemoveParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  test("REMOVE n:A") {
    parsesTo[Clause](
      remove(
        Seq(removeLabelItem("n", Seq("A")))
      )
    )
  }

  test("REMOVE n IS A") {
    parsesTo[Clause](
      remove(
        Seq(removeLabelItem("n", Seq("A"), containsIs = true))
      )
    )
  }

  test("REMOVE n:A:B:C") {
    parsesTo[Clause](
      remove(
        Seq(removeLabelItem("n", Seq("A", "B", "C")))
      )
    )
  }

  test("REMOVE n:A, n:B") {
    parsesTo[Clause](
      remove(
        Seq(removeLabelItem("n", Seq("A")), removeLabelItem("n", Seq("B")))
      )
    )
  }

  test("REMOVE n IS A, n IS B") {
    parsesTo[Clause](
      remove(
        Seq(removeLabelItem("n", Seq("A"), containsIs = true), removeLabelItem("n", Seq("B"), containsIs = true))
      )
    )
  }

  test("REMOVE n IS A, n:B") {
    parsesTo[Clause](
      remove(
        Seq(removeLabelItem("n", Seq("A"), containsIs = true), removeLabelItem("n", Seq("B")))
      )
    )
  }

  test("REMOVE n:A, r.prop, m IS B") {
    parsesTo[Clause](
      remove(
        Seq(
          removeLabelItem("n", Seq("A")),
          removePropertyItem("r", "prop"),
          removeLabelItem("m", Seq("B"), containsIs = true)
        )
      )
    )
  }

  // Invalid mix of colon conjunction and IS, this will be disallowed in semantic checking

  test("REMOVE n IS A:B") {
    parsesTo[Clause](
      remove(
        Seq(removeLabelItem("n", Seq("A", "B"), containsIs = true))
      )
    )
  }

  test("REMOVE n IS A, m:A:B") {
    parsesTo[Clause](
      remove(
        Seq(
          removeLabelItem("n", Seq("A"), containsIs = true),
          removeLabelItem("m", Seq("A", "B"))
        )
      )
    )
  }

  //  Invalid use of other label expression symbols than :

  test("REMOVE n:A|B") {
    failsParsing[Statements]
  }

  test("REMOVE n:!A") {
    failsParsing[Clause]
  }

  test("REMOVE n:%") {
    failsParsing[Clause]
  }

  test("REMOVE n:A&B") {
    failsParsing[Statements]
  }

  test("REMOVE n IS A&B") {
    failsParsing[Statements]
  }

  test("REMOVE :A") {
    failsParsing[Clause]
  }

  test("REMOVE IS A") {
    failsParsing[Clause]
  }
}
