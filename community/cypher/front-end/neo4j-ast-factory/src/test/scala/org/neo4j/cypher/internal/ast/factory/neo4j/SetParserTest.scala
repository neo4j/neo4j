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
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.LegacyAstParsingTestSupport

class SetParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  test("SET n:A") {
    gives[Clause](
      set_(
        Seq(setLabelItem("n", Seq("A")))
      )
    )
  }

  test("SET n IS A") {
    gives[Clause](
      set_(
        Seq(setLabelItem("n", Seq("A"), containsIs = true))
      )
    )
  }

  test("SET n:A:B:C") {
    gives[Clause](
      set_(
        Seq(setLabelItem("n", Seq("A", "B", "C")))
      )
    )
  }

  test("SET n:A, n:B") {
    gives[Clause](
      set_(
        Seq(setLabelItem("n", Seq("A")), setLabelItem("n", Seq("B")))
      )
    )
  }

  test("SET n IS A, n IS B") {
    gives[Clause](
      set_(
        Seq(setLabelItem("n", Seq("A"), containsIs = true), setLabelItem("n", Seq("B"), containsIs = true))
      )
    )
  }

  test("SET n IS A, n:B") {
    gives[Clause](
      set_(
        Seq(setLabelItem("n", Seq("A"), containsIs = true), setLabelItem("n", Seq("B")))
      )
    )
  }

  test("SET n:A, r.prop = 1, m IS B") {
    gives[Clause](
      set_(
        Seq(
          setLabelItem("n", Seq("A")),
          setPropertyItem("r", "prop", literalInt(1)),
          setLabelItem("m", Seq("B"), containsIs = true)
        )
      )
    )
  }

  test("SET n._1 = 1") {
    gives[Clause](
      set_(
        Seq(
          setPropertyItem("n", "_1", literalInt(1))
        )
      )
    )
  }

  // Invalid mix of colon conjunction and IS, this will be disallowed in semantic checking

  test("SET n IS A:B") {
    gives[Clause](
      set_(
        Seq(setLabelItem("n", Seq("A", "B"), containsIs = true))
      )
    )
  }

  test("SET n IS A, m:A:B") {
    gives[Clause](
      set_(
        Seq(
          setLabelItem("n", Seq("A"), containsIs = true),
          setLabelItem("m", Seq("A", "B"))
        )
      )
    )
  }

  //  Invalid use of other label expression symbols than :

  test("SET n:A|B") {
    failsToParse[Clause]()
  }

  test("SET n:!A") {
    failsToParse[Clause]()
  }

  test("SET n:%") {
    failsToParse[Clause]()
  }

  test("SET n:A&B") {
    failsToParse[Clause]()
  }

  test("SET n IS A&B") {
    failsToParse[Clause]()
  }

  test("SET :A") {
    failsToParse[Clause]()
  }

  test("SET IS A") {
    failsToParse[Clause]()
  }
}
