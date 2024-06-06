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
package org.neo4j.cypher.internal.ast.factory.query

import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.SetDynamicPropertyItem
import org.neo4j.cypher.internal.ast.SetPropertyItem
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase

class SetParserTest extends AstParsingTestBase {

  test("SET n:A") {
    parsesTo[Clause](
      set_(
        Seq(setLabelItem("n", Seq("A")))
      )
    )
  }

  test("SET n IS A") {
    parsesTo[Clause](
      set_(
        Seq(setLabelItem("n", Seq("A"), containsIs = true))
      )
    )
  }

  test("SET n:A:B:C") {
    parsesTo[Clause](
      set_(
        Seq(setLabelItem("n", Seq("A", "B", "C")))
      )
    )
  }

  test("SET n:A, n:B") {
    parsesTo[Clause](
      set_(
        Seq(setLabelItem("n", Seq("A")), setLabelItem("n", Seq("B")))
      )
    )
  }

  test("SET n IS A, n IS B") {
    parsesTo[Clause](
      set_(
        Seq(setLabelItem("n", Seq("A"), containsIs = true), setLabelItem("n", Seq("B"), containsIs = true))
      )
    )
  }

  test("SET n IS A, n:B") {
    parsesTo[Clause](
      set_(
        Seq(setLabelItem("n", Seq("A"), containsIs = true), setLabelItem("n", Seq("B")))
      )
    )
  }

  test("SET n:A, r.prop = 1, m IS B") {
    parsesTo[Clause](
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
    parsesTo[Clause](
      set_(
        Seq(
          setPropertyItem("n", "_1", literalInt(1))
        )
      )
    )
  }

  test("SET map.n.prop = 1") {
    parsesTo[Clause](
      set_(
        Seq(
          SetPropertyItem(prop(prop(varFor("map"), "n"), "prop"), literalInt(1))(pos)
        )
      )
    )
  }

  test("SET map[\"prop\"] = 1") {
    parsesTo[Clause](
      set_(
        Seq(
          SetDynamicPropertyItem(containerIndex(varFor("map"), literalString("prop")), literalInt(1))(pos)
        )
      )
    )
  }

  test("SET (CASE WHEN true THEN r END).name = 'neo4j'") {
    parsesTo[Clause](
      set_(
        Seq(
          SetPropertyItem(prop(caseExpression((literalBoolean(true), varFor("r"))), "name"), literalString("neo4j"))(
            pos
          )
        )
      )
    )
  }

  test("SET (CASE WHEN true THEN r END)[toUpper(\"prop\")] = 'neo4j'") {
    parsesTo[Clause](
      set_(
        Seq(
          SetDynamicPropertyItem(
            containerIndex(
              caseExpression((literalBoolean(true), varFor("r"))),
              function("toUpper", literalString("prop"))
            ),
            literalString("neo4j")
          )(
            pos
          )
        )
      )
    )
  }

  test("SET (listOfNodes[0])[toUpper(\"prop\")] = 'neo4j'") {
    parsesTo[Clause](
      set_(
        Seq(
          SetDynamicPropertyItem(
            containerIndex(
              containerIndex(varFor("listOfNodes"), 0),
              function("toUpper", literalString("prop"))
            ),
            literalString("neo4j")
          )(
            pos
          )
        )
      )
    )
  }

  test("SET listOfNodes[0][toUpper(\"prop\")] = 'neo4j'") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '['")
      case _ => _.withSyntaxError(
          """Invalid input '[': expected '=' (line 1, column 19 (offset: 18))
            |"SET listOfNodes[0][toUpper("prop")] = 'neo4j'"
            |                   ^""".stripMargin
        )
    }
  }

  // Invalid mix of colon conjunction and IS, this will be disallowed in semantic checking

  test("SET n IS A:B") {
    parsesTo[Clause](
      set_(
        Seq(setLabelItem("n", Seq("A", "B"), containsIs = true))
      )
    )
  }

  test("SET n IS A, m:A:B") {
    parsesTo[Clause](
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
    failsParsing[Statements]
  }

  test("SET n:!A") {
    failsParsing[Statements]
  }

  test("SET n:%") {
    failsParsing[Statements]
  }

  test("SET n:A&B") {
    failsParsing[Statements]
  }

  test("SET n IS A&B") {
    failsParsing[Statements]
  }

  test("SET :A") {
    failsParsing[Statements]
  }

  test("SET IS A") {
    failsParsing[Statements]
  }
}
