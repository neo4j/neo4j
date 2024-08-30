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
import org.neo4j.cypher.internal.ast.RemoveDynamicPropertyItem
import org.neo4j.cypher.internal.ast.RemovePropertyItem
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.util.symbols.CTAny

class RemoveParserTest extends AstParsingTestBase {

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

  test("REMOVE map.n.prop") {
    parsesTo[Clause](
      remove(
        Seq(
          RemovePropertyItem(prop(prop(varFor("map"), "n"), "prop"))
        )
      )
    )
  }

  test("REMOVE map[prop]") {
    parsesTo[Clause](
      remove(
        Seq(
          RemoveDynamicPropertyItem(containerIndex(varFor("map"), varFor("prop")))
        )
      )
    )
  }

  test("REMOVE (CASE WHEN true THEN r END).name") {
    parsesTo[Clause](
      remove(
        Seq(
          RemovePropertyItem(prop(caseExpression((literalBoolean(true), varFor("r"))), "name"))
        )
      )
    )
  }

  test("REMOVE (CASE WHEN true THEN r END)[toUpper(\"prop\")]") {
    parsesTo[Clause](
      remove(
        Seq(
          RemoveDynamicPropertyItem(
            containerIndex(
              caseExpression((literalBoolean(true), varFor("r"))),
              function("toUpper", literalString("prop"))
            )
          )
        )
      )
    )
  }

  test("REMOVE (listOfNodes[0])[toUpper(\"prop\")]") {
    parsesTo[Clause](
      remove(
        Seq(
          RemoveDynamicPropertyItem(
            containerIndex(
              containerIndex(varFor("listOfNodes"), 0),
              function("toUpper", literalString("prop"))
            )
          )
        )
      )
    )
  }

  test("REMOVE listOfNodes[0][toUpper(\"prop\")]") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '['")
      case _ => _.withMessage(
          """Invalid input '[': expected 'FOREACH', ',', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 22 (offset: 21))
            |"REMOVE listOfNodes[0][toUpper("prop")]"
            |                      ^""".stripMargin
        )
    }
  }

  //  Invalid use of other label expression symbols than :

  test("REMOVE n:A|B") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '|'")
      case _ => _.withMessage(
          """Invalid input '|': expected 'FOREACH', ',', ':', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 11 (offset: 10))
            |"REMOVE n:A|B"
            |           ^""".stripMargin
        )
    }
  }

  test("REMOVE n:!A") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '!'")
      case _ => _.withMessage(
          """Invalid input '!': expected an identifier or '$' (line 1, column 10 (offset: 9))
            |"REMOVE n:!A"
            |          ^""".stripMargin
        )
    }
  }

  test("REMOVE n:%") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '%'")
      case _ => _.withMessage(
          """Invalid input '%': expected an identifier or '$' (line 1, column 10 (offset: 9))
            |"REMOVE n:%"
            |          ^""".stripMargin
        )
    }
  }

  test("REMOVE n:A&B") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '&'")
      case _ => _.withMessage(
          """Invalid input '&': expected 'FOREACH', ',', ':', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 11 (offset: 10))
            |"REMOVE n:A&B"
            |           ^""".stripMargin
        )
    }
  }

  test("REMOVE n IS A&B") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '&'")
      case _ => _.withMessage(
          """Invalid input '&': expected 'FOREACH', ',', ':', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 14 (offset: 13))
            |"REMOVE n IS A&B"
            |              ^""".stripMargin
        )
    }
  }

  test("REMOVE :A") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input ':'")
      case _ => _.withMessage(
          """Invalid input ':': expected an expression (line 1, column 8 (offset: 7))
            |"REMOVE :A"
            |        ^""".stripMargin
        )
    }
  }

  test("REMOVE IS A") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'A': expected \"IS\"")
      case _ => _.withMessage(
          """Invalid input 'A': expected an expression, '.', ':', 'IS' or '[' (line 1, column 11 (offset: 10))
            |"REMOVE IS A"
            |           ^""".stripMargin
        )
    }
  }

  // Dynamic Labels

  test("REMOVE n:$(A)") {
    parsesTo[Clause](
      remove(
        Seq(removeLabelItem("n", Seq.empty, dynamicLabels = Seq(varFor("A"))))
      )
    )
  }

  test("REMOVE n IS $(A)") {
    parsesTo[Clause](
      remove(
        Seq(removeLabelItem("n", Seq.empty, dynamicLabels = Seq(varFor("A")), containsIs = true))
      )
    )
  }

  test("REMOVE n:$(A):B:$(C)") {
    parsesTo[Clause](
      remove(
        Seq(removeLabelItem("n", Seq("B"), dynamicLabels = Seq(varFor("A"), varFor("C"))))
      )
    )
  }

  test("REMOVE n:$($param), n:$(B)") {
    parsesTo[Clause](
      remove(
        Seq(
          removeLabelItem("n", Seq.empty, dynamicLabels = Seq(parameter("param", CTAny))),
          removeLabelItem("n", Seq.empty, dynamicLabels = Seq(varFor("B")))
        )
      )
    )
  }

  test("REMOVE n IS $(\"A\"), n IS B") {
    parsesTo[Clause](
      remove(
        Seq(
          removeLabelItem("n", Seq.empty, Seq(literalString("A")), containsIs = true),
          removeLabelItem("n", Seq("B"), containsIs = true)
        )
      )
    )
  }

  test("REMOVE n IS $(a || b), n:$(b || c)") {
    parsesTo[Clause](
      remove(
        Seq(
          removeLabelItem("n", Seq.empty, Seq(concatenate(varFor("a"), varFor("b"))), containsIs = true),
          removeLabelItem("n", Seq.empty, Seq(concatenate(varFor("b"), varFor("c"))))
        )
      )
    )
  }

  test("REMOVE n:$(CASE WHEN x THEN \"Label1\" ELSE \"Label2\" END), r.prop, m IS $($param)") {
    parsesTo[Clause](
      remove(
        Seq(
          removeLabelItem(
            "n",
            Seq.empty,
            Seq(caseExpression(None, Some(literalString("Label2")), (varFor("x"), literalString("Label1"))))
          ),
          removePropertyItem("r", "prop"),
          removeLabelItem("m", Seq.empty, Seq(parameter("param", CTAny)), containsIs = true)
        )
      )
    )
  }
}
