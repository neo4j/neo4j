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
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.Antlr
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.JavaCc
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase

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

  //  Invalid use of other label expression symbols than :

  test("REMOVE n:A|B") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Invalid input '|'"))
      .parseIn(Antlr)(_.withMessage(
        """Invalid input '|': expected 'FOREACH', ',', ':', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'MATCH', 'MERGE', 'NODETACH', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 11 (offset: 10))
          |"REMOVE n:A|B"
          |           ^""".stripMargin
      ))
  }

  test("REMOVE n:!A") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Invalid input '!'"))
      .parseIn(Antlr)(_.withMessage(
        """Invalid input '!': expected an identifier (line 1, column 10 (offset: 9))
          |"REMOVE n:!A"
          |          ^""".stripMargin
      ))
  }

  test("REMOVE n:%") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Invalid input '%'"))
      .parseIn(Antlr)(_.withMessage(
        """Invalid input '%': expected an identifier (line 1, column 10 (offset: 9))
          |"REMOVE n:%"
          |          ^""".stripMargin
      ))
  }

  test("REMOVE n:A&B") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Invalid input '&'"))
      .parseIn(Antlr)(_.withMessage(
        """Invalid input '&': expected 'FOREACH', ',', ':', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'MATCH', 'MERGE', 'NODETACH', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 11 (offset: 10))
          |"REMOVE n:A&B"
          |           ^""".stripMargin
      ))
  }

  test("REMOVE n IS A&B") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Invalid input '&'"))
      .parseIn(Antlr)(_.withMessage(
        """Invalid input '&': expected 'FOREACH', ',', ':', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'MATCH', 'MERGE', 'NODETACH', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 14 (offset: 13))
          |"REMOVE n IS A&B"
          |              ^""".stripMargin
      ))
  }

  test("REMOVE :A") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Invalid input ':'"))
      .parseIn(Antlr)(_.withMessage(
        """Invalid input ':': expected an expression (line 1, column 8 (offset: 7))
          |"REMOVE :A"
          |        ^""".stripMargin
      ))
  }

  test("REMOVE IS A") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Invalid input 'A': expected \"IS\""))
      .parseIn(Antlr)(_.withMessage(
        """Invalid input 'A': expected an expression, '.', ':' or 'IS' (line 1, column 11 (offset: 10))
          |"REMOVE IS A"
          |           ^""".stripMargin
      ))
  }
}
