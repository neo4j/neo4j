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
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.Antlr
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.JavaCc
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.exceptions.SyntaxException

class ConcatenationParserTest extends AstParsingTestBase {

  test("a || b") {
    parsesTo[Expression] {
      concatenate(
        varFor("a"),
        varFor("b")
      )
    }
  }

  test("a || b || c") {
    parsesTo[Expression] {
      concatenate(
        concatenate(
          varFor("a"),
          varFor("b")
        ),
        varFor("c")
      )
    }
  }

  test("a + b || c - d || e") {
    parsesTo[Expression] {
      concatenate(
        subtract(
          concatenate(
            add(
              varFor("a"),
              varFor("b")
            ),
            varFor("c")
          ),
          varFor("d")
        ),
        varFor("e")
      )
    }
  }

  test("a ||") {
    failsParsing[Clause]
      .parseIn(JavaCc)(_.withMessageStart("Encountered \" <IDENTIFIER> \"a\"\" at line 1, column 1"))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input 'a': expected 'USE', 'FINISH', 'RETURN', 'CREATE', 'INSERT', 'DETACH', 'NODETACH', 'DELETE', 'SET', 'REMOVE', 'OPTIONAL', 'MATCH', 'MERGE', 'WITH', 'UNWIND', 'CALL', 'LOAD', 'FOREACH' (line 1, column 1 (offset: 0))
          |"a ||"
          | ^""".stripMargin
      ))
  }

  test("|| b") {
    failsParsing[Clause]
      .parseIn(JavaCc)(_.withMessageStart("Encountered \" \"||\" \"||\"\" at line 1, column 1."))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input '||': expected 'USE', 'FINISH', 'RETURN', 'CREATE', 'INSERT', 'DETACH', 'NODETACH', 'DELETE', 'SET', 'REMOVE', 'OPTIONAL', 'MATCH', 'MERGE', 'WITH', 'UNWIND', 'CALL', 'LOAD', 'FOREACH' (line 1, column 1 (offset: 0))
          |"|| b"
          | ^""".stripMargin
      ))
  }

  test("a ||| b") {
    failsParsing[Clause]
      .parseIn(JavaCc)(_.withMessageStart("Encountered \" <IDENTIFIER> \"a\"\" at line 1, column 1."))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input 'a': expected 'USE', 'FINISH', 'RETURN', 'CREATE', 'INSERT', 'DETACH', 'NODETACH', 'DELETE', 'SET', 'REMOVE', 'OPTIONAL', 'MATCH', 'MERGE', 'WITH', 'UNWIND', 'CALL', 'LOAD', 'FOREACH' (line 1, column 1 (offset: 0))
          |"a ||| b"
          | ^""".stripMargin
      ))
  }

  test("a || || b") {
    failsParsing[Clause]
      .parseIn(JavaCc)(_.withMessageStart("Encountered \" <IDENTIFIER> \"a\"\" at line 1, column 1."))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input 'a': expected 'USE', 'FINISH', 'RETURN', 'CREATE', 'INSERT', 'DETACH', 'NODETACH', 'DELETE', 'SET', 'REMOVE', 'OPTIONAL', 'MATCH', 'MERGE', 'WITH', 'UNWIND', 'CALL', 'LOAD', 'FOREACH' (line 1, column 1 (offset: 0))
          |"a || || b"
          | ^""".stripMargin
      ))
  }
}
