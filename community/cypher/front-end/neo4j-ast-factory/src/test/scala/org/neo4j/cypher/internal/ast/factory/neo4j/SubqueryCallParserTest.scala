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

import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.Antlr
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.JavaCc
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase

class SubqueryCallParserTest extends AstParsingTestBase {

  test("CALL { RETURN 1 }") {
    parsesTo(subqueryCall(return_(literalInt(1).unaliased)))
  }

  test("CALL { CALL { RETURN 1 as a } }") {
    parsesTo(subqueryCall(subqueryCall(return_(literalInt(1).as("a")))))
  }

  test("CALL { RETURN 1 AS a UNION RETURN 2 AS a }") {
    parsesTo(subqueryCall(unionDistinct(
      singleQuery(return_(literalInt(1).as("a"))),
      singleQuery(return_(literalInt(2).as("a")))
    )))
  }

  test("CALL { }") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Invalid input '}'"))
      .parseIn(Antlr)(_.withMessage(
        """Invalid input '}': expected 'FOREACH', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'MATCH', 'MERGE', 'NODETACH', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'UNWIND', 'USE' or 'WITH' (line 1, column 8 (offset: 7))
          |"CALL { }"
          |        ^""".stripMargin
      ))
  }

  test("CALL { CREATE (n:N) }") {
    parsesTo(subqueryCall(create(nodePat(Some("n"), Some(labelLeaf("N"))))))
  }
}
