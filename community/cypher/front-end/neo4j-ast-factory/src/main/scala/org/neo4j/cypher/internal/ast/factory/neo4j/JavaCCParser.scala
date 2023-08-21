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

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.parser.javacc.Cypher
import org.neo4j.cypher.internal.parser.javacc.CypherCharStream
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InputPosition

case object JavaCCParser {

  /**
   * @param queryText The query to be parsed.
   * @param cypherExceptionFactory A factory for producing error messages related to the specific implementation of the language.
   * @return
   */
  def parse(
    queryText: String,
    cypherExceptionFactory: CypherExceptionFactory
  ): Statement = {
    val charStream = new CypherCharStream(queryText)
    val astExceptionFactory = new Neo4jASTExceptionFactory(cypherExceptionFactory)
    val astFactory = new Neo4jASTFactory(queryText, astExceptionFactory)

    val statements = new Cypher(astFactory, astExceptionFactory, charStream).Statements()
    if (statements.size() == 1) {
      statements.get(0)
    } else {
      throw cypherExceptionFactory.syntaxException(
        s"Expected exactly one statement per query but got: ${statements.size}",
        InputPosition.NONE
      )
    }
  }
}
