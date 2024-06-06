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

import org.neo4j.cypher.internal.ast.Delete
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.test.util.LegacyAstParsingTestSupport
import org.neo4j.cypher.internal.util.InputPosition

class DeleteParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  test(
    """MATCH (n)
      |NODETACH DELETE n""".stripMargin
  ) {
    val deleteClause: Delete = Delete(
      Seq(varFor("n")),
      forced = false
    )(InputPosition(10, 2, 1))

    parses[Statements].toAstPositioned {
      singleQuery(
        match_(nodePat(name = Some("n"))),
        deleteClause
      )
    }
  }

  test(
    """MATCH (n)
      |DELETE n""".stripMargin
  ) {
    val deleteClause: Delete = Delete(
      Seq(varFor("n")),
      forced = false
    )(InputPosition(10, 2, 1))

    parses[Statements].toAstPositioned {
      singleQuery(
        match_(nodePat(name = Some("n"))),
        deleteClause
      )
    }
  }

  test(
    """MATCH (n)
      |DETACH DELETE n""".stripMargin
  ) {
    val deleteClause: Delete = Delete(
      Seq(varFor("n")),
      forced = true
    )(InputPosition(10, 2, 1))

    parses[Statements].toAstPositioned {
      singleQuery(
        match_(nodePat(name = Some("n"))),
        deleteClause
      )
    }
  }

  test(
    """MATCH (n)
      |DETACH NODETACH DELETE n""".stripMargin
  ) {
    failsParsing[Statements]
  }

  test(
    """MATCH (n)
      |NODETACH DETACH DELETE n""".stripMargin
  ) {
    failsParsing[Statements]
  }
}
