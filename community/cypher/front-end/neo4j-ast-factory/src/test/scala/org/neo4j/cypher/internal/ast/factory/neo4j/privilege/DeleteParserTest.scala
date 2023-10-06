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
package org.neo4j.cypher.internal.ast.factory.neo4j.privilege

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.Delete
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaccRule
import org.neo4j.cypher.internal.ast.factory.neo4j.ParserSyntaxTreeBase
import org.neo4j.cypher.internal.cst.factory.neo4j.AntlrRule
import org.neo4j.cypher.internal.cst.factory.neo4j.Cst
import org.neo4j.cypher.internal.util.InputPosition

class DeleteParserTest extends ParserSyntaxTreeBase[Cst.Statement, ast.Statement] {

  implicit private val javaccRule: JavaccRule[Statement] = JavaccRule.Statement
  implicit private val antlrRule: AntlrRule[Cst.Statement] = AntlrRule.Statement

  test(
    """MATCH (n)
      |NODETACH DELETE n""".stripMargin
  ) {
    val deleteClause: Delete = Delete(
      Seq(varFor("n")),
      forced = false
    )(InputPosition(10, 2, 1))

    givesIncludingPositions {
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

    givesIncludingPositions {
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

    givesIncludingPositions {
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
    failsToParse
  }

  test(
    """MATCH (n)
      |NODETACH DETACH DELETE n""".stripMargin
  ) {
    failsToParse
  }
}
