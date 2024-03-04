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
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.LegacyAstParsingTestSupport
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.ParserSupport.NotAnyAntlr
import org.scalatest.LoneElement

class StatementReturnColumnsParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport
    with LoneElement {

  private def columns(cols: String*)(statement: Statement) =
    statement.returnColumns.map(_.name) shouldBe cols

  test("MATCH ... RETURN ...") {
    "MATCH (n) RETURN n, n.prop AS m" should parse[Statement].withAstLike(columns("n", "m"))
    "MATCH (n) WITH 1 AS x RETURN x" should parse[Statement].withAstLike(columns("x"))
  }

  test("UNION") {
    "MATCH (n) RETURN n UNION MATCH (n) RETURN n" should parse[Statement].withAstLike(columns("n"))
    "MATCH (n) RETURN n UNION ALL MATCH (n) RETURN n" should parse[Statement].withAstLike(columns("n"))
  }

  test("CALL ... YIELD ...") {
    "CALL foo YIELD x, y" should parse[Statement](NotAnyAntlr).withAstLike(columns("x", "y"))
    "CALL foo YIELD x, y AS z" should parse[Statement](NotAnyAntlr).withAstLike(columns("x", "z"))
  }

  test("Updates") {
    "MATCH (n) CREATE ()" should parse[Statement].withAstLike(columns())
    "MATCH (n) SET n.prop=12" should parse[Statement].withAstLike(columns())
    "MATCH (n) REMOVE n.prop" should parse[Statement].withAstLike(columns())
    "MATCH (n) DELETE (m)" should parse[Statement].withAstLike(columns())
    "MATCH (n) MERGE (m:Person {name: 'Stefan'}) ON MATCH SET n.happy = 100" should
      parse[Statement].withAstLike(columns())
    "MATCH (n) FOREACH (m IN [1,2,3] | CREATE())" should parse[Statement](NotAnyAntlr).withAstLike(columns())
  }
}
