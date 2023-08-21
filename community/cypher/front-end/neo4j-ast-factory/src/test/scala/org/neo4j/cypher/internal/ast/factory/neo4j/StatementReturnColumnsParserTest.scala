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
import org.neo4j.cypher.internal.cst.factory.neo4j.AntlrRule
import org.neo4j.cypher.internal.cst.factory.neo4j.Cst

class StatementReturnColumnsParserTest extends ParserTestBase[Cst.Statement, Statement, List[String]] {

  override def convert(statement: Statement): List[String] = statement.returnColumns.map(_.name)

  implicit private val javaccRule: JavaccRule[Statement] = JavaccRule.Statement
  implicit private val antlrRule: AntlrRule[Cst.Statement] = AntlrRule.Statement

  test("MATCH ... RETURN ...") {
    parsing("MATCH (n) RETURN n, n.prop AS m") shouldGive List("n", "m")
    parsing("MATCH (n) WITH 1 AS x RETURN x") shouldGive List("x")
  }

  test("UNION") {
    parsing("MATCH (n) RETURN n UNION MATCH (n) RETURN n") shouldGive List("n")
    parsing("MATCH (n) RETURN n UNION ALL MATCH (n) RETURN n") shouldGive List("n")
  }

  test("CALL ... YIELD ...") {
    parsing("CALL foo YIELD x, y") shouldGive List("x", "y")
    parsing("CALL foo YIELD x, y AS z") shouldGive List("x", "z")
  }

  test("Updates") {
    parsing("MATCH (n) CREATE ()") shouldGive List.empty
    parsing("MATCH (n) SET n.prop=12") shouldGive List.empty
    parsing("MATCH (n) REMOVE n.prop") shouldGive List.empty
    parsing("MATCH (n) DELETE (m)") shouldGive List.empty
    parsing("MATCH (n) MERGE (m:Person {name: 'Stefan'}) ON MATCH SET n.happy = 100") shouldGive List.empty
    parsing("MATCH (n) FOREACH (m IN [1,2,3] | CREATE())") shouldGive List.empty
  }
}
