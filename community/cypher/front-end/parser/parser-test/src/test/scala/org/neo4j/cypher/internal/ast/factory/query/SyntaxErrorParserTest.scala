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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.query.SyntaxErrorParserTest.clauseExpected
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher6
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory

/** Make sure syntax error messages are stable */
class SyntaxErrorParserTest extends AstParsingTestBase {

  private def invalid(input: String, expected: String, offset: Int): Unit = {
    invalid(input, expected, expected, offset)
  }

  private def invalid(input: String, expected5: String, expected6: String, offset: Int): Unit = {
    val pos = InputPosition(offset, 1, offset + 1)
    val expectedMessageCypher5 =
      s"""Invalid input '$input': expected $expected5 ($pos)
         |"$testName"
         | ${" ".repeat(pos.offset)}^""".stripMargin
    val expectedMessageCypher6 =
      s"""Invalid input '$input': expected $expected6 ($pos)
         |"$testName"
         | ${" ".repeat(pos.offset)}^""".stripMargin
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.throws[OpenCypherExceptionFactory.SyntaxException]
      case Cypher5       => _.withSyntaxError(expectedMessageCypher5)
      case Cypher6       => _.withSyntaxError(expectedMessageCypher6)
    }
  }

  test("merge") { invalid("", "a graph pattern", 5) }
  test("match") { invalid("", "a graph pattern, 'DIFFERENT' or 'REPEATABLE'", 5) }

  test("for each") {
    invalid(
      "for",
      "'FOREACH', 'ALTER', 'CALL', 'USING PERIODIC COMMIT', 'CREATE', 'LOAD CSV', 'START DATABASE', 'STOP DATABASE', 'DEALLOCATE', 'DELETE', 'DENY', 'DETACH', 'DROP', 'DRYRUN', 'FINISH', 'GRANT', 'INSERT', 'MATCH', 'MERGE', 'NODETACH', 'OPTIONAL', 'REALLOCATE', 'REMOVE', 'RENAME', 'RETURN', 'REVOKE', 'ENABLE SERVER', 'SET', 'SHOW', 'TERMINATE', 'UNWIND', 'USE' or 'WITH'",
      "'FOREACH', 'ALTER', 'CALL', 'CREATE', 'LOAD CSV', 'START DATABASE', 'STOP DATABASE', 'DEALLOCATE', 'DELETE', 'DENY', 'DETACH', 'DROP', 'DRYRUN', 'FINISH', 'GRANT', 'INSERT', 'MATCH', 'MERGE', 'NODETACH', 'OPTIONAL', 'REALLOCATE', 'REMOVE', 'RENAME', 'RETURN', 'REVOKE', 'ENABLE SERVER', 'SET', 'SHOW', 'TERMINATE', 'UNWIND', 'USE' or 'WITH'",
      0
    )
  }

  test("create") {
    invalid(
      "",
      "a graph pattern, 'ALIAS', 'CONSTRAINT', 'DATABASE', 'COMPOSITE DATABASE', 'INDEX', 'BTREE INDEX', 'FULLTEXT INDEX', 'LOOKUP INDEX', 'POINT INDEX', 'RANGE INDEX', 'TEXT INDEX', 'VECTOR INDEX', 'OR REPLACE', 'ROLE' or 'USER'",
      6
    )
  }
  test("return") { invalid("", "an expression, '*' or 'DISTINCT'", 6) }
  test("insert") { invalid("", "'('", 6) }
  test("delete") { invalid("", "an expression", 6) }
  test("set") { invalid("", "an expression", 3) }
  test("remove") { invalid("", "an expression", 6) }
  test("with") { invalid("", "an expression, '*' or 'DISTINCT'", 4) }
  test("unwind") { invalid("", "an expression", 6) }
  test("call") { invalid("", "an identifier or '{'", 4) }
  test("load csv") { invalid("", "'FROM' or 'WITH HEADERS'", 8) }
  test("match (a)-[r]>(b) return *") { invalid(">", "'-'", 13) }
  test("match (a)-[:]->(b) return *") { invalid("]", "a node label/relationship type name, '%' or '('", 12) }

  test("match (a)-[->() return *") {
    invalid("-", "a parameter, a variable name, '*', ':', 'IS', 'WHERE', ']' or '{'", 11)
  }
  test("match (a)-(b) return *") { invalid("(", "'-'", 10) }

  test("match (1bcd) return *") {
    invalid("1bcd", "a graph pattern, a parameter, a variable name, ')', ':', 'IS', 'WHERE' or '{'", 7)
  }
  test("match (`a`b`) return *") { invalid("b", "a graph pattern, a parameter, ')', ':', 'IS', 'WHERE' or '{'", 10) }

  test("atch (n) return *") {
    invalid("atch", clauseExpected(CypherVersion.Cypher5), clauseExpected(CypherVersion.Cypher6), 0)
  }
  test("match (n:*) return *") { invalid("*", "a node label/relationship type name, '%' or '('", 9) }
  test("match (n:Label|) return *") { invalid("|", "a parameter, '&', ')', ':', 'WHERE', '{' or '|'", 14) }
  test("match (n:Label:) return *") { invalid(":", "a parameter, '&', ')', ':', 'WHERE', '{' or '|'", 14) }
  test("match (n:Label:1Label) return *") { invalid(":", "a parameter, '&', ')', ':', 'WHERE', '{' or '|'", 14) }
  test("match (n:1Label) return *") { invalid("1Label", "a node label/relationship type name, '%' or '('", 9) }
  test("match (n:`1Labe`l`) return *") { invalid("l", "a parameter, '&', ')', ':', 'WHERE', '{' or '|'", 16) }
  test("match (n {p}) return *") { invalid("}", "':'", 11) }
  test("match (n {p:}) return *") { invalid("}", "an expression", 12) }
  test("match (n {p:{}) return *") { invalid(")", "an expression, ',' or '}'", 14) }
  test("create (a)-[r]>(b) return *") { invalid(">", "'-'", 14) }
  test("create (a)-[:]->(b) return *") { invalid("]", "a node label/relationship type name, '%' or '('", 13) }

  test("create (a)-[->() return *") {
    invalid("-", "a parameter, a variable name, '*', ':', 'IS', 'WHERE', ']' or '{'", 12)
  }
  test("create (a)-(b) return *") { invalid("(", "'-'", 11) }

  test("create (1bcd) return *") {
    invalid("1bcd", "a graph pattern, a parameter, a variable name, ')', ':', 'IS', 'WHERE' or '{'", 8)
  }
  test("create (`a`b`) return *") { invalid("b", "a graph pattern, a parameter, ')', ':', 'IS', 'WHERE' or '{'", 11) }

  test("reate (n) return *") {
    invalid("reate", clauseExpected(CypherVersion.Cypher5), clauseExpected(CypherVersion.Cypher6), 0)
  }
  test("create (n:*) return *") { invalid("*", "a node label/relationship type name, '%' or '('", 10) }
  test("create (n:Label|) return *") { invalid("|", "a parameter, '&', ')', ':', 'WHERE', '{' or '|'", 15) }
  test("create (n:Label:) return *") { invalid(":", "a parameter, '&', ')', ':', 'WHERE', '{' or '|'", 15) }
  test("create (n:Label:1Label) return *") { invalid(":", "a parameter, '&', ')', ':', 'WHERE', '{' or '|'", 15) }
  test("create (n:1Label) return *") { invalid("1Label", "a node label/relationship type name, '%' or '('", 10) }
  test("create (n:`1Labe`l`) return *") { invalid("l", "a parameter, '&', ')', ':', 'WHERE', '{' or '|'", 17) }
  test("create (n {p:{}) return *") { invalid(")", "an expression, ',' or '}'", 15) }
  test("create (n {p:}) return *") { invalid("}", "an expression", 13) }
  test("merge (a)-[r]>(b) return *") { invalid(">", "'-'", 13) }
  test("merge (a)-[:]->(b) return *") { invalid("]", "a node label/relationship type name, '%' or '('", 12) }

  test("merge (a)-[->() return *") {
    invalid("-", "a parameter, a variable name, '*', ':', 'IS', 'WHERE', ']' or '{'", 11)
  }
  test("merge (a)-(b) return *") { invalid("(", "'-'", 10) }

  test("merge (1bcd) return *") {
    invalid("1bcd", "a graph pattern, a parameter, a variable name, ')', ':', 'IS', 'WHERE' or '{'", 7)
  }
  test("merge (`a`b`) return *") { invalid("b", "a graph pattern, a parameter, ')', ':', 'IS', 'WHERE' or '{'", 10) }

  test("erge (n) return *") {
    invalid("erge", clauseExpected(CypherVersion.Cypher5), clauseExpected(CypherVersion.Cypher6), 0)
  }
  test("merge (n:*) return *") { invalid("*", "a node label/relationship type name, '%' or '('", 9) }
  test("merge (n:Label|) return *") { invalid("|", "a parameter, '&', ')', ':', 'WHERE', '{' or '|'", 14) }
  test("merge (n:Label:) return *") { invalid(":", "a parameter, '&', ')', ':', 'WHERE', '{' or '|'", 14) }
  test("merge (n:Label:1Label) return *") { invalid(":", "a parameter, '&', ')', ':', 'WHERE', '{' or '|'", 14) }
  test("merge (n:1Label) return *") { invalid("1Label", "a node label/relationship type name, '%' or '('", 9) }
  test("merge (n:`1Labe`l`) return *") { invalid("l", "a parameter, '&', ')', ':', 'WHERE', '{' or '|'", 16) }
  test("merge (n {p}) return *") { invalid("}", "':'", 11) }
  test("merge (n {p:}) return *") { invalid("}", "an expression", 12) }
  test("merge (n {p:{}) return *") { invalid(")", "an expression, ',' or '}'", 14) }

  test("return `a`b`") {
    invalid(
      "b",
      "an expression, 'FOREACH', ',', 'AS', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF>",
      10
    )
  }
  test("return [1,") { invalid("", "an expression", 10) }
  test("return [1") { invalid("", "an expression, ',' or ']'", 9) }
  test("return [") { invalid("", "an expression", 8) }
  test("return {1a:''}") { invalid("1a", "an identifier or '}'", 8) }

  test("return true AN false") {
    invalid(
      "AN",
      "an expression, 'FOREACH', ',', 'AS', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF>",
      12
    )
  }
  test("return {") { invalid("", "an identifier or '}'", 8) }
  test("eturn 1") { invalid("eturn", clauseExpected(CypherVersion.Cypher5), clauseExpected(CypherVersion.Cypher6), 0) }
  test("return 1 skip") { invalid("", "an expression", 13) }
  test("return 1 skip *") { invalid("*", "an expression", 14) }
  test("return 1 limit") { invalid("", "an expression", 14) }
  test("return 1 limit *") { invalid("*", "an expression", 15) }
  test("return 1 order by") { invalid("", "an expression", 17) }
  test("return 1 order by *") { invalid("*", "an expression", 18) }
  test("return 1 order by x,") { invalid("", "an expression", 20) }
  test("return 1 order by x,*") { invalid("*", "an expression", 20) }
  test("call hej() yield x as 1y") { invalid("1y", "an identifier", 22) }
  test("call hej() yield 1x as y") { invalid("1x", "an identifier or '*'", 17) }
  test("call 1hej()") { invalid("1hej", "an identifier or '{'", 5) }
  test("show procedures yield") { invalid("", "a variable name or '*'", 21) }
  test("create database 1a") { invalid("1a", "a database name, a graph pattern or a parameter", 16) }
  test("with 1 as 1p return *") { invalid("1p", "an identifier", 10) }
  test("with 1 as 1bcd return *") { invalid("1bcd", "an identifier", 10) }

  test("revoke") {
    invalid(
      "",
      "'ACCESS', 'ALIAS', 'ALL', 'ALTER', 'ASSIGN', 'COMPOSITE', 'CONSTRAINT', 'CONSTRAINTS', 'CREATE', 'DATABASE', 'DELETE', 'DENY', 'DROP', 'EXECUTE', 'GRANT', 'IMMUTABLE', 'IMPERSONATE', 'INDEX', 'INDEXES', 'MATCH', 'MERGE', 'NAME', 'LOAD ON', 'WRITE ON', 'PRIVILEGE', 'READ', 'REMOVE', 'RENAME', 'ROLE', 'ROLES', 'SERVER', 'SET', 'SHOW', 'START', 'STOP', 'TERMINATE', 'TRANSACTION', 'TRAVERSE' or 'USER'",
      6
    )
  }

  test("revoke deny") {
    invalid(
      "",
      "'ACCESS', 'ALIAS', 'ALL', 'ALTER', 'ASSIGN', 'COMPOSITE', 'CONSTRAINT', 'CONSTRAINTS', 'CREATE', 'DATABASE', 'DELETE', 'DROP', 'EXECUTE', 'IMPERSONATE', 'INDEX', 'INDEXES', 'MATCH', 'MERGE', 'NAME', 'LOAD ON', 'WRITE ON', 'PRIVILEGE', 'READ', 'REMOVE', 'RENAME', 'ROLE', 'SERVER', 'SET', 'SHOW', 'START', 'STOP', 'TERMINATE', 'TRANSACTION', 'TRAVERSE' or 'USER'",
      11
    )
  }
  test("revoke deny all") { invalid("", "'DATABASE', 'DBMS', 'GRAPH', 'ON' or 'PRIVILEGES'", 15) }
  test("start") { invalid("", "'DATABASE'", 5) }
  test("start database") { invalid("", "a database name or a parameter", 14) }
  test("start database a wai") { invalid("wai", "a database name, 'NOWAIT', 'WAIT' or <EOF>", 17) }
  test("stop") { invalid("", "'DATABASE'", 4) }
  test("stop database") { invalid("", "a database name or a parameter", 13) }
  test("stop database a wai") { invalid("wai", "a database name, 'NOWAIT', 'WAIT' or <EOF>", 16) }
  test("deallocate") { invalid("", "'DATABASE' or 'DATABASES'", 10) }
  test("deallocate database") { invalid("", "'FROM'", 19) }
  test("deallocate database from") { invalid("", "'SERVER' or 'SERVERS'", 24) }
  test("deallocate database from server") { invalid("", "a parameter or a string", 31) }
  test("enable server") { invalid("", "a parameter or a string", 13) }
  test("enable server a") { invalid("a", "a parameter or a string", 14) }
  test("enable server 'a' options") { invalid("", "a parameter or '{'", 25) }
  test("enable server 'a' options {") { invalid("", "an identifier or '}'", 27) }
  test("rename") { invalid("", "'ROLE', 'SERVER' or 'USER'", 6) }
  test("alter") { invalid("", "'ALIAS', 'DATABASE', 'CURRENT USER SET PASSWORD FROM', 'SERVER' or 'USER'", 5) }

  test("drop") {
    invalid("", "'ALIAS', 'COMPOSITE', 'CONSTRAINT', 'DATABASE', 'INDEX', 'ROLE', 'SERVER' or 'USER'", 4)
  }

  test("grant") {
    invalid(
      "",
      "'ACCESS', 'ALIAS', 'ALL', 'ALTER', 'ASSIGN', 'COMPOSITE', 'CONSTRAINT', 'CONSTRAINTS', 'CREATE', 'DATABASE', 'DELETE', 'DROP', 'EXECUTE', 'IMMUTABLE', 'IMPERSONATE', 'INDEX', 'INDEXES', 'MATCH', 'MERGE', 'NAME', 'LOAD ON', 'WRITE ON', 'PRIVILEGE', 'READ', 'REMOVE', 'RENAME', 'ROLE', 'ROLES', 'SERVER', 'SET', 'SHOW', 'START', 'STOP', 'TERMINATE', 'TRANSACTION', 'TRAVERSE' or 'USER'",
      5
    )
  }

  test("deny") {
    invalid(
      "",
      "'ACCESS', 'ALIAS', 'ALL', 'ALTER', 'ASSIGN', 'COMPOSITE', 'CONSTRAINT', 'CONSTRAINTS', 'CREATE', 'DATABASE', 'DELETE', 'DROP', 'EXECUTE', 'IMPERSONATE', 'INDEX', 'INDEXES', 'MATCH', 'MERGE', 'NAME', 'LOAD ON', 'WRITE ON', 'PRIVILEGE', 'READ', 'REMOVE', 'RENAME', 'ROLE', 'SERVER', 'SET', 'SHOW', 'START', 'STOP', 'TERMINATE', 'TRANSACTION', 'TRAVERSE' or 'USER'",
      4
    )
  }
}

object SyntaxErrorParserTest {

  val clausesNotInCypher6: Seq[String] = Seq("USING PERIODIC COMMIT")

  def clauseExpected(cypherVersion: CypherVersion): String = {
    var tokens = Seq(
      "FOREACH",
      "ALTER",
      "CALL",
      "USING PERIODIC COMMIT",
      "CREATE",
      "LOAD CSV",
      "START DATABASE",
      "STOP DATABASE",
      "DEALLOCATE",
      "DELETE",
      "DENY",
      "DETACH",
      "DROP",
      "DRYRUN",
      "FINISH",
      "GRANT",
      "INSERT",
      "MATCH",
      "MERGE",
      "NODETACH",
      "OPTIONAL",
      "REALLOCATE",
      "REMOVE",
      "RENAME",
      "RETURN",
      "REVOKE",
      "ENABLE SERVER",
      "SET",
      "SHOW",
      "TERMINATE",
      "UNWIND",
      "USE",
      "WITH"
    )
    if (cypherVersion == CypherVersion.Cypher6) {
      tokens = tokens.filter(token => !clausesNotInCypher6.contains(token))
    }
    tokens.dropRight(1).map(s => s"'$s'").mkString(", ") + " or '" + tokens.last + "'"
  }
}
