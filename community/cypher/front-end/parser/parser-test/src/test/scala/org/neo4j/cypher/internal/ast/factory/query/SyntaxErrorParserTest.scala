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
import org.neo4j.cypher.internal.ast.test.util.AstParsing.ParserInTest
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.util.InputPosition

/** Make sure syntax error messages are stable */
class SyntaxErrorParserTest extends AstParsingTestBase {

  private def invalid(input: String, expected: String, offset: Int): Unit = {
    invalid(_ => (input, expected, offset))
  }

  private def invalid(expected: ParserInTest => (String, String, Int)): Unit = {
    failsParsing[Statements].in(pit => {
      val (input, expectedTokens, offset) = expected(pit)
      val pos = InputPosition(offset, 1, offset + 1)
      _.withSyntaxError(
        s"""Invalid input '$input': expected $expectedTokens ($pos)
           |"$testName"
           | ${" " * pos.offset}^""".stripMargin
      )
    })
  }

  test("merge") { invalid("", "a graph pattern", 5) }
  test("match") { invalid("", "a graph pattern, 'DIFFERENT' or 'REPEATABLE'", 5) }

  test("for each") {
    invalid({
      case Cypher5 => (
          "for",
          "'ALTER', 'ORDER BY', 'CALL', 'USING PERIODIC COMMIT', 'CREATE', 'LOAD CSV', 'START DATABASE', 'STOP DATABASE', 'DEALLOCATE', 'DELETE', 'DENY', 'DETACH', 'DROP', 'DRYRUN', 'FINISH', 'FOREACH', 'GRANT', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REALLOCATE', 'REMOVE', 'RENAME', 'RETURN', 'REVOKE', 'ENABLE SERVER', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNWIND', 'USE' or 'WITH'",
          0
        )
      // ≥ Cypher25: `FOR` begins forListClause; after variable `each`, `IN` is required (not FOREACH shorthand).
      case _ => (
          "",
          "'IN'",
          8
        )
    })
  }

  test("create") {
    invalid({
      case Cypher5 => (
          "",
          "a graph pattern, 'ALIAS', 'CONSTRAINT', 'DATABASE', 'COMPOSITE DATABASE', 'IMMUTABLE', 'INDEX', 'BTREE INDEX', 'FULLTEXT INDEX', 'LOOKUP INDEX', 'POINT INDEX', 'RANGE INDEX', 'TEXT INDEX', 'VECTOR INDEX', 'OR REPLACE', 'ROLE' or 'USER'",
          6
        )
      // ≥ Cypher25
      case _ => (
          "",
          "a graph pattern, 'ALIAS', 'CONSTRAINT', 'DATABASE', 'COMPOSITE DATABASE', 'REPLICA DATABASE', 'IMMUTABLE', 'INDEX', 'FULLTEXT INDEX', 'LOOKUP INDEX', 'POINT INDEX', 'RANGE INDEX', 'TEXT INDEX', 'VECTOR INDEX', 'OR REPLACE', 'ROLE', 'AUTH RULE' or 'USER'",
          6
        )
    })
  }

  test("return") {
    invalid({
      case Cypher5 => ("", "an expression, '*' or 'DISTINCT'", 6)
      // ≥ Cypher25
      case _ => ("", "an expression, '*', 'ALL' or 'DISTINCT'", 6)
    })
  }
  test("insert") { invalid("", "'('", 6) }
  test("delete") { invalid("", "an expression", 6) }

  test("set") {
    invalid({
      case Cypher5 => ("", "an expression", 3)
      // ≥ Cypher25
      case _ => ("", "a variable name or an expression", 3)
    })
  }

  test("remove") {
    invalid({
      case Cypher5 => ("", "an expression", 6)
      // ≥ Cypher25
      case _ => ("", "a variable name or an expression", 6)
    })
  }

  test("with") {
    invalid({
      case Cypher5 => ("", "an expression, '*' or 'DISTINCT'", 4)
      // ≥ Cypher25
      case _ => ("", "an expression, '*', 'ALL' or 'DISTINCT'", 4)
    })
  }
  test("unwind") { invalid("", "an expression", 6) }
  test("call") { invalid("", "an identifier, '(' or '{'", 4) }
  test("load csv") { invalid("", "'FROM' or 'WITH HEADERS'", 8) }

  test("match (a)-[r]>(b) return *") { invalid(">", "'-'", 13) }
  test("match (a)-[:]->(b) return *") { invalid("]", "a node label/relationship type name, '$', '%' or '('", 12) }

  test("match (a)-[->() return *") {
    invalid("-", "a parameter, a variable name, '*', ':', 'IS', 'WHERE', ']' or '{'", 11)
  }

  test("match (a)-(b) return *") {
    invalid({
      case Cypher5 => ("(", "'-'", 10)
      // ≥ Cypher25
      case _ => ("(", "'-' or '['", 10)
    })
  }

  test("match (1bcd) return *") {
    invalid("1bcd", "a graph pattern, a parameter, a variable name, ')', ':', 'IS', 'WHERE' or '{'", 7)
  }
  test("match (`a`b`) return *") { invalid("b", "a graph pattern, a parameter, ')', ':', 'IS', 'WHERE' or '{'", 10) }

  test("atch (n) return *") {
    invalid({
      case Cypher5 => ("atch", clauseExpected(CypherVersion.Cypher5), 0)
      // ≥ Cypher25
      case _ => ("atch", clauseExpected(CypherVersion.Cypher25), 0)
    })
  }
  test("match (n:*) return *") { invalid("*", "a node label/relationship type name, '$', '%' or '('", 9) }
  test("match (n:Label|) return *") { invalid("|", "a parameter, '&', ')', ':', 'WHERE', '{' or '|'", 14) }

  test("match (n:Label:) return *") {
    invalid({
      case Cypher5 => (":", "a parameter, '&', ')', ':', 'WHERE', '{' or '|'", 14)
      // ≥ Cypher25
      case _ => (")", "a node label/relationship type name, '$', '%' or '('", 15)
    })
  }

  test("match (n:Label:1Label) return *") {
    invalid({
      case Cypher5 => (":", "a parameter, '&', ')', ':', 'WHERE', '{' or '|'", 14)
      // ≥ Cypher25
      case _ => ("1Label", "a node label/relationship type name, '$', '%' or '('", 15)
    })
  }
  test("match (n:1Label) return *") { invalid("1Label", "a node label/relationship type name, '$', '%' or '('", 9) }
  test("match (n:`1Labe`l`) return *") { invalid("l", "a parameter, '&', ')', ':', 'WHERE', '{' or '|'", 16) }
  test("match (n {p}) return *") { invalid("}", "':'", 11) }
  test("match (n {p:}) return *") { invalid("}", "an expression", 12) }
  test("match (n {p:{}) return *") { invalid(")", "an expression, ',' or '}'", 14) }

  test("matxh (n) return *") {
    invalid({
      case Cypher5 => ("matxh", clauseExpected(CypherVersion.Cypher5), 0)
      // ≥ Cypher25
      case _ => ("matxh", clauseExpected(CypherVersion.Cypher25), 0)
    })
  }

  test("match (n1) matxh (n2) return *") {
    invalid({
      case Cypher5 => (
          "matxh",
          "a graph pattern, ',', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'FOREACH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'USING', 'WHERE', 'WITH' or <EOF>",
          11
        )
      // ≥ Cypher25
      case _ => (
          "matxh",
          "a graph pattern, ',', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', 'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SEARCH', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'USING', 'WHERE', 'WITH' or <EOF>",
          11
        )
    })
  }

  test("match (n1) match (n2) match (n3) matxh(n4) return *") {
    invalid({
      case Cypher5 => (
          "matxh",
          "a graph pattern, ',', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'FOREACH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'USING', 'WHERE', 'WITH' or <EOF>",
          33
        )
      // ≥ Cypher25
      case _ => (
          "matxh",
          "a graph pattern, ',', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', 'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SEARCH', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'USING', 'WHERE', 'WITH' or <EOF>",
          33
        )
    })
  }

  test("match (n1) match (n2) with * limit 1 matxh(n3) return *") {
    invalid({
      case Cypher5 => (
          "matxh",
          "an expression, 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'FOREACH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH' or <EOF>",
          37
        )
      // ≥ Cypher25
      case _ => (
          "matxh",
          "an expression, 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', 'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH' or <EOF>",
          37
        )
    })
  }

  test("match (n) where exists { match (n2) wrongClause } return 1") {
    invalid({
      case Cypher5 => (
          "wrongClause",
          "a graph pattern, ',', 'USING' or 'WHERE'",
          36
        )
      // ≥ Cypher25
      case _ => (
          "{",
          "an expression, 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', 'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SEARCH', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF>",
          23
        )
    })
  }

  test("create (a)-[r]>(b) return *") { invalid(">", "'-'", 14) }
  test("create (a)-[:]->(b) return *") { invalid("]", "a node label/relationship type name, '$', '%' or '('", 13) }

  test("create (a)-[->() return *") {
    invalid("-", "a parameter, a variable name, '*', ':', 'IS', 'WHERE', ']' or '{'", 12)
  }

  test("create (a)-(b) return *") {
    invalid({
      case Cypher5 => ("(", "'-'", 11)
      // ≥ Cypher25
      case _ => ("(", "'-' or '['", 11)
    })
  }

  test("create (1bcd) return *") {
    invalid("1bcd", "a graph pattern, a parameter, a variable name, ')', ':', 'IS', 'WHERE' or '{'", 8)
  }
  test("create (`a`b`) return *") { invalid("b", "a graph pattern, a parameter, ')', ':', 'IS', 'WHERE' or '{'", 11) }

  test("reate (n) return *") {
    invalid({
      case Cypher5 => ("reate", clauseExpected(CypherVersion.Cypher5), 0)
      // ≥ Cypher25
      case _ => ("reate", clauseExpected(CypherVersion.Cypher25), 0)
    })
  }
  test("create (n:*) return *") { invalid("*", "a node label/relationship type name, '$', '%' or '('", 10) }
  test("create (n:Label|) return *") { invalid("|", "a parameter, '&', ')', ':', 'WHERE', '{' or '|'", 15) }

  test("create (n:Label:) return *") {
    invalid({
      case Cypher5 => (":", "a parameter, '&', ')', ':', 'WHERE', '{' or '|'", 15)
      // ≥ Cypher25
      case _ => (")", "a node label/relationship type name, '$', '%' or '('", 16)
    })
  }

  test("create (n:Label:1Label) return *") {
    invalid({
      case Cypher5 => (":", "a parameter, '&', ')', ':', 'WHERE', '{' or '|'", 15)
      // ≥ Cypher25
      case _ => ("1Label", "a node label/relationship type name, '$', '%' or '('", 16)
    })
  }
  test("create (n:1Label) return *") { invalid("1Label", "a node label/relationship type name, '$', '%' or '('", 10) }
  test("create (n:`1Labe`l`) return *") { invalid("l", "a parameter, '&', ')', ':', 'WHERE', '{' or '|'", 17) }
  test("create (n {p:{}) return *") { invalid(")", "an expression, ',' or '}'", 15) }
  test("create (n {p:}) return *") { invalid("}", "an expression", 13) }

  test("merge (a)-[r]>(b) return *") { invalid(">", "'-'", 13) }
  test("merge (a)-[:]->(b) return *") { invalid("]", "a node label/relationship type name, '$', '%' or '('", 12) }

  test("merge (a)-[->() return *") {
    invalid("-", "a parameter, a variable name, '*', ':', 'IS', 'WHERE', ']' or '{'", 11)
  }

  test("merge (a)-(b) return *") {
    invalid({
      case Cypher5 => ("(", "'-'", 10)
      // ≥ Cypher25
      case _ => ("(", "'-' or '['", 10)
    })
  }

  test("merge (1bcd) return *") {
    invalid("1bcd", "a graph pattern, a parameter, a variable name, ')', ':', 'IS', 'WHERE' or '{'", 7)
  }
  test("merge (`a`b`) return *") { invalid("b", "a graph pattern, a parameter, ')', ':', 'IS', 'WHERE' or '{'", 10) }

  test("erge (n) return *") {
    invalid({
      case Cypher5 => ("erge", clauseExpected(CypherVersion.Cypher5), 0)
      // ≥ Cypher25
      case _ => ("erge", clauseExpected(CypherVersion.Cypher25), 0)
    })
  }
  test("merge (n:*) return *") { invalid("*", "a node label/relationship type name, '$', '%' or '('", 9) }
  test("merge (n:Label|) return *") { invalid("|", "a parameter, '&', ')', ':', 'WHERE', '{' or '|'", 14) }

  test("merge (n:Label:) return *") {
    invalid({
      case Cypher5 => (":", "a parameter, '&', ')', ':', 'WHERE', '{' or '|'", 14)
      // ≥ Cypher25
      case _ => (")", "a node label/relationship type name, '$', '%' or '('", 15)
    })
  }

  test("merge (n:Label:1Label) return *") {
    invalid({
      case Cypher5 => (":", "a parameter, '&', ')', ':', 'WHERE', '{' or '|'", 14)
      // ≥ Cypher25
      case _ => ("1Label", "a node label/relationship type name, '$', '%' or '('", 15)
    })
  }
  test("merge (n:1Label) return *") { invalid("1Label", "a node label/relationship type name, '$', '%' or '('", 9) }
  test("merge (n:`1Labe`l`) return *") { invalid("l", "a parameter, '&', ')', ':', 'WHERE', '{' or '|'", 16) }
  test("merge (n {p}) return *") { invalid("}", "':'", 11) }
  test("merge (n {p:}) return *") { invalid("}", "an expression", 12) }
  test("merge (n {p:{}) return *") { invalid(")", "an expression, ',' or '}'", 14) }

  test("return `a`b`") {
    invalid({
      case Cypher5 => (
          "b",
          "an expression, ',', 'AS', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'FOREACH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF>",
          10
        )
      // ≥ Cypher25
      case _ => (
          "b",
          "an expression, ',', 'AS', 'GROUP BY', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', 'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF>",
          10
        )
    })
  }
  test("return [1,") { invalid("", "an expression", 10) }
  test("return [1") { invalid("", "an expression, ',' or ']'", 9) }
  test("return [") { invalid("", "an expression", 8) }
  test("return {1a:''}") { invalid("1a", "an identifier or '}'", 8) }

  test("return true AN false") {
    invalid({
      case Cypher5 => (
          "AN",
          "an expression, ',', 'AS', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'FOREACH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF>",
          12
        )
      // ≥ Cypher25
      case _ => (
          "AN",
          "an expression, ',', 'AS', 'GROUP BY', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', 'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF>",
          12
        )
    })
  }
  test("return {") { invalid("", "an identifier or '}'", 8) }

  test("eturn 1") {
    invalid({
      case Cypher5 => ("eturn", clauseExpected(CypherVersion.Cypher5), 0)
      // ≥ Cypher25
      case _ => ("eturn", clauseExpected(CypherVersion.Cypher25), 0)
    })
  }

  test("return 1 skip") { invalid("", "an expression", 13) }
  test("return 1 skip *") { invalid("*", "an expression", 14) }
  test("return 1 limit") { invalid("", "an expression", 14) }
  test("return 1 limit *") { invalid("*", "an expression", 15) }
  test("return 1 order by") { invalid("", "an expression", 17) }
  test("return 1 order by *") { invalid("*", "an expression", 18) }
  test("return 1 order by x,") { invalid("", "an expression", 20) }
  test("return 1 order by x,*") { invalid("*", "an expression", 20) }

  test("call hej() yield x as 1y") {
    invalid({
      case Cypher5 => ("1y", "an identifier", 22)
      // ≥ Cypher25
      case _ => ("1y", "a variable name", 22)
    })
  }

  test("call hej() yield 1x as y") {
    invalid({
      case Cypher5 => ("1x", "an identifier or '*'", 17)
      // ≥ Cypher25
      case _ => ("1x", "a variable name or '*'", 17)
    })
  }
  test("call 1hej()") { invalid("1hej", "an identifier, '(' or '{'", 5) }
  test("show procedures yield") { invalid("", "a variable name or '*'", 21) }

  test("create database 1a") {
    invalid("1a", "a database name, a graph pattern or a parameter", 16)
  }

  test("with 1 as 1p return *") {
    invalid({
      case Cypher5 => ("1p", "an identifier", 10)
      // ≥ Cypher25
      case _ => ("1p", "a variable name", 10)
    })
  }

  test("with 1 as 1bcd return *") {
    invalid({
      case Cypher5 => ("1bcd", "an identifier", 10)
      // ≥ Cypher25
      case _ => ("1bcd", "a variable name", 10)
    })
  }

  test("revoke") {
    invalid({
      case Cypher5 => (
          "",
          "'ACCESS', 'ALIAS', 'ALL', 'ALTER', 'ASSIGN', 'COMPOSITE', 'CONSTRAINT', 'CONSTRAINTS', 'CREATE', 'DATABASE', 'DELETE', 'DENY', 'DROP', 'EXECUTE', 'GRANT', 'IMMUTABLE', 'IMPERSONATE', 'INDEX', 'INDEXES', 'MATCH', 'MERGE', 'NAME', 'LOAD ON', 'WRITE ON', 'PRIVILEGE', 'READ', 'REMOVE', 'RENAME', 'ROLE', 'ROLES', 'SERVER', 'SET', 'SHOW', 'START', 'STOP', 'TERMINATE', 'TRANSACTION', 'TRAVERSE' or 'USER'",
          6
        )
      case _ => (
          "",
          "'ACCESS', 'ALIAS', 'ALL', 'ALTER', 'ASSIGN', 'COMPOSITE', 'CONSTRAINT', 'CONSTRAINTS', 'CREATE', 'DATABASE', 'DELETE', 'DENY', 'DROP', 'EXECUTE', 'GRANT', 'IMMUTABLE', 'IMPERSONATE', 'INDEX', 'INDEXES', 'MATCH', 'MERGE', 'NAME', 'LOAD ON', 'PRIVILEGE', 'READ', 'REMOVE', 'RENAME', 'ROLE', 'ROLES', 'AUTH RULE', 'SECRETS', 'SERVER', 'SET', 'SHOW', 'START', 'STOP', 'TERMINATE', 'TRANSACTION', 'TRAVERSE', 'USER' or 'WRITE'",
          6
        )
    })
  }

  test("revoke deny") {
    invalid({
      case Cypher5 => (
          "",
          "'ACCESS', 'ALIAS', 'ALL', 'ALTER', 'ASSIGN', 'COMPOSITE', 'CONSTRAINT', 'CONSTRAINTS', 'CREATE', 'DATABASE', 'DELETE', 'DROP', 'EXECUTE', 'IMPERSONATE', 'INDEX', 'INDEXES', 'MATCH', 'MERGE', 'NAME', 'LOAD ON', 'WRITE ON', 'PRIVILEGE', 'READ', 'REMOVE', 'RENAME', 'ROLE', 'SERVER', 'SET', 'SHOW', 'START', 'STOP', 'TERMINATE', 'TRANSACTION', 'TRAVERSE' or 'USER'",
          11
        )
      case _ => (
          "",
          "'ACCESS', 'ALIAS', 'ALL', 'ALTER', 'ASSIGN', 'COMPOSITE', 'CONSTRAINT', 'CONSTRAINTS', 'CREATE', 'DATABASE', 'DELETE', 'DROP', 'EXECUTE', 'IMPERSONATE', 'INDEX', 'INDEXES', 'MATCH', 'MERGE', 'NAME', 'LOAD ON', 'PRIVILEGE', 'READ', 'REMOVE', 'RENAME', 'ROLE', 'AUTH RULE', 'SECRETS', 'SERVER', 'SET', 'SHOW', 'START', 'STOP', 'TERMINATE', 'TRANSACTION', 'TRAVERSE', 'USER' or 'WRITE'",
          11
        )
    })
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

  test("rename") {
    invalid({
      case Cypher5 => ("", "'ROLE', 'SERVER' or 'USER'", 6)
      case _       => ("", "'ROLE', 'AUTH RULE', 'SERVER' or 'USER'", 6)
    })
  }

  test("alter") {
    invalid({
      case Cypher5 => ("", "'ALIAS', 'DATABASE', 'CURRENT USER SET PASSWORD FROM', 'SERVER' or 'USER'", 5)
      case _       => ("", "'ALIAS', 'CURRENT', 'DATABASE', 'AUTH RULE', 'SERVER', 'USER' or 'USERS'", 5)
    })
  }

  test("drop") {
    invalid({
      case Cypher5 => (
          "",
          "'ALIAS', 'COMPOSITE', 'CONSTRAINT', 'DATABASE', 'INDEX', 'ROLE', 'SERVER' or 'USER'",
          4
        )
      // ≥ Cypher25
      case _ => (
          "",
          "'ALIAS', 'COMPOSITE', 'CONSTRAINT', 'DATABASE', 'INDEX', 'ROLE', 'AUTH RULE', 'SERVER' or 'USER'",
          4
        )
    })
  }

  test("grant") {
    invalid({
      case Cypher5 => (
          "",
          "'ACCESS', 'ALIAS', 'ALL', 'ALTER', 'ASSIGN', 'COMPOSITE', 'CONSTRAINT', 'CONSTRAINTS', 'CREATE', 'DATABASE', 'DELETE', 'DROP', 'EXECUTE', 'IMMUTABLE', 'IMPERSONATE', 'INDEX', 'INDEXES', 'MATCH', 'MERGE', 'NAME', 'LOAD ON', 'WRITE ON', 'PRIVILEGE', 'READ', 'REMOVE', 'RENAME', 'ROLE', 'ROLES', 'SERVER', 'SET', 'SHOW', 'START', 'STOP', 'TERMINATE', 'TRANSACTION', 'TRAVERSE' or 'USER'",
          5
        )
      case _ => (
          "",
          "'ACCESS', 'ALIAS', 'ALL', 'ALTER', 'ASSIGN', 'COMPOSITE', 'CONSTRAINT', 'CONSTRAINTS', 'CREATE', 'DATABASE', 'DELETE', 'DROP', 'EXECUTE', 'IMMUTABLE', 'IMPERSONATE', 'INDEX', 'INDEXES', 'MATCH', 'MERGE', 'NAME', 'LOAD ON', 'PRIVILEGE', 'READ', 'REMOVE', 'RENAME', 'ROLE', 'ROLES', 'AUTH RULE', 'SECRETS', 'SERVER', 'SET', 'SHOW', 'START', 'STOP', 'TERMINATE', 'TRANSACTION', 'TRAVERSE', 'USER' or 'WRITE'",
          5
        )
    })
  }

  test("deny") {
    invalid({
      case Cypher5 => (
          "",
          "'ACCESS', 'ALIAS', 'ALL', 'ALTER', 'ASSIGN', 'COMPOSITE', 'CONSTRAINT', 'CONSTRAINTS', 'CREATE', 'DATABASE', 'DELETE', 'DROP', 'EXECUTE', 'IMPERSONATE', 'INDEX', 'INDEXES', 'MATCH', 'MERGE', 'NAME', 'LOAD ON', 'WRITE ON', 'PRIVILEGE', 'READ', 'REMOVE', 'RENAME', 'ROLE', 'SERVER', 'SET', 'SHOW', 'START', 'STOP', 'TERMINATE', 'TRANSACTION', 'TRAVERSE' or 'USER'",
          4
        )
      case _ => (
          "",
          "'ACCESS', 'ALIAS', 'ALL', 'ALTER', 'ASSIGN', 'COMPOSITE', 'CONSTRAINT', 'CONSTRAINTS', 'CREATE', 'DATABASE', 'DELETE', 'DROP', 'EXECUTE', 'IMPERSONATE', 'INDEX', 'INDEXES', 'MATCH', 'MERGE', 'NAME', 'LOAD ON', 'PRIVILEGE', 'READ', 'REMOVE', 'RENAME', 'ROLE', 'AUTH RULE', 'SECRETS', 'SERVER', 'SET', 'SHOW', 'START', 'STOP', 'TERMINATE', 'TRANSACTION', 'TRAVERSE', 'USER' or 'WRITE'",
          4
        )
    })
  }

  test("return ******") {
    failsParsing[Statements].in(_ => {
      _.withSyntaxError(
        s"""Invalid input '******' (line 1, column 8 (offset: 7))
           |"$testName"
           | ${" " * 7}^""".stripMargin
      )
    })
  }

  test("call (******) { return 1 as x } return x") {
    invalid("******", "a variable name, ')' or '*'", 6)
  }
}

object SyntaxErrorParserTest {

  val clausesNotInCypher25: Seq[String] = Seq("USING PERIODIC COMMIT")

  val clausesNotInCypher5: Seq[String] = Seq("FILTER", "LET", "WHEN", "{")

  def clauseExpected(cypherVersion: CypherVersion): String = {
    var tokens = Seq(
      "ALTER",
      "ORDER BY",
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
      "FILTER",
      "FINISH",
      "FOREACH",
      "GRANT",
      "INSERT",
      "LET",
      "LIMIT",
      "MATCH",
      "MERGE",
      "NODETACH",
      "OFFSET",
      "OPTIONAL",
      "REALLOCATE",
      "REMOVE",
      "RENAME",
      "RETURN",
      "REVOKE",
      "ENABLE SERVER",
      "SET",
      "SHOW",
      "SKIP",
      "TERMINATE",
      "UNWIND",
      "USE",
      "WHEN",
      "WITH",
      "{"
    )
    if (cypherVersion == CypherVersion.Cypher25) {
      tokens = tokens.filter(token => !clausesNotInCypher25.contains(token))
      val foreachIdx = tokens.indexOf("FOREACH")
      tokens = tokens.take(foreachIdx) ++ Seq("FOR") ++ tokens.drop(foreachIdx)
    }

    if (cypherVersion == CypherVersion.Cypher5) {
      tokens = tokens.filter(token => !clausesNotInCypher5.contains(token))
    }

    tokens.dropRight(1).map(s => s"'$s'").mkString(", ") + " or '" + tokens.last + "'"
  }
}
