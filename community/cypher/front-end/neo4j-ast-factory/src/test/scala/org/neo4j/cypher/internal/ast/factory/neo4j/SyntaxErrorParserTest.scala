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
import org.neo4j.cypher.internal.ast.factory.neo4j.SyntaxErrorParserTest.ParsingFailure
import org.neo4j.cypher.internal.ast.factory.neo4j.SyntaxErrorParserTest.ParsingFailure.extraneous
import org.neo4j.cypher.internal.ast.factory.neo4j.SyntaxErrorParserTest.ParsingFailure.mismatch
import org.neo4j.cypher.internal.ast.factory.neo4j.SyntaxErrorParserTest.ParsingFailure.missing
import org.neo4j.cypher.internal.ast.factory.neo4j.SyntaxErrorParserTest.ParsingFailure.notViable
import org.neo4j.cypher.internal.ast.factory.neo4j.SyntaxErrorParserTest.clauseExpected
import org.neo4j.cypher.internal.ast.factory.neo4j.SyntaxErrorParserTest.expectedError
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.Antlr
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.JavaCc
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.exceptions.SyntaxException

class SyntaxErrorParserTest extends AstParsingTestBase {

  private def assertSyntaxError(query: String, expected: ParsingFailure): Unit = {
    query should notParse[Statements]
      .parseIn(JavaCc)(_.throws[OpenCypherExceptionFactory.SyntaxException])
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(expectedError(query, expected)))
  }

  // Note, these messages are not optimal in many cases,
  // the tests are here to make sure errors are stable.
  test("syntax errors have stable error messages in read/write queries") {

    // Unfinished beginning of clauses

    assertSyntaxError(
      "merge",
      mismatch("", "a variable name, 'ANY', 'ALL', 'SHORTEST', 'allShortestPaths', 'shortestPath', '('", 5)
    )
    assertSyntaxError(
      "match",
      mismatch(
        "",
        "'REPEATABLE', 'DIFFERENT', a variable name, 'ANY', 'ALL', 'SHORTEST', 'allShortestPaths', 'shortestPath', '('",
        5
      )
    )
    assertSyntaxError(
      "for each",
      mismatch(
        "for",
        "'USING', 'USE', 'CREATE', 'DROP', 'ALTER', 'RENAME', 'DENY', 'REVOKE', 'GRANT', 'START', 'STOP', 'ENABLE', 'DRYRUN', 'DEALLOCATE', 'REALLOCATE', 'SHOW', 'TERMINATE', 'FINISH', 'RETURN', 'INSERT', 'DETACH', 'NODETACH', 'DELETE', 'SET', 'REMOVE', 'OPTIONAL', 'MATCH', 'MERGE', 'WITH', 'UNWIND', 'CALL', 'LOAD', 'FOREACH'",
        0
      )
    )
    assertSyntaxError("create", notViable("", 6)) // Exceptionally unhelpful
    assertSyntaxError("return", mismatch("", "'DISTINCT', '*', an expression", 6))
    assertSyntaxError("insert", mismatch("", "an identifier, '('", 6))
    assertSyntaxError("delete", mismatch("", "an expression", 6))
    assertSyntaxError("set", mismatch("", "an expression, a variable name", 3))
    assertSyntaxError("remove", mismatch("", "an expression, a variable name", 6))
    assertSyntaxError("with", mismatch("", "'DISTINCT', '*', an expression", 4))
    assertSyntaxError("unwind", mismatch("", "an expression", 6))
    assertSyntaxError("call", notViable("", 4)) // Exceptionally unhelpful
    assertSyntaxError("load csv", mismatch("", "'WITH', 'FROM'", 8))

    // MATCH

    assertSyntaxError("match (a)-[r]>(b) return *", missing("'-'", "'>'", 13))
    assertSyntaxError("match (a)-[:]->(b) return *", mismatch("]", "'!', '(', '%', a relationship type name", 12))
    assertSyntaxError(
      "match (a)-[->() return *",
      mismatch("-", "a variable name, ':', 'IS', '*', '{', '$', 'WHERE', ']'", 11)
    )
    assertSyntaxError("match (a)-(b) return *", mismatch("(", "'[', '-'", 10))
    assertSyntaxError("match (1bcd) return *", notViable("", 7))
    assertSyntaxError("match (`a`b`) return *", notViable("", 10))
    assertSyntaxError("atch (n) return *", mismatch("atch", clauseExpected, 0))
    assertSyntaxError("match (n:*) return *", mismatch("*", "'!', '(', '%', a node label name", 9))
    assertSyntaxError("match (n:Label|) return *", extraneous("|", "'{', '$', 'WHERE', ')'", 14))
    assertSyntaxError("match (n:Label:) return *", extraneous(":", "'{', '$', 'WHERE', ')'", 14))
    assertSyntaxError("match (n:Label:1Label) return *", mismatch(":", "'{', '$', 'WHERE', ')'", 14))
    assertSyntaxError("match (n:1Label) return *", mismatch("1Label", "'!', '(', '%', a node label name", 9))
    assertSyntaxError("match (n:`1Labe`l`) return *", mismatch("l", "'{', '$', 'WHERE', ')'", 16))
    assertSyntaxError("match (n {p}) return *", mismatch("}", "':'", 11))
    assertSyntaxError("match (n {p:}) return *", mismatch("}", "an expression", 12))
    assertSyntaxError("match (n {p:{}) return *", mismatch(")", "',', '}'", 14))

    // CREATE

    assertSyntaxError("create (a)-[r]>(b) return *", missing("'-'", "'>'", 14))
    assertSyntaxError("create (a)-[:]->(b) return *", mismatch("]", "'!', '(', '%', a relationship type name", 13))
    assertSyntaxError(
      "create (a)-[->() return *",
      mismatch("-", "a variable name, ':', 'IS', '*', '{', '$', 'WHERE', ']'", 12)
    )
    assertSyntaxError("create (a)-(b) return *", mismatch("(", "'[', '-'", 11))
    assertSyntaxError("create (1bcd) return *", notViable("", 8))
    assertSyntaxError("create (`a`b`) return *", notViable("", 11))
    assertSyntaxError("reate (n) return *", mismatch("reate", clauseExpected, 0))
    assertSyntaxError("create (n:*) return *", mismatch("*", "'!', '(', '%', a node label name", 10))
    assertSyntaxError("create (n:Label|) return *", extraneous("|", "'{', '$', 'WHERE', ')'", 15))
    assertSyntaxError("create (n:Label:) return *", extraneous(":", "'{', '$', 'WHERE', ')'", 15))
    assertSyntaxError("create (n:Label:1Label) return *", mismatch(":", "'{', '$', 'WHERE', ')'", 15))
    assertSyntaxError("create (n:1Label) return *", mismatch("1Label", "'!', '(', '%', a node label name", 10))
    assertSyntaxError("create (n:`1Labe`l`) return *", mismatch("l", "'{', '$', 'WHERE', ')'", 17))
    assertSyntaxError("create (n {p}) return *", mismatch("}", "':'", 12))
    assertSyntaxError("create (n {p:}) return *", mismatch("}", "an expression", 13))
    assertSyntaxError("create (n {p:{}) return *", mismatch(")", "',', '}'", 15))

    // MERGE

    assertSyntaxError("merge (a)-[r]>(b) return *", missing("'-'", "'>'", 13))
    assertSyntaxError("merge (a)-[:]->(b) return *", mismatch("]", "'!', '(', '%', a relationship type name", 12))
    assertSyntaxError(
      "merge (a)-[->() return *",
      mismatch("-", "a variable name, ':', 'IS', '*', '{', '$', 'WHERE', ']'", 11)
    )
    assertSyntaxError("merge (a)-(b) return *", mismatch("(", "'[', '-'", 10))
    assertSyntaxError("merge (1bcd) return *", notViable("", 7))
    assertSyntaxError("merge (`a`b`) return *", notViable("", 10))
    assertSyntaxError("erge (n) return *", mismatch("erge", clauseExpected, 0))
    assertSyntaxError("merge (n:*) return *", mismatch("*", "'!', '(', '%', a node label name", 9))
    assertSyntaxError("merge (n:Label|) return *", extraneous("|", "'{', '$', 'WHERE', ')'", 14))
    assertSyntaxError("merge (n:Label:) return *", extraneous(":", "'{', '$', 'WHERE', ')'", 14))
    assertSyntaxError("merge (n:Label:1Label) return *", mismatch(":", "'{', '$', 'WHERE', ')'", 14))
    assertSyntaxError("merge (n:1Label) return *", mismatch("1Label", "'!', '(', '%', a node label name", 9))
    assertSyntaxError("merge (n:`1Labe`l`) return *", mismatch("l", "'{', '$', 'WHERE', ')'", 16))
    assertSyntaxError("merge (n {p}) return *", mismatch("}", "':'", 11))
    assertSyntaxError("merge (n {p:}) return *", mismatch("}", "an expression", 12))
    assertSyntaxError("merge (n {p:{}) return *", mismatch(")", "',', '}'", 14))

    // RETURN

    assertSyntaxError("return `a`b`", mismatch("b", "';', <EOF>", 10))
    assertSyntaxError("return [1,", mismatch("", "an expression", 10))
    assertSyntaxError("return [1", mismatch("", "',', ']'", 9))
    assertSyntaxError("return [", notViable("an expression", 8))
    assertSyntaxError("return {1a:''}", mismatch("1a", "an identifier, '}'", 8))
    assertSyntaxError("return true AN false", mismatch("AN", "';', <EOF>", 12))
    assertSyntaxError("return {", mismatch("", "an identifier, '}'", 8))
    assertSyntaxError("eturn 1", mismatch("eturn", clauseExpected, 0))
    assertSyntaxError("return 1 skip", mismatch("", "an expression", 13))
    assertSyntaxError("return 1 skip *", mismatch("*", "an expression", 14))
    assertSyntaxError("return 1 limit", mismatch("", "an expression", 14))
    assertSyntaxError("return 1 limit *", mismatch("*", "an expression", 15))
    assertSyntaxError("return 1 order by", mismatch("", "an expression", 17))
    assertSyntaxError("return 1 order by *", mismatch("*", "an expression", 18))
    assertSyntaxError("return 1 order by x,", mismatch("", "an expression", 20))
    assertSyntaxError("return 1 order by x,*", mismatch("*", "an expression", 20))

    // CALL

    assertSyntaxError("call hej() yield x as 1y", mismatch("1y", "a variable name", 22))
    assertSyntaxError("call hej() yield 1x as y", extraneous("1x", "'*', an identifier", 17))
    assertSyntaxError("call 1hej()", notViable("", 5)) // Exceptionally unhelpful

    // MISC

    assertSyntaxError("show procedures yield", mismatch("", "'*', a variable name", 21))
    assertSyntaxError("create database 1a", notViable("", 16)) // Exceptionally unhelpful
    assertSyntaxError("with 1 as 1p return *", extraneous("1p", "a variable name", 10))
    assertSyntaxError("with 1 as 1bcd return *", extraneous("1bcd", "a variable name", 10))
  }

  test("syntax errors have stable error messages in ddl") {
    assertSyntaxError(
      "revoke",
      mismatch(
        "",
        "'DENY', 'GRANT', 'IMMUTABLE', 'ALL', 'CREATE', 'ACCESS', 'START', 'STOP', 'INDEX', 'INDEXES', 'CONSTRAINT', 'CONSTRAINTS', 'NAME', 'TRANSACTION', 'TERMINATE', 'ALTER', 'ASSIGN', 'ALIAS', 'COMPOSITE', 'DATABASE', 'PRIVILEGE', 'ROLE', 'SERVER', 'USER', 'EXECUTE', 'RENAME', 'IMPERSONATE', 'DROP', 'LOAD', 'DELETE', 'MERGE', 'TRAVERSE', 'MATCH', 'READ', 'REMOVE', 'SET', 'SHOW', 'WRITE', 'ROLES'",
        6
      )
    )
    assertSyntaxError(
      "revoke deny",
      mismatch(
        "",
        "'IMMUTABLE', 'ALL', 'CREATE', 'ACCESS', 'START', 'STOP', 'INDEX', 'INDEXES', 'CONSTRAINT', 'CONSTRAINTS', 'NAME', 'TRANSACTION', 'TERMINATE', 'ALTER', 'ASSIGN', 'ALIAS', 'COMPOSITE', 'DATABASE', 'PRIVILEGE', 'ROLE', 'SERVER', 'USER', 'EXECUTE', 'RENAME', 'IMPERSONATE', 'DROP', 'LOAD', 'DELETE', 'MERGE', 'TRAVERSE', 'MATCH', 'READ', 'REMOVE', 'SET', 'SHOW', 'WRITE'",
        11
      )
    )

    assertSyntaxError("revoke deny all", notViable("", 15))
    assertSyntaxError("start", mismatch("", "'DATABASE'", 5))
    assertSyntaxError("start database", mismatch("", "an identifier, '$'", 14))
    assertSyntaxError("start database a wai", extraneous("wai", "';', <EOF>", 17))
    assertSyntaxError("stop", mismatch("", "'DATABASE'", 4))
    assertSyntaxError("stop database", mismatch("", "an identifier, '$'", 13))
    assertSyntaxError("stop database a wai", extraneous("wai", "';', <EOF>", 16))
    assertSyntaxError("deallocate", mismatch("", "'DATABASE', 'DATABASES'", 10))
    assertSyntaxError("deallocate database", mismatch("", "'FROM'", 19))
    assertSyntaxError("deallocate database from", mismatch("", "'SERVER', 'SERVERS'", 24))
    assertSyntaxError("deallocate database from server", mismatch("", "a string value, '$'", 31))
    assertSyntaxError("enable server", mismatch("", "a string value, '$'", 13))
    assertSyntaxError("enable server a", mismatch("a", "a string value, '$'", 14))
    assertSyntaxError("enable server 'a' options", mismatch("", "'{', '$'", 25))
    assertSyntaxError("enable server 'a' options {", mismatch("", "an identifier, '}'", 27))
    assertSyntaxError("rename", mismatch("", "'ROLE', 'SERVER', 'USER'", 6))
    assertSyntaxError("alter", mismatch("", "'ALIAS', 'CURRENT', 'DATABASE', 'USER', 'SERVER'", 5))
    assertSyntaxError(
      "drop",
      mismatch("", "'ALIAS', 'CONSTRAINT', 'COMPOSITE', 'DATABASE', 'INDEX', 'ROLE', 'SERVER', 'USER'", 4)
    )
    assertSyntaxError(
      "grant",
      mismatch(
        "",
        "'IMMUTABLE', 'ALL', 'CREATE', 'ACCESS', 'START', 'STOP', 'INDEX', 'INDEXES', 'CONSTRAINT', 'CONSTRAINTS', 'NAME', 'TRANSACTION', 'TERMINATE', 'ALTER', 'ASSIGN', 'ALIAS', 'COMPOSITE', 'DATABASE', 'PRIVILEGE', 'ROLE', 'SERVER', 'USER', 'EXECUTE', 'RENAME', 'IMPERSONATE', 'DROP', 'LOAD', 'DELETE', 'MERGE', 'TRAVERSE', 'MATCH', 'READ', 'REMOVE', 'SET', 'SHOW', 'WRITE', 'ROLES'",
        5
      )
    )
    assertSyntaxError(
      "deny",
      mismatch(
        "",
        "'IMMUTABLE', 'ALL', 'CREATE', 'ACCESS', 'START', 'STOP', 'INDEX', 'INDEXES', 'CONSTRAINT', 'CONSTRAINTS', 'NAME', 'TRANSACTION', 'TERMINATE', 'ALTER', 'ASSIGN', 'ALIAS', 'COMPOSITE', 'DATABASE', 'PRIVILEGE', 'ROLE', 'SERVER', 'USER', 'EXECUTE', 'RENAME', 'IMPERSONATE', 'DROP', 'LOAD', 'DELETE', 'MERGE', 'TRAVERSE', 'MATCH', 'READ', 'REMOVE', 'SET', 'SHOW', 'WRITE'",
        4
      )
    )
  }
}

object SyntaxErrorParserTest {
  sealed trait ParsingFailure

  object ParsingFailure {

    def mismatch(input: String, expected: String, offset: Int): Mismatch = {
      Mismatch(input, expected, InputPosition(offset, 1, offset + 1))
    }

    def extraneous(input: String, expected: String, offset: Int): Extraneous = {
      Extraneous(input, expected, InputPosition(offset, 1, offset + 1))
    }

    def notViable(expected: String, offset: Int): NotViable = {
      NotViable("", expected, InputPosition(offset, 1, offset + 1))
    }

    def missing(missing: String, at: String, offset: Int): Missing = {
      Missing(missing, at, InputPosition(offset, 1, offset + 1))
    }
  }
  case class Mismatch(input: String, expected: String, pos: InputPosition) extends ParsingFailure
  case class Extraneous(input: String, expected: String, pos: InputPosition) extends ParsingFailure
  case class NotViable(input: String, expected: String, pos: InputPosition) extends ParsingFailure
  case class Missing(missing: String, at: String, pos: InputPosition) extends ParsingFailure

  def expectedError(query: String, failure: ParsingFailure): String = {
    failure match {
      case Mismatch(input, expected, pos) =>
        s"""Mismatched input '$input': expected $expected ($pos)
           |"$query"
           | ${" ".repeat(pos.offset)}^""".stripMargin
      case Extraneous(input, expected, pos) =>
        s"""Extraneous input '$input': expected $expected ($pos)
           |"$query"
           | ${" ".repeat(pos.offset)}^""".stripMargin
      case NotViable(input, expected, pos) =>
        val expectedPart = if (expected.nonEmpty) s": expected $expected " else " "
        s"""No viable alternative$expectedPart($pos)
           |"$query"
           | ${" ".repeat(pos.offset)}^""".stripMargin
      case Missing(missing, at, pos) =>
        s"""Missing $missing at $at ($pos)
           |"$query"
           | ${" ".repeat(pos.offset)}^""".stripMargin

    }
  }

  val clauseExpected = Seq(
    "USING",
    "USE",
    "CREATE",
    "DROP",
    "ALTER",
    "RENAME",
    "DENY",
    "REVOKE",
    "GRANT",
    "START",
    "STOP",
    "ENABLE",
    "DRYRUN",
    "DEALLOCATE",
    "REALLOCATE",
    "SHOW",
    "TERMINATE",
    "FINISH",
    "RETURN",
    "INSERT",
    "DETACH",
    "NODETACH",
    "DELETE",
    "SET",
    "REMOVE",
    "OPTIONAL",
    "MATCH",
    "MERGE",
    "WITH",
    "UNWIND",
    "CALL",
    "LOAD",
    "FOREACH"
  ).map(s => s"'$s'").mkString(", ")
}
