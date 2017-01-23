/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher

import scala.util.matching.Regex

class SyntaxExceptionTest extends ExecutionEngineFunSuite {
  test("should raise error when missing index value") {
    test(
      "start s = node:index(key=) return s",
      "Invalid input ')': expected whitespace, \"...string...\" or a parameter (line 1, column 26)"
    )
  }

  test("should give nice error when missing equals sign") {
    test(
      "start n=node:customer(id : {id}) return n",
      "Invalid input ':': expected whitespace, comment or '=' (line 1, column 26)"
    )
  }

  test("should raise error when missing index key") {
    test(
      "start s = node:index(=\"value\") return s",
      "Invalid input '=': expected whitespace, an identifier, \"...string...\" or a parameter (line 1, column 22)"
    )
  }

  test("start without node or rel") {
    test(
      "start s return s",
      "Invalid input 'r': expected whitespace, comment or '=' (line 1, column 9)"
    )
  }

  test("should raise error when missing return columns") {
    test(
      "match (s) where id(s) = 0 return",
      "Unexpected end of input: expected whitespace, DISTINCT, '*' or an expression (line 1, column 33)"
    )
  }

  test("should raise error when missing return") {
    test(
      "match (s) where id(s) = 0",
      "Query cannot conclude with MATCH (must be RETURN or an update clause) (line 1, column 1)"
    )
  }

  test("should complain about whole numbers") {
    test(
      "match (s) where id(s) = 0 return s limit -1",
      "Invalid input '-1' is not a valid value, must be a positive integer (line 1, column 42 (offset: 41))"
    )
  }

  test("match without variable has to have parenthesis") {
    test(
      "match (a) where id(a) = 0 match a--b, --> a return a",
      "Invalid input '-': expected whitespace, comment or a pattern (line 1, column 39)"
    )
  }

  test("matchWithoutVariableHasToHaveParenthesis2") {
    test(
      "match (a) where id(a) = 0 match (a) -->, a-->b return a",
      "Invalid input ',': expected whitespace or a node pattern (line 1, column 40)"
    )
  }

  test("should complain about a string being expected") {
    test(
      "start s=node:index(key = value) return s",
      "Invalid input 'v': expected whitespace, comment, \"...string...\" or a parameter (line 1, column 26)"
    )
  }

  test("shortest path can not have minimum depth different from zero or one") {
    test(
      "match (a), (b) where id(a) = 0 and id(b) = 1 match p=shortestPath(a-[*2..3]->b) return p",
      "shortestPath(...) does not support a minimal length different from 0 or 1 (line 1, column 54)"
    )
  }

  test("shortest path can not have multiple links in it") {
    test(
      "match (a), (b) where id(a) = 0 and id(b) = 1 match p=shortestPath(a-->()-->b) return p",
      "shortestPath(...) requires a pattern containing a single relationship (line 1, column 54)"
    )
  }

  test("old node syntax gives helpful error") {
    test(
      "start a=(0) return a",
      "Invalid input '(': expected whitespace, NODE or RELATIONSHIP (line 1, column 9)"
    )
  }

  test("weird spelling") {
    test(
      "start a=ndoe(0) return a",
      "Invalid input 'd': expected 'o/O' (line 1, column 10)"
    )
  }

  test("unclosed parenthesis") {
    test(
      "start a=node(0 return a",
      "Invalid input 'r': expected whitespace, comment, ',' or ')' (line 1, column 16)"
    )
  }

  test("trailing comma") {
    test(
      "start a=node(0,1,) return a",
      "Invalid input ')': expected whitespace or an unsigned integer (line 1, column 18)"
    )
  }

  test("unclosed curly") {
    test(
      "start a=node({0) return a",
      "Invalid input ')': expected whitespace or '}' (line 1, column 16)"
    )
  }

  test("two equal signs") {
    test(
      "start a==node(0) return a",
      "Invalid input '=' (line 1, column 9)"
    )
  }

  test("forget by in order by") {
    test(
      "match (a) where id(a) = 0 return a order a.name",
      "Invalid input 'a': expected whitespace, comment or BY (line 1, column 42)"
    )
  }

  test("using random function in aggregate") {
    test(
      "match (a) where id(a) = 0 return count(rand())",
      "Can't use non-deterministic (random) functions inside of aggregate functions."
    )
  }

  test("unknown function") {
    test(
      "match (a) where id(a) = 0 return foo(a)",
      "Unknown function 'foo' (line 1, column 34)"
    )
  }

  test("handles multiline queries") {
    test(
      """start
         a=node(0),
         b=node(0),
         c=node(0),
         d=node(0),
         e=node(0),
         f=node(0),
         g=node(0),
         s=node:index(key = value) return s""",
      "Invalid input 'v': expected whitespace, comment, \"...string...\" or a parameter (line 9, column 29)"
    )
  }

  test("create node without") {
    test(
      """start
         a=node(0),
         b=node(0),
         c=node(0),
         d=node(0),
         e=node(0),
         f=node(0),
         g=node(0),
         s=node:index(key = value) return s""",
      "Invalid input 'v': expected whitespace, comment, \"...string...\" or a parameter (line 9, column 29)"
    )
  }

  test("should raise error for invalid hexliteral") {
    test(
      "return 0x23G34",
      "invalid literal number (line 1, column 8)"
    )
    test(
      "return 0x23j",
      "invalid literal number (line 1, column 8)"
    )
  }

  test("should handle empty string") {
    test(" ", "Unexpected end of input: expected whitespace, comment, CYPHER options, EXPLAIN, PROFILE or Query (line 1, column 2 (offset: 1))")
  }

  test("should handle newline") {
    val sep = System.lineSeparator()
    test(sep, s"Unexpected end of input: expected whitespace, comment, CYPHER options, EXPLAIN, PROFILE or Query (line 2, column 1 (offset: ${sep.length}))")
  }

  def test(query: String, message: String) {
    try {
      execute(query)
      fail(s"Did not get the expected syntax error, expected: $message")
    } catch {
      case x: CypherException => {
        val actual = x.getMessage.lines.next.trim
        actual should startWith(message.init)
      }
    }
  }

  def test(query: String, messageRegex: Regex) {
    try {
      execute(query)
      fail(s"Did not get the expected syntax error, expected matching: '$messageRegex'")
    } catch {
      case x: CypherException =>
        val actual = x.getMessage.lines.next().trim
        messageRegex findFirstIn actual match {
          case None => fail(s"Expected matching '$messageRegex', but was '$actual'")
          case Some(_) => ()
        }
    }
  }
}
