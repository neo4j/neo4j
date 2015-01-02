/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0

import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions
import org.hamcrest.CoreMatchers.equalTo
import org.neo4j.cypher.{ExecutionEngineJUnitSuite, ExecutionEngineTestSupport, CypherException}
import scala.util.matching.Regex

class SyntaxExceptionTest extends ExecutionEngineJUnitSuite {
  @Test def shouldRaiseErrorWhenMissingIndexValue() {
    test(
      "start s = node:index(key=) return s",
      "Invalid input ')': expected whitespace, \"...string...\" or a parameter (line 1, column 26)"
    )
  }

  @Test def shouldGiveNiceErrorWhenMissingEqualsSign() {
    test(
      "start n=node:customer(id : {id}) return n",
      "Invalid input ':': expected whitespace, comment or '=' (line 1, column 26)"
    )
  }

  @Test def shouldRaiseErrorWhenMissingIndexKey() {
    test(
      "start s = node:index(=\"value\") return s",
      "Invalid input '=': expected whitespace, an identifier, \"...string...\" or a parameter (line 1, column 22)"
    )
  }

  @Test def startWithoutNodeOrRel() {
    test(
      "start s return s",
      "Invalid input 'r': expected whitespace, comment or '=' (line 1, column 9)"
    )
  }

  @Test def shouldRaiseErrorWhenMissingReturnColumns() {
    test(
      "start s = node(0) return",
      "Unexpected end of input: expected whitespace, DISTINCT, '*' or an expression (line 1, column 25)"
    )
  }

  @Test def shouldRaiseErrorWhenMissingReturn() {
    test(
      "start s = node(0)",
      "Query cannot conclude with START (must be RETURN or an update clause) (line 1, column 1)"
    )
  }

  @Test def shouldComplainAboutWholeNumbers() {
    test(
      "start s=node(0) return s limit -1",
      "Invalid input '-': expected whitespace, comment, an unsigned integer or a parameter (line 1, column 32)"
    )
  }

  @Test def matchWithoutIdentifierHasToHaveParenthesis() {
    test(
      "start a = node(0) match a--b, --> a return a",
      "Invalid input '-': expected whitespace, comment or a pattern (line 1, column 31)"
    )
  }

  @Test def matchWithoutIdentifierHasToHaveParenthesis2() {
    test(
      "start a = node(0) match (a) -->, a-->b return a",
      "Invalid input ',': expected whitespace or a node pattern (line 1, column 32)"
    )
  }

  @Test def shouldComplainAboutAStringBeingExpected() {
    test(
      "start s=node:index(key = value) return s",
      "Invalid input 'v': expected whitespace, comment, \"...string...\" or a parameter (line 1, column 26)"
    )
  }

  @Test def shortestPathCanNotHaveMinimumDepth() {
    test(
      "start a=node(0), b=node(1) match p=shortestPath(a-[*2..3]->b) return p",
      "shortestPath(...) does not support a minimal length (line 1, column 36)"
    )
  }

  @Test def shortestPathCanNotHaveMultipleLinksInIt() {
    test(
      "start a=node(0), b=node(1) match p=shortestPath(a-->()-->b) return p",
      "shortestPath(...) requires a pattern containing a single relationship (line 1, column 36)"
    )
  }

  @Test def oldNodeSyntaxGivesHelpfulError() {
    test(
      "start a=(0) return a",
      "Invalid input '(': expected whitespace, NODE or RELATIONSHIP (line 1, column 9)"
    )
  }

  @Test def weirdSpelling() {
    test(
      "start a=ndoe(0) return a",
      "Invalid input 'd': expected 'o/O' (line 1, column 10)"
    )
  }

  @Test def unclosedParenthesis() {
    test(
      "start a=node(0 return a",
      "Invalid input 'r': expected whitespace, comment, ',' or ')' (line 1, column 16)"
    )
  }

  @Test def trailingComa() {
    test(
      "start a=node(0,1,) return a",
      "Invalid input ')': expected whitespace or an unsigned integer (line 1, column 18)"
    )
  }

  @Test def unclosedCurly() {
    test(
      "start a=node({0) return a",
      "Invalid input ')': expected whitespace or '}' (line 1, column 16)"
    )
  }

  @Test def twoEqualSigns() {
    test(
      "start a==node(0) return a",
      "Invalid input '=' (line 1, column 9)"
    )
  }

  @Test def forgetByInOrderBy() {
    test(
      "start a=node(0) return a order a.name",
      "Invalid input 'a': expected whitespace, comment or BY (line 1, column 32)"
    )
  }

  @Test def unknownFunction() {
    test(
      "start a=node(0) return foo(a)",
      "Unknown function 'foo' (line 1, column 24)"
    )
  }

  @Test def usingRandomFunctionInAggregate() {
    test(
      "start a=node(0) return count(rand())",
      "Can't use non-deterministic (random) functions inside of aggregate functions."
    )
  }

  @Test def handlesMultiLineQueries() {
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

  @Test def createNodeWithout() {
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

  @Test def shouldRaiseErrorForInvalidHexLiteral() {
    test(
      "return 0x23G34",
      "invalid literal number (line 1, column 8)"
    )
    test(
      "return 0x23j",
      "invalid literal number (line 1, column 8)"
    )
  }

  def test(query: String, message: String) {
    try {
      execute(query)
      fail(s"Did not get the expected syntax error, expected: $message")
    } catch {
      case x: CypherException =>
        val actual = x.getMessage.lines.next().trim
        assertThat(actual, equalTo(message))
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
