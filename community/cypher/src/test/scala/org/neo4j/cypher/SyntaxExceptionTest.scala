/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.scalatest.junit.JUnitSuite
import org.junit.Assert._
import org.junit.{Test}

class SyntaxExceptionTest extends JUnitSuite {
  def expectError(query: String, expectedError: String) {
    val parser = new CypherParser()
    try {
      parser.parse(query)
      fail("Should have produced the error: " + expectedError)
    } catch {
      case x: SyntaxException => {
        assertTrue(x.getMessage, x.getMessage.startsWith(expectedError))
      }
    }
  }

  @Test def shouldRaiseErrorWhenMissingIndexValue() {
    expectError(
      "start s = node:index(key=) return s",
      "string literal or parameter expected")
  }

  @Test def shouldGiveNiceErrorWhenMissingEqualsSign() {
    expectError(
      "start n=node:customer(id : {id}) return n",
      "`=` expected")
  }

  @Test def shouldRaiseErrorWhenMissingIndexKey() {
    expectError(
      "start s = node:index(=\"value\") return s",
      "Need index key")
  }

  @Test def startWithoutNodeOrRel() {
    expectError(
      "start s return s",
      "expected identifier assignment")
  }

  @Test def shouldRaiseErrorWhenMissingReturnColumns() {
    expectError(
      "start s = node(0) return",
      "return column list expected")
  }

  @Test def shouldRaiseErrorWhenMissingReturn() {
    expectError(
      "start s = node(0)",
      "Non-mutating queries must return data")
  }

  @Test def shouldWarnAboutMissingStart() {
    expectError(
      "where s.name = Name and s.age = 10 return s",
      "expected START or CREATE")
  }

  @Test def shouldComplainAboutWholeNumbers() {
    expectError(
      "start s=node(0) return s limit -1",
      "expected positive integer or parameter")
  }

  @Test def matchWithoutIdentifierHasToHaveParenthesis() {
    expectError(
      "start a = node(0) match a--b, --> a return a",
      "expected an expression that is a node")
  }

  @Test def matchWithoutIdentifierHasToHaveParenthesis2() {
    expectError(
      "start a = node(0) match (a) -->, a-->b return a",
      "expected an expression that is a node")
  }


  @Test def shouldComplainAboutAStringBeingExpected() {
    expectError(
      "start s=node:index(key = value) return s",
      "string literal or parameter expected")
  }

  @Test def shortestPathCanNotHaveMinimumDepth() {
    expectError(
      "start a=node(0), b=node(1) match p=shortestPath(a-[*2..3]->b) return p",
      "Shortest path does not support a minimal length")
  }

  @Test def shortestPathCanNotHaveMultipleLinksInIt() {
    expectError(
      "start a=node(0), b=node(1) match p=shortestPath(a-->()-->b) return p",
      "expected single path segment")
  }

  @Test def oldNodeSyntaxGivesHelpfulError() {
    expectError(
      "start a=(0) return a",
      "expected either node or relationship here")
  }

  @Test def weirdSpelling() {
    expectError(
      "start a=ndoe(0) return a",
      "expected either node or relationship here")
  }

  @Test def unclosedParenthesis() {
    expectError(
      "start a=node(0 return a",
      "Unclosed parenthesis")
  }

  @Test def trailingComa() {
    expectError(
      "start a=node(0,1,) return a",
      "trailing coma")
  }

  @Test def unclosedCurly() {
    expectError(
      "start a=node({0) return a",
      "Unclosed curly bracket")
  }

  @Test def twoEqualSigns() {
    expectError(
      "start a==node(0) return a",
      "expected either node or relationship here")
  }

  @Test def oldSyntax() {
    expectError(
      "start a=node(0) where all(x in a.prop : x = 'apa') return a",
      "expected where")
  }


  @Test def forgetByInOrderBy() {
    expectError(
      "start a=node(0) return a order a.name",
      "expected by")
  }

  @Test def unknownFunction() {
    expectError(
      "start a=node(0) return foo(a)",
      "unknown function")
  }

  @Test def handlesMultilineQueries() {
    expectError("""start
    a=node(0),
    b=node(0),
    c=node(0),
    d=node(0),
    e=node(0),
    f=node(0),
    g=node(0),
    s=node:index(key = value) return s""",
      "string literal or parameter expected")
  }

  @Test def createNodeWithout() {
    expectError("""start
    a=node(0),
    b=node(0),
    c=node(0),
    d=node(0),
    e=node(0),
    f=node(0),
    g=node(0),
    s=node:index(key = value) return s""",
      "string literal or parameter expected")
  }
}
