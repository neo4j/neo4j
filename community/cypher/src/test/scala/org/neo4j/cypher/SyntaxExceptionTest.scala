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
import org.junit.Test
import org.scalatest.Assertions
import org.hamcrest.CoreMatchers.equalTo
import CypherVersion._

class SyntaxExceptionTest extends JUnitSuite with Assertions {
  @Test def shouldRaiseErrorWhenMissingIndexValue() {
    test("start s = node:index(key=) return s",
      v2_0 -> "string literal or parameter expected"
    )
  }

  @Test def shouldGiveNiceErrorWhenMissingEqualsSign() {
    test("start n=node:customer(id : {id}) return n",
      v2_0 -> "`=` expected"
    )
  }

  @Test def shouldRaiseErrorWhenMissingIndexKey() {
    test("start s = node:index(=\"value\") return s",
      v2_0 -> "Need index key"
    )
  }

  @Test def startWithoutNodeOrRel() {
    test("start s return s",
      v2_0 -> "expected identifier assignment"
    )
  }

  @Test def shouldRaiseErrorWhenMissingReturnColumns() {
    test("start s = node(0) return",
      v2_0 -> "return column list expected"
    )
  }

  @Test def shouldRaiseErrorWhenMissingReturn() {
    test("start s = node(0)",
      v2_0 -> "expected return clause"
    )
  }

  @Test def shouldWarnAboutMissingStart() {
    test("where s.name = Name and s.age = 10 return s",
      v2_0 -> "invalid start of query"
    )
  }

  @Test def shouldComplainAboutWholeNumbers() {
    test("start s=node(0) return s limit -1",
      v2_0 -> "expected positive integer or parameter"
    )
  }

  @Test def matchWithoutIdentifierHasToHaveParenthesis() {
    test("start a = node(0) match a--b, --> a return a",
      v2_0 -> "expected an expression that is a node"
    )
  }

  @Test def matchWithoutIdentifierHasToHaveParenthesis2() {
    test("start a = node(0) match (a) -->, a-->b return a",
      v2_0 -> "expected an expression that is a node"
    )
  }


  @Test def shouldComplainAboutAStringBeingExpected() {
    test("start s=node:index(key = value) return s",
      v2_0 -> "string literal or parameter expected"
    )
  }

  @Test def shortestPathCanNotHaveMinimumDepth() {
    test("start a=node(0), b=node(1) match p=shortestPath(a-[*2..3]->b) return p",
      v2_0 -> "Shortest path does not support a minimal length"
    )
  }

  @Test def shortestPathCanNotHaveMultipleLinksInIt() {
    test("start a=node(0), b=node(1) match p=shortestPath(a-->()-->b) return p",
      v2_0 -> "expected single path segment for shortest path"
    )
  }

  @Test def oldNodeSyntaxGivesHelpfulError() {
    test("start a=(0) return a",
      v2_0 -> "expected either node or relationship here"
    )
  }

  @Test def weirdSpelling() {
    test("start a=ndoe(0) return a",
      v2_0 -> "expected either node or relationship here"
    )
  }

  @Test def unclosedParenthesis() {
    test("start a=node(0 return a",
      v2_0 -> "Unclosed parenthesis"
    )
  }

  @Test def trailingComa() {
    test("start a=node(0,1,) return a",
      v2_0 -> "trailing coma"
    )
  }

  @Test def unclosedCurly() {
    test("start a=node({0) return a",
      v2_0 -> "Unclosed curly brackets"
    )
  }

  @Test def twoEqualSigns() {
    test("start a==node(0) return a",
      v2_0 -> "expected either node or relationship here"
    )
  }

  @Test def oldSyntax() {
    test("start a=node(0) where all(x in a.prop : x = 'apa') return a",
      v2_0 -> "expected where"
    )
  }


  @Test def forgetByInOrderBy() {
    test("start a=node(0) return a order a.name",
      v2_0 -> "expected by"
    )
  }

  @Test def unknownFunction() {
    test("start a=node(0) return foo(a)",
      v2_0 -> "unknown function"
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
      v2_0 -> "string literal or parameter expected"
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
      v2_0 -> "string literal or parameter expected"
    )
  }

  private def test(query: String, variants: (CypherVersion, String)*) {
    for ((versions, message) <- variants) {
      test(versions, query, message)
    }
  }

  def test(version: CypherVersion, query: String, message: String) {
    val (qWithVer, versionString) = version match {
      case `v2_0` => (query, "the default parser")
      case _      => (s"cypher ${version.name} " + query, "parser version " + version.name)
    }
    val errorMessage = s"Using ${versionString}: Did not get the expected syntax error, expected: ${message}"

    val parser = new CypherParser()
    try {
      val result = parser.parse(qWithVer)
      fail(errorMessage)
    } catch {
      case x: CypherException => {
        val actual = x.getMessage.lines.next.trim
        assertThat(errorMessage, actual, equalTo(message))
      }
    }
  }
}
