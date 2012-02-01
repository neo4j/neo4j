/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import org.scalatest.junit.JUnitSuite
import parser.CypherParser
import org.junit.Assert._
import org.junit.{Ignore, Test}

class SyntaxExceptionTest extends JUnitSuite {
  def expectError(query: String, expectedError: String) {
    val parser = new CypherParser()
    try {
      parser.parse(query)
      fail("Should have produced the error: " + expectedError)
    } catch {
      case x: SyntaxException => assertTrue(x.getMessage, x.getMessage.startsWith(expectedError))
    }
  }

  @Test def shouldRaiseErrorWhenSortingOnNode() {
    expectError(
      "start s = node(1) return s order by s",
      "Cannot ORDER BY on nodes or relationships")
  }

  @Test def shouldRaiseErrorWhenMissingIndexValue() {
    expectError(
      "start s = node:index(key=) return s",
      "String literal expected")
  }

  @Test def shouldRaiseErrorWhenMissingIndexKey() {
    expectError(
      "start s = node:index(=\"value\") return s",
      "String literal expected")
  }

  @Test def shouldRaiseErrorWhenMissingReturn() {
    expectError(
      "start s = node(0)",
      "Missing RETURN clause")
  }

  @Test def shouldRaiseErrorWhenFinishingAListWithAComma() {
    expectError(
      "start s = node(1,2,) return s order by s",
      "Last element of list must be a value")
  }

  @Test def shouldComplainAboutNonQuotedStrings() {
    expectError(
      "start s = node(1) where s.name = Name and s.age = 10 return s",
      "Probably missing quotes around a string")
  }

  @Test def shouldWarnAboutMissingStart() {
    expectError(
      "where s.name = Name and s.age = 10 return s",
      "Missing START clause")
  }

  @Test def shouldComplainAboutWholeNumbers() {
    expectError(
      "start s=node(0) return s limit -1",
      "Whole number expected")
  }

  @Test def matchWithoutIdentifierHasToHaveParenthesis() {
    expectError(
      "start a = node(0) match --> a return a",
      "Matching nodes without identifiers have to have parenthesis: ()")
  }


  @Test def matchWithoutIdentifierHasToHaveParenthesis2() {
    expectError(
      "start a = node(0) match (a) --> return a",
      "return is a reserved keyword and may not be used here.")
  }


  @Test def shouldComplainAboutAStringBeingExpected() {
    expectError(
      "start s=node:index(key = value) return s limit -1",
      "String literal expected")
  }

  @Test def shortestPathCanNotHaveMinimumDepth() {
    expectError(
      "start a=node(0), b=node(1) match p=shortestPath(a-[*2..3]->b) return p",
      "Shortest path does not support a minimal length")
  }

  @Test def shortestPathCanNotHaveMultipleLinksInIt() {
    expectError(
      "start a=node(0), b=node(1) match p=shortestPath(a-->()-->b) return p",
      "Shortest path does not support having multiple path segments")
  }

  @Test def oldNodeSyntaxGivesHelpfulError() {
    expectError(
      "start a=(0) return a",
      "The syntax for bound nodes has changed in v1.5 of Neo4j. Now, it is START a=node(<nodeId>), or START a=node:idxName(key='value').")
  }

  @Test def unknownFunction() {
    expectError(
      "start a=node(0) return foo(a)",
      "No function 'foo' exists.")
  }

  @Ignore @Test def nodeParenthesisMustBeClosed() {
    expectError(
      "start s=node(1) match s-->(x return x",
      "Unfinished parenthesis around 'x'")
  }

  @Test def handlesMultilineQueries() {
    val query = """start
    a=node(0),
    b=node(0),
    c=node(0),
    d=node(0),
    e=node(0),
    f=node(0),
    g=node(0),
    s=node:index(key = value) return s"""

    val expected = """String literal expected
"    s=node:index(key = value) return s"
                        ^"""

    try {
      new CypherParser().parse(query)
    } catch {
      case x: SyntaxException => assertEquals(expected, x.getMessage)
    }
  }
}
