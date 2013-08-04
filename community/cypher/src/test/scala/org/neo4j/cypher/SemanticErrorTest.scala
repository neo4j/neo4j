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

import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions
import org.hamcrest.CoreMatchers.equalTo
import CypherVersion._

class SemanticErrorTest extends ExecutionEngineHelper with Assertions {
  @Test def returnNodeThatsNotThere() {
    test("start x=node(0) return bar",
      v2_0    -> "Unknown identifier `bar`.",
      vExperimental -> "bar not defined (line 1, column 24)"
    )
  }

  @Test def defineNodeAndTreatItAsARelationship() {
    test("start r=node(0) match a-[r]->b return r",
      v2_0    -> "Some identifiers are used as both relationships and nodes: r",
      vExperimental -> "Type mismatch: r already defined with conflicting type Node (expected Relationship) (line 1, column 26)"
    )
  }

  @Test def redefineSymbolInMatch() {
    test("start a=node(0) match a-[r]->b-->r return r",
      v2_0    -> "Some identifiers are used as both relationships and nodes: r",
      vExperimental -> "Type mismatch: r already defined with conflicting type Relationship (expected Node) (line 1, column 34)"
    )
  }

  @Test def cantUseTYPEOnNodes() {
    test("start r=node(0) return type(r)",
      v2_0    -> "Expected `r` to be a Relationship but it was a Node",
      vExperimental -> "Type mismatch: r already defined with conflicting type Node (expected Relationship) (line 1, column 29)"
    )
  }

  @Test def cantUseLENGTHOnNodes() {
    test("start n=node(0) return length(n)",
      v2_0    -> "Expected `n` to be a Collection<Any> but it was a Node",
      vExperimental -> "Type mismatch: n already defined with conflicting type Node (expected Collection<Any>) (line 1, column 31)"
    )
  }

  @Test def cantReUseRelationshipIdentifier() {
    test("start a=node(0) match a-[r]->b-[r]->a return r",
      v2_0    -> "Can't re-use pattern relationship 'r' with different start/end nodes.",
      vExperimental -> "Can't re-use pattern relationship 'r' with different start/end nodes."
    )
  }

  @Test def shouldKnowNotToCompareStringsAndNumbers() {
    test("start a=node(0) where a.age =~ 13 return a",
      v2_0    -> "Literal(13) expected to be of type String but it is of type Number",
      vExperimental -> "Type mismatch: expected String but was Long (line 1, column 32)"
    )
  }

  @Test def shouldComplainAboutUnknownIdentifier() {
    test("start s = node(1) where s.name = Name and s.age = 10 return s",
      v2_0    -> "Unknown identifier `Name`",
      vExperimental -> "Name not defined (line 1, column 34)"
    )
  }

  @Test def shouldComplainIfShortestPathHasNoRelationship() {
    test("start n=node(0) match p=shortestPath(n) return p",
      vExperimental -> "shortestPath requires a pattern containing a single relationship (line 1, column 25)"
    )
  }

  @Test def shouldComplainIfShortestPathHasMultipleRelationships() {
    test("start a=node(0), b=node(1) match p=shortestPath(a--()--b) return p",
      v2_0 -> "expected single path segment for shortest path",
      vExperimental -> "shortestPath requires a pattern containing a single relationship (line 1, column 36)"
    )
  }

  @Test def shouldComplainIfShortestPathHasAMinimalLength() {
    test("start a=node(0), b=node(1) match p=shortestPath(a-[*1..2]->b) return p",
      v2_0    -> "Shortest path does not support a minimal length",
      vExperimental -> "shortestPath does not support a minimal length (line 1, column 36)"
    )
  }

  @Test def shortestPathNeedsBothEndNodes() {
    test("start a=node(0) match p=shortestPath(a-->b) return p",
      v2_0    -> "Unknown identifier `b`",
      vExperimental -> "Unknown identifier `b`"
    )
  }

  @Test def shouldBeSemanticallyIncorrectToReferToUnknownIdentifierInCreateConstraint() {
    test("create constraint on (foo:Foo) bar.name is unique",
      v2_0    -> "Unknown identifier `bar`, was expecting `foo`"
    )
  }

  @Test def shouldBeSemanticallyIncorrectToReferToUnknownIdentifierInDropConstraint() {
    test("drop constraint on (foo:Foo) bar.name is unique",
      v2_0    -> "Unknown identifier `bar`, was expecting `foo`"
    )
  }

  @Test def shouldFailTypeCheckWhenDeleting() {
    test("start a=node(0) delete 1 + 1",
      v2_0 -> "Expression `Add(Literal(1),Literal(1))` yielded `2`. Don't know how to delete that.",
      vExperimental -> "Type mismatch: expected Node, Relationship or Collection<Map> but was Long (line 1, column 26)"
    )
  }

  private def test(query: String, variants: (CypherVersion, String)*) {
    for ((versions, message) <- variants) {
      test(versions, query, message)
    }
  }

  def test(version: CypherVersion, query: String, message: String) {
    val (qWithVer, versionString) = version match {
      case `vDefault` => (query, "the default parser")
      case _          => (s"cypher ${version.name} " + query, "parser version " + version.name)
    }
    val errorMessage = s"Using ${versionString}: Did not get the expected syntax error, expected: ${message}"

    try {
      val result = parseAndExecute(qWithVer)
      result.toList
      fail(errorMessage)
    } catch {
      case x: CypherException => {
        val actual = x.getMessage.lines.next.trim
        assertThat(errorMessage, actual, equalTo(message))
      }
    }
  }
}
