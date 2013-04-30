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

class SemanticErrorTest extends ExecutionEngineHelper {
  @Test def returnNodeThatsNotThere() {
    expectedError("start x=node(0) return bar",
      "Unknown identifier `bar`.")
  }

  @Test def defineNodeAndTreatItAsARelationship() {
    expectedError("start r=node(0) match a-[r]->b return r",
      "Some identifiers are used as both relationships and nodes: r")
  }

  @Test def redefineSymbolInMatch() {
    expectedError("start a=node(0) match a-[r]->b-->r return r",
      "Some identifiers are used as both relationships and nodes: r")
  }

  @Test def cantUseTYPEOnNodes() {
    expectedError("start r=node(0) return type(r)",
      "Expected `r` to be a Relationship but it was a Node")
  }

  @Test def cantUseLENGTHOnNodes() {
    expectedError("start n=node(0) return length(n)",
      "Expected `n` to be a Collection<Any> but it was a Node")
  }

  @Test def cantReUseRelationshipIdentifier() {
    expectedError("start a=node(0) match a-[r]->b-[r]->a return r",
      "Can't re-use pattern relationship 'r' with different start/end nodes.")
  }

  @Test def shouldKnowNotToCompareStringsAndNumbers() {
    expectedError("start a=node(0) where a.age =~ 13 return a",
      "Literal(13) expected to be of type String but it is of type Number")
  }

  @Test def shouldComplainAboutUnknownIdentifier() {
    expectedError("start s = node(1) where s.name = Name and s.age = 10 return s",
      "Unknown identifier `Name`")
  }

  @Test def shortestPathNeedsBothEndNodes() {
    expectedError("start n=node(0) match p=shortestPath(n-->b) return p",
      "Unknown identifier `b`")
  }
  
  @Test def shouldBeSemanticallyIncorrectToReferToUnknownIdentifierInCreateConstraint() {
    expectedError("create constraint on (foo:Foo) bar.name is unique",
        "Unknown identifier `bar`, was expecting `foo`")
  }

  @Test def shouldBeSemanticallyIncorrectToReferToUnknownIdentifierInDropConstraint() {
    expectedError("drop constraint on (foo:Foo) bar.name is unique",
        "Unknown identifier `bar`, was expecting `foo`")
  }
  
  def expectedError(query: String, message: String) {
    try {
      val result = parseAndExecute(query)
      result.toList
      fail("Did not get the expected syntax error, expected: %s".format(message))
    } catch {
      case x: CypherException => assertEquals(message, x.getMessage)
    }
  }
}