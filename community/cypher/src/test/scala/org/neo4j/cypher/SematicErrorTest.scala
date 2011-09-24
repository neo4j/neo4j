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
package org.neo4j.cypher

import commands._
import org.junit.Assert._
import org.junit.Test
import parser.CypherParser

class SematicErrorTest extends ExecutionEngineHelper {
  @Test def returnNodeThatsNotThere() {
    expectedError("start x=(0) return bar",
      """Unknown identifier "bar".""")
  }

  @Test def throwOnDisconnectedPattern() {
    expectedError("start x=(0) match a-[rel]->b return x",
      "All parts of the pattern must either directly or indirectly be connected to at least one bound entity. These identifiers were found to be disconnected: a, b, rel")
  }

  @Test def defineNodeAndTreatItAsARelationship() {
    expectedError("start r=(0) match a-[r]->b return r",
      "Some identifiers are used as both relationships and nodes: r")
  }

  @Test def redefineSymbolInMatch() {
    expectedError("start a=(0) match a-[r]->b-->r return r",
      "Some identifiers are used as both relationships and nodes: r")
  }

  @Test def cantUseTYPEOnNodes() {
    expectedError("start r=(0) return type(r)",
      "Expected r to be a RelationshipIdentifier but it was NodeIdentifier")
  }

  @Test def cantUseLENGTHOnNodes() {
    expectedError("start n=(0) return length(n)",
      "Expected n to be an iterable, but it is not.")
  }

  @Test def shortestPathNeedsBothEndNodes() {
    expectedError("start n=(0) match p=shortestPath(n-->b) return p",
      "Shortest path needs both ends of the path to be provided. Couldn't find b")
  }

  def parse(txt:String):Query = new CypherParser().parse(txt)

  def expectedError(query: String, message: String) { expectedError(parse(query), message) }

  def expectedError(query: Query, message: String) {
    try {
      execute(query).toList
      fail("Did not get the expected syntax error, expected: " + message)
    } catch {
      case x: SyntaxException => assertEquals(message, x.getMessage)
    }
  }
}