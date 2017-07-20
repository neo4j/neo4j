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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.matching

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.frontend.v3_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.{Relationship, RelationshipType}
import org.neo4j.values.storable.Values.{stringArray, stringValue}
import org.neo4j.values.virtual.VirtualValues.{EMPTY_MAP, edgeValue, nodeValue}

class HistoryTest extends CypherFunSuite {

  val typ = RelationshipType.withName("REL")

  test("excludingPatternRelsWorksAsExpected") {
    val a = new PatternNode("a")
    val b = new PatternNode("b")
    val pr: PatternRelationship = a.relateTo("r", b, Seq(), SemanticDirection.BOTH)
    val r: Relationship = mock[Relationship]
    val mp = MatchingPair(pr, r)
    val history = new InitialHistory(ExecutionContext.empty, Seq.empty).add(mp)

    history.removeSeen(Set[PatternRelationship](pr)) shouldBe empty
  }

  test("should_known_that_it_has_seen_a_relationship") {
    val r = edgeValue(11L, nodeValue(11L, stringArray("f"), EMPTY_MAP), nodeValue(12L, stringArray("f"), EMPTY_MAP), stringValue("T"), EMPTY_MAP)
    val history = new InitialHistory(ExecutionContext.empty, Seq(r))
    history.hasSeen(r) should equal(true)
  }

  test("should_know_that_it_has_not_seen_a_relationship") {
    val r = mock[Relationship]
    val history = new InitialHistory(ExecutionContext.empty, Seq.empty)
    history.hasSeen(r) should equal(false)
  }
}
