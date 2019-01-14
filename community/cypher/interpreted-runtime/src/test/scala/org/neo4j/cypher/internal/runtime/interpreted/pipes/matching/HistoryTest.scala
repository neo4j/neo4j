/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes.matching

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CommunityExecutionContextFactory
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection
import org.neo4j.graphdb.{Relationship, RelationshipType}
import org.neo4j.values.storable.Values.{stringArray, stringValue}
import org.neo4j.values.virtual.VirtualValues.{EMPTY_MAP, relationshipValue, nodeValue}

class HistoryTest extends CypherFunSuite {

  val typ = RelationshipType.withName("REL")

  test("excludingPatternRelsWorksAsExpected") {
    val a = new PatternNode("a")
    val b = new PatternNode("b")
    val pr: PatternRelationship = a.relateTo("r", b, Seq(), SemanticDirection.BOTH)
    val r: Relationship = mock[Relationship]
    val mp = MatchingPair(pr, r)
    val history = new InitialHistory(ExecutionContext.empty, Seq.empty, CommunityExecutionContextFactory()).add(mp)

    history.removeSeen(Set[PatternRelationship](pr)) shouldBe empty
  }

  test("should_known_that_it_has_seen_a_relationship") {
    val r = relationshipValue(11L, nodeValue(11L, stringArray("f"), EMPTY_MAP), nodeValue(12L, stringArray("f"), EMPTY_MAP), stringValue("T"), EMPTY_MAP)
    val history = new InitialHistory(ExecutionContext.empty, Seq(r), CommunityExecutionContextFactory())
    history.hasSeen(r) should equal(true)
  }

  test("should_know_that_it_has_not_seen_a_relationship") {
    val r = mock[Relationship]
    val history = new InitialHistory(ExecutionContext.empty, Seq.empty, CommunityExecutionContextFactory())
    history.hasSeen(r) should equal(false)
  }
}
