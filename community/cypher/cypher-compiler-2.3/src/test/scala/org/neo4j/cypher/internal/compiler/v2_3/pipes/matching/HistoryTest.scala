/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.pipes.matching

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.{DynamicRelationshipType, Relationship}

class HistoryTest extends CypherFunSuite {

  val typ = DynamicRelationshipType.withName("REL")

  test("excludingPatternRelsWorksAsExpected") {
    val a = new PatternNode("a")
    val b = new PatternNode("b")
    val pr: PatternRelationship = a.relateTo("r", b, Seq(), SemanticDirection.BOTH)
    val r: Relationship = mock[Relationship]
    val mp = new MatchingPair(pr, r)
    val history = new InitialHistory(ExecutionContext.empty, Seq.empty).add(mp)

    history.removeSeen(Set[PatternRelationship](pr)) shouldBe empty
  }

  test("should_known_that_it_has_seen_a_relationship") {
    val r = mock[Relationship]
    val history = new InitialHistory(ExecutionContext.empty, Seq(r))
    history.hasSeen(r) should equal(true)
  }

  test("should_know_that_it_has_not_seen_a_relationship") {
    val r = mock[Relationship]
    val history = new InitialHistory(ExecutionContext.empty, Seq.empty)
    history.hasSeen(r) should equal(false)
  }
}
