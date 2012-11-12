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
package org.neo4j.cypher.internal.pipes.matching

import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.GraphDatabaseTestBase
import org.neo4j.cypher.internal.commands.True

class PatternNodeTest extends GraphDatabaseTestBase with Assertions {
  @Test def returnsPatternRelationships() {
    val a = new PatternNode("a")
    val b = new PatternNode("b")

    val r = a.relateTo("r", b, Seq(), Direction.BOTH, false, True())

    val rels = a.getPRels(Seq())

    assert(rels === Seq(r))
  }

  @Test def doesntReturnRelsAlreadyVisited() {
    val a = createNode()
    val b = createNode()
    val rel = relate(a, b, "r")

    val pA = new PatternNode("a")
    val pB = new PatternNode("b")

    val pRel = pA.relateTo("r", pB, Seq(), Direction.BOTH, false, True())

    val rels = pA.getPRels(Seq(MatchingPair(pRel, rel)))

    assert(rels === Seq())
  }
}