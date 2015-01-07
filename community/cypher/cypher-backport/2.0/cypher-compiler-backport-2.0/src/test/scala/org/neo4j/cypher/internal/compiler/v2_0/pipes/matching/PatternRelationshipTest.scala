/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.pipes.matching

import org.scalatest.junit.JUnitSuite
import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Literal

class PatternRelationshipTest extends JUnitSuite with Assertions {

  @Test def should_provide_the_other_node() {
    // given
    val a = new PatternNode("a")
    val b = new PatternNode("b")

    // then r can be constructed
    val r = a.relateTo("r", b, Seq(), Direction.BOTH)

    // such that
    assert(r.getOtherNode(a) === b)
  }

  @Test def should_filter_out_relationships_based_on_properties_and_provide_the_other_node() {
    // given
    val a = new PatternNode("a")
    val b = new PatternNode("b")

    // then r can be constructed
    val r = a.relateTo("r", b, Seq(), Direction.BOTH, Map("prop" -> Literal(42)))

    // such that
    assert(r.getOtherNode(a) === b)
  }

  @Test def should_filter_out_var_length_relationships_based_on_properties_and_provide_the_other_node() {
    // given
    val a = new PatternNode("a")
    val b = new PatternNode("b")

    // then r can be constructed
    val r = a.relateViaVariableLengthPathTo("p", b, Some(1), Some(2), Seq("REL"), Direction.BOTH, Some("r"),  Map("prop" -> Literal(42)))

    // such that
    assert(r.getOtherNode(a) === b)
  }
}
