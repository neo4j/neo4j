/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.ExhaustivePathPattern
import org.neo4j.cypher.internal.ir.NodeBinding
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VariableGrouping
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class NodeConnectionManipulationTest extends CypherFunSuite with NodeConnectionManipulation {

  test(
    "generates increasingly larger variations of a concatenated path pattern by continuously incrementing the various upper bounds in lockstep"
  ) {
    // (a) ((b)-[r]->(c)){1, ?} (d)
    def qpp1(upperBound: UpperBound): QuantifiedPathPattern =
      QuantifiedPathPattern(
        leftBinding = NodeBinding(v"b", v"a"),
        rightBinding = NodeBinding(v"c", v"d"),
        patternRelationships =
          NonEmptyList(PatternRelationship(
            v"r",
            (v"b", v"c"),
            SemanticDirection.OUTGOING,
            Seq.empty,
            SimplePatternLength
          )),
        repetition = Repetition(min = 1, max = upperBound),
        nodeVariableGroupings = Set(VariableGrouping(v"b", v"b"), VariableGrouping(v"c", v"c")),
        relationshipVariableGroupings = Set(VariableGrouping(v"r", v"r"))
      )

    // (d)-[s]-(e)
    val relationship =
      PatternRelationship(
        variable = v"s",
        boundaryNodes = (v"d", v"e"),
        dir = SemanticDirection.BOTH,
        types = Nil,
        length = SimplePatternLength
      )

    // (e) ((f)<-[t]-(g)){0, ?} (h)
    def qpp2(upperBound: UpperBound): QuantifiedPathPattern =
      QuantifiedPathPattern(
        leftBinding = NodeBinding(v"f", v"e"),
        rightBinding = NodeBinding(v"g", v"h"),
        patternRelationships =
          NonEmptyList(PatternRelationship(
            v"t",
            (v"f", v"g"),
            SemanticDirection.INCOMING,
            Seq.empty,
            SimplePatternLength
          )),
        repetition = Repetition(min = 0, max = upperBound),
        nodeVariableGroupings = Set(VariableGrouping(v"f", v"f"), VariableGrouping(v"g", v"g")),
        relationshipVariableGroupings = Set(VariableGrouping(v"t", v"t"))
      )

    // (a) ((b)-[r]->(c))+ (d)-[s]-(e) ((f)<-[t]-(g)){0,2} (h)
    val pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(
      // (a) ((b)-[r]->(c))+ (d)
      qpp1(UpperBound.Unlimited),
      // (d)-[s]-(e)
      relationship,
      // (e) ((f)<-[t]-(g)){0,2} (h)
      qpp2(UpperBound.Limited(2))
    ))

    increasinglyLargerPatterns(pathPattern).toList shouldEqual (1 to 32).map { n =>
      ExhaustivePathPattern.NodeConnections(NonEmptyList(
        // the upper bound for (a) ((b)-[r]->(c))+ (d) goes from 1 to 32
        qpp1(UpperBound.Limited(n)),
        // the relationship (d)-[s]-(e) remains constant
        relationship,
        // the upper bound for (e) ((f)<-[t]-(g)){0,2} (h) skips 0 and is capped at 2
        qpp2(UpperBound.Limited(math.min(n, 2)))
      ))
    }
  }

  test(
    "generates increasingly larger variations of a quantified path pattern by incrementing its upper bound step by step"
  ) {
    def qpp(upperBound: UpperBound): QuantifiedPathPattern =
      QuantifiedPathPattern(
        leftBinding = NodeBinding(v"b", v"a"),
        rightBinding = NodeBinding(v"c", v"d"),
        patternRelationships =
          NonEmptyList(PatternRelationship(
            v"r",
            (v"b", v"c"),
            SemanticDirection.OUTGOING,
            Seq.empty,
            SimplePatternLength
          )),
        repetition = Repetition(min = 0, max = upperBound),
        nodeVariableGroupings = Set(VariableGrouping(v"b", v"b"), VariableGrouping(v"c", v"c")),
        relationshipVariableGroupings = Set(VariableGrouping(v"r", v"r"))
      )

    increasinglyLargerConnection(qpp(UpperBound.Unlimited)).toList shouldEqual (1 to 32).map { n =>
      qpp(UpperBound.Limited(n))
    }
  }

  test("transposing a non-empty list of lists of various sizes, including an infinite one") {
    transpose(NonEmptyList(
      LazyList(1, 2, 3),
      LazyList(0),
      LazyList.from(10)
    )).take(10).toList shouldEqual List(
      NonEmptyList(1, 0, 10),
      NonEmptyList(2, 0, 11),
      NonEmptyList(3, 0, 12),
      NonEmptyList(3, 0, 13),
      NonEmptyList(3, 0, 14),
      NonEmptyList(3, 0, 15),
      NonEmptyList(3, 0, 16),
      NonEmptyList(3, 0, 17),
      NonEmptyList(3, 0, 18),
      NonEmptyList(3, 0, 19)
    )
  }
}
