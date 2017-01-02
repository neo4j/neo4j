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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.triplet

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v2_2.{RelTypeId, LabelId}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.{SpecifiedButUnknown, Unspecified, SpecifiedAndKnown}
import org.neo4j.graphdb.Direction

class TripletTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("Reverses directed triplets") {
    val triplet = Triplet(
      name = "r",
      left = "a", leftLabels = Set(SpecifiedAndKnown(LabelId(1)), SpecifiedAndKnown(LabelId(2))),
      right = "b", rightLabels = Set(Unspecified()),
      relTypes = Set(SpecifiedAndKnown(RelTypeId(4)), SpecifiedButUnknown()),
      directed = true
    )

    val result = triplet.reverse

    result should equal(Triplet(
      name = triplet.name,
      left = triplet.right, leftLabels = triplet.rightLabels,
      right = triplet.left, rightLabels = triplet.leftLabels,
      relTypes = triplet.relTypes,
      directed = true
    ))
  }

  test("Reverses undirected triplets") {
    val triplet = Triplet(
      name = "r",
      left = "a", leftLabels = Set(SpecifiedAndKnown(LabelId(1)), SpecifiedAndKnown(LabelId(2))),
      right = "b", rightLabels = Set(Unspecified()),
      relTypes = Set(SpecifiedAndKnown(RelTypeId(4)), SpecifiedButUnknown()),
      directed = false
    )

    val result = triplet.reverse

    result should equal(Triplet(
      name = triplet.name,
      left = triplet.right, leftLabels = triplet.rightLabels,
      right = triplet.left, rightLabels = triplet.leftLabels,
      relTypes = triplet.relTypes,
      directed = false
    ))
  }
}
