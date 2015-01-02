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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.triplet

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.{SpecifiedButUnknown, Unspecified, SpecifiedAndKnown}
import org.neo4j.cypher.internal.compiler.v2_2.{RelTypeId, LabelId}
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Cardinality
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics.QueryGraphCardinalityInput
import org.neo4j.cypher.internal.compiler.v2_2.planner._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{IdName, SimplePatternLength, PatternRelationship}
import org.neo4j.graphdb.Direction

class TripletConverterTest extends CypherFunSuite with LogicalPlanningTestSupport2 with AstConstructionTestSupport {

  test("Converts PatternRelationship to triplet and resolves all specs") {

    val patRel = PatternRelationship(
      name = "r",
      nodes = ("a", "b"),
      dir = Direction.OUTGOING,
      types = Seq(RelTypeName("KNOWS")_, RelTypeName("ZAPS")_),
      length = SimplePatternLength
    )

    val qg = QueryGraph
      .empty
      .addPatternNodes("a", "b")
      .addPatternRel(patRel)
      .addSelections(Selections(Set(Predicate(Set("a"), HasLabels(ident("a"), Seq(LabelName("Animal")_))_))))

    val input = QueryGraphCardinalityInput(Map(IdName("a") -> Set(LabelName("Person")_)), Cardinality.SINGLE)

    val table = SemanticTable(ASTAnnotationMap.empty)
    table.resolvedLabelIds.put("Person", LabelId(1))
    table.resolvedLabelIds.put("Animal", LabelId(2))
    table.resolvedRelTypeNames.put("KNOWS", RelTypeId(4))

    val converter = TripletConverter(qg, input, table)

    val triplet = converter(patRel)

    triplet should equal(
      Triplet(
        name = "r",
        left = "a", leftLabels = Set(SpecifiedAndKnown(LabelId(1)), SpecifiedAndKnown(LabelId(2))),
        right = "b", rightLabels = Set(Unspecified()),
        relTypes = Set(SpecifiedAndKnown(RelTypeId(4)), SpecifiedButUnknown()),
        directed = true
      ))
  }
}
