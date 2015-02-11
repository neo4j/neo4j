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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.ast.{HasLabels, Identifier, LabelName}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics.{QueryGraphCardinalityInput, CardinalityModel}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.{LogicalPlanningTestSupport, PlannerQuery}
import org.neo4j.graphdb.Direction

class CardinalityCostModelTest extends CypherFunSuite with LogicalPlanningTestSupport {
  object cardinalityModel extends CardinalityModel {
    def apply(lp: LogicalPlan, card: QueryGraphCardinalityInput): Cardinality = lp match {
      case e : Expand => Cardinality(100)
      case _          => Cardinality(10)
    }
  }

  val costModel = CardinalityCostModel(cardinalityModel)

  test("expand should only be counted once") {
    val plan =
      Selection(List(HasLabels(Identifier("a")_, Seq(LabelName("Awesome")_))_),
        Expand(
          Selection(List(HasLabels(Identifier("a")_, Seq(LabelName("Awesome")_))_),
            Expand(
              Argument(Set("a"))(PlannerQuery.empty)(),
                "a",Direction.OUTGOING, Seq.empty, "b", "r1")(PlannerQuery.empty)
          )(PlannerQuery.empty),"a",Direction.OUTGOING, Seq.empty, "b", "r1")(PlannerQuery.empty)
        )(PlannerQuery.empty)


    costModel(plan, QueryGraphCardinalityInput.empty) should equal(Cost(401))
  }
}
