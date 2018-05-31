/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical

import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanningTestSupport2
import org.opencypher.v9_0.expressions.GetDegree
import org.opencypher.v9_0.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.v3_5.logical.plans.{AllNodesScan, Projection}

class PlanRewritingPlanningIntegrationTest  extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should use GetDegree to compute the degree of a node") {
    val result = (new given {
    } getLogicalPlanFor  "MATCH (n) RETURN length((n)-->()) AS deg")._2

    result should equal(
      Projection(
        AllNodesScan("n", Set.empty),
        Map("deg" -> GetDegree(varFor("n"), None, OUTGOING)_)
      )
    )
  }
}
