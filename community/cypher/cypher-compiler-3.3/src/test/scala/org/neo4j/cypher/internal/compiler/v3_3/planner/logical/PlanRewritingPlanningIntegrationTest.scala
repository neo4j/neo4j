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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical

import org.neo4j.cypher.internal.compiler.v3_3.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.Projection
import org.neo4j.cypher.internal.frontend.v3_3.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.frontend.v3_3.ast.GetDegree
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite

class PlanRewritingPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should use GetDegree to compute the degree of a node") {
    val result = (new given {} getLogicalPlanFor "MATCH (n) RETURN length((n)-->()) AS deg")._2

    result should equal(
      Projection(
        AllNodesScan("n", Set.empty)(solved),
        Map("deg" -> GetDegree(varFor("n"), None, OUTGOING) _)
      )(result.solved)
    )
  }
}
