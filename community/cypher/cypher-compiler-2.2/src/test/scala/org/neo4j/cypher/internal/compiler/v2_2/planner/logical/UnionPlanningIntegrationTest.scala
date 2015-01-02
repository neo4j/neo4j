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
import org.neo4j.cypher.internal.compiler.v2_2.ast.{LabelName, Identifier}
import org.neo4j.cypher.internal.compiler.v2_2.pipes.LazyLabel
import org.neo4j.cypher.internal.compiler.v2_2.planner.{PlannerQuery, LogicalPlanningTestSupport2}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._

class UnionPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("MATCH (a:A) RETURN a AS a UNION ALL MATCH (a:B) RETURN a AS a") {

    implicit val (logicalPlan, semanticTable) = new given {
      knownLabels = Set("A", "B")
    } getLogicalPlanFor "MATCH (a:A) RETURN a AS a UNION ALL MATCH (a:B) RETURN a AS a"

    logicalPlan should equal(
      Union(
        Projection(
          NodeByLabelScan("  a@7", LazyLabel(LabelName("A")_), Set.empty)(PlannerQuery.empty),
          Map("a" -> Identifier("  a@7")_)
        )(PlannerQuery.empty),
        Projection(
          NodeByLabelScan("  a@43", LazyLabel(LabelName("B")_), Set.empty)(PlannerQuery.empty),
          Map("a" -> Identifier("  a@43")_)
        )(PlannerQuery.empty)
      )(PlannerQuery.empty)
    )
  }
  test("MATCH (a:A) RETURN a AS a UNION MATCH (a:B) RETURN a AS a") {

    implicit val (logicalPlan, semanticTable) = new given {
      knownLabels = Set("A", "B")
    } getLogicalPlanFor "MATCH (a:A) RETURN a AS a UNION MATCH (a:B) RETURN a AS a"

    logicalPlan should equal(
      Aggregation(
        left = Union(
          Projection(
            NodeByLabelScan("  a@7", LazyLabel(LabelName("A")_), Set.empty)(PlannerQuery.empty),
            Map("a" -> Identifier("  a@7")_)
          )(PlannerQuery.empty),
          Projection(
            NodeByLabelScan("  a@39", LazyLabel(LabelName("B")_), Set.empty)(PlannerQuery.empty),
            Map("a" -> Identifier("  a@39")_)
          )(PlannerQuery.empty)
        )(PlannerQuery.empty),
        groupingExpressions = Map("a" -> ident("a")),
        aggregationExpression = Map.empty
      )(PlannerQuery.empty)
    )
  }
}
