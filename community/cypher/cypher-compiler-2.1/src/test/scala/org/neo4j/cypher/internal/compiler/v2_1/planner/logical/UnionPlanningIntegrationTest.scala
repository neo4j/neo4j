/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._

class UnionPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("MATCH (a:A) RETURN a UNION ALL MATCH (a:B) RETURN a") {

    implicit val (logicalPlan, semanticTable) = new given {
      knownLabels = Set("A", "B")
    } getLogicalPlanFor "MATCH (a:A) RETURN a UNION ALL MATCH (a:B) RETURN a"

    logicalPlan should equal(
      Union(
        NodeByLabelScan("a", Right(semanticTable.resolvedLabelIds("A"))),
        NodeByLabelScan("a", Right(semanticTable.resolvedLabelIds("B"))))
    )
  }
  test("MATCH (a:A) RETURN a UNION MATCH (a:B) RETURN a") {

    implicit val (logicalPlan, semanticTable) = new given {
      knownLabels = Set("A", "B")
    } getLogicalPlanFor "MATCH (a:A) RETURN a UNION MATCH (a:B) RETURN a"

    logicalPlan should equal(
      Aggregation(
        left = Union(
          NodeByLabelScan("a", Right(semanticTable.resolvedLabelIds("A"))),
          NodeByLabelScan("a", Right(semanticTable.resolvedLabelIds("B")))),
        groupingExpressions = Map("a" -> ident("a")),
        aggregationExpression = Map.empty
      )
    )
  }
}
