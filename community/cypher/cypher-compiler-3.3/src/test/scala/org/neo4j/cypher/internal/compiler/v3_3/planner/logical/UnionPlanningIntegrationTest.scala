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
import org.neo4j.cypher.internal.frontend.v3_4.ast.Variable
import org.neo4j.cypher.internal.v3_4.logical.plans.{Distinct, NodeByLabelScan, Projection, Union}

class UnionPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("MATCH (a:A) RETURN a AS a UNION ALL MATCH (a:B) RETURN a AS a") {

    val setup = new given {
      knownLabels = Set("A", "B")
    }
    implicit val (_, logicalPlan, _) = setup.getLogicalPlanFor("MATCH (a:A) RETURN a AS a UNION ALL MATCH (a:B) RETURN a AS a")

    logicalPlan should equal(
      Union(
        Projection(
          NodeByLabelScan("  a@7", lblName("A"), Set.empty)(solved),
          Map("a" -> Variable("  a@7") _)
        )(solved),
        Projection(
          NodeByLabelScan("  a@43", lblName("B"), Set.empty)(solved),
          Map("a" -> Variable("  a@43") _)
        )(solved)
      )(solved)
    )
  }

  test("MATCH (a:A) RETURN a AS a UNION MATCH (a:B) RETURN a AS a") {

    val setup = new given {
      knownLabels = Set("A", "B")
    }
    implicit val (_, logicalPlan, _) = setup.getLogicalPlanFor("MATCH (a:A) RETURN a AS a UNION MATCH (a:B) RETURN a AS a")

    logicalPlan should equal(
      Distinct(
        left = Union(
          Projection(
            NodeByLabelScan("  a@7", lblName("A"), Set.empty)(solved),
            Map("a" -> Variable("  a@7") _)
          )(solved),
          Projection(
            NodeByLabelScan("  a@39", lblName("B"), Set.empty)(solved),
            Map("a" -> Variable("  a@39") _)
          )(solved)
        )(solved),
        groupingExpressions = Map("a" -> varFor("a"))
      )(solved)
    )
  }
}
