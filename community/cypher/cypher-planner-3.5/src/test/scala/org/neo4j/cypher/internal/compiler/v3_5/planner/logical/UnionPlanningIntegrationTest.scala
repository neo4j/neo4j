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
import org.opencypher.v9_0.expressions.Variable
import org.neo4j.cypher.internal.v3_5.logical.plans.{Distinct, NodeByLabelScan, Projection, Union}

class UnionPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("MATCH (a:A) RETURN a AS a UNION ALL MATCH (a:B) RETURN a AS a") {

    val setup = new given {
      knownLabels = Set("A", "B")
    }
    val (_, logicalPlan, _ , _, _) = setup.getLogicalPlanFor("MATCH (a:A) RETURN a AS a UNION ALL MATCH (a:B) RETURN a AS a")

    logicalPlan should equal(
      Union(
        Projection(
          NodeByLabelScan("  a@7", lblName("A"), Set.empty),
          Map("a" -> Variable("  a@7") _)
        ),
        Projection(
          NodeByLabelScan("  a@43", lblName("B"), Set.empty),
          Map("a" -> Variable("  a@43") _)
        )
      )
    )
  }

  test("MATCH (a:A) RETURN a AS a UNION MATCH (a:B) RETURN a AS a") {

    val setup = new given {
      knownLabels = Set("A", "B")
    }
    val (_, logicalPlan, _, _, _) = setup.getLogicalPlanFor("MATCH (a:A) RETURN a AS a UNION MATCH (a:B) RETURN a AS a")

    logicalPlan should equal(
      Distinct(
        source = Union(
          Projection(
            NodeByLabelScan("  a@7", lblName("A"), Set.empty),
            Map("a" -> Variable("  a@7") _)
          ),
          Projection(
            NodeByLabelScan("  a@39", lblName("B"), Set.empty),
            Map("a" -> Variable("  a@39") _)
          )
        ),
        groupingExpressions = Map("a" -> varFor("a"))
      )
    )
  }
}
