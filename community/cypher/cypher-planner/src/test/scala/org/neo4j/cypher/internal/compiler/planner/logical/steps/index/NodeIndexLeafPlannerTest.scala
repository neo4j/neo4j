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
package org.neo4j.cypher.internal.compiler.planner.logical.steps.index

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class NodeIndexLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport2 with AstConstructionTestSupport {

  test("testFindIndexCompatiblePredicates on hasLabel with label with constraint") {

    new given {
      qg = QueryGraph()
      nodeConstraints = Set(("A", Set("prop1")))
    } withLogicalPlanningContext { (_, context) =>
      val compatiblePredicates = NodeIndexLeafPlanner.findIndexCompatiblePredicates(
        Set(hasLabels("n", "A")),
        Set.empty,
        context.semanticTable,
        context.staticComponents.planContext,
        context.plannerState.indexCompatiblePredicatesProviderContext
      )
      compatiblePredicates.size shouldBe 1
      val predicate = isNotNull(prop("n", "prop1"))
      compatiblePredicates.foreach { compatiblePredicate =>
        compatiblePredicate.predicate should be(predicate)
      }
    }
  }

  test("testFindIndexCompatiblePredicates on hasLabel with label without constraint") {

    new given {
      qg = QueryGraph()
    } withLogicalPlanningContext { (_, context) =>
      val compatiblePredicates = NodeIndexLeafPlanner.findIndexCompatiblePredicates(
        Set(hasLabels("n", "A")),
        Set.empty,
        context.semanticTable,
        context.staticComponents.planContext,
        context.plannerState.indexCompatiblePredicatesProviderContext
      )
      compatiblePredicates shouldBe empty
    }
  }
}
