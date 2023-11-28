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
package org.neo4j.cypher.internal.compiler.planner.logical.plans

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.OrLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.allRelationshipsScanLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.labelScanLeafPlanner
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters.PredicateConverter
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.storageengine.api.AllRelationshipsScan

class OrLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should not plan node filter disjunction on top of relationship leaf plans") {
    // Allow only NodeByLabelScan and AllRelationshipsScan leaf plans.
    val orLeafPlanner = OrLeafPlanner(Seq(
      labelScanLeafPlanner(Set.empty),
      allRelationshipsScanLeafPlanner(Set.empty)
    ))
    // Node predicate disjunction
    val predicates: Set[Expression] = Set(ors(hasLabels("n", "A"), hasLabels("n", "B")))

    new givenConfig {
      qg = QueryGraph(
        selections = Selections(predicates.flatMap(_.asPredicates)),
        patternNodes = Set("n", "m"),
        patternRelationships =
          Set(PatternRelationship(v"r", (v"n", v"m"), OUTGOING, Seq(relTypeName("R")), SimplePatternLength)),
        argumentIds = Set()
      )
      // Make AllRelationshipsScan (also with Selection on top) cheap,
      // make NodeByLabelScan expensive.
      cost = {
        case (_: AllRelationshipsScan, _, _, _) => 1.0
        case (_: Selection, _, _, _)            => 1.0
        case (_: NodeByLabelScan, _, _, _)      => 1000.0
      }
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = orLeafPlanner(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans shouldEqual Set(
        new LogicalPlanBuilder(wholePlan = false)
          .orderedDistinct(Seq("n"), "n AS n")
          .orderedUnion("n ASC")
          .|.nodeByLabelScan("n", "B", IndexOrderAscending)
          .nodeByLabelScan("n", "A", IndexOrderAscending)
          .build()
          /* and not
            new LogicalPlanBuilder(wholePlan = false)
            .distinct("r AS r", "n AS n", "m AS m")
            .union()
            .|.filter("n:B")
            .|.allRelationshipsScan("(n)-[r]->(m)")
            .filter("n:A")
            .allRelationshipsScan("(n)-[r]->(m)")
            .build()
           */
      )
    }
  }
}
