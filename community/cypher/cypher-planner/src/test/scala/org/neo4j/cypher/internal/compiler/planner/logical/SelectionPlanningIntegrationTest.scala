/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher.beLike
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.util.PredicateOrdering
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SelectionPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 with LogicalPlanningIntegrationTestSupport {

  /**
   * The assumptions on the ordering for these selectivities is based on that [[PredicateOrdering]] is used.
   */
  test("Should order predicates in selection first by cost then by selectivity") {
    val label = hasLabels("n", "Label")
    val otherLabel = hasLabels("n", "OtherLabel")
    val nProp = equals(prop("n", "prop"), literalInt(5))
    val nPropIn = in(prop("n", "prop"), listOfInt(5))
    val nParam = equals(varFor("n"), parameter("param", CTAny))
    val nFooBar = equals(prop("n", "foo"), prop("n", "bar"))
    val selectivities = Map[Expression, Double](
      label -> 0.1, // More selective, so will be chosen for LabelScan.
      otherLabel -> 0.9, // So unselective it comes last, even if it has one store access less than nFooBar.
      nProp -> 0.2, // Most selective predicate with 1 store access.
      nPropIn -> 0.2, // -"-
      nParam -> 0.9, // Least selective, but cheapest, therefore comes first.
      nFooBar -> 0.1, // Very selective, but most costly with 2 store accesses. Comes almost last.
    )

    val plan = new given {
      cardinality = mapCardinality {
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _)  =>
          queryGraph.selections.predicates.foldLeft(1000.0){ case (rows, predicate) => rows * selectivities(predicate.expr)}
      }
    }.getLogicalPlanFor(
      """MATCH (n:Label:OtherLabel)
        |WHERE n.prop = 5
        |AND n = $param
        |AND n.foo = n.bar
        |RETURN n""".stripMargin)._2
    val noArgs = Set.empty[String]
    plan should beLike {
      // We cannot use "plan should equal ..." because equality for [[Ands]] is overridden to not care about the order.
      // But unapply takes the order into account for [[Ands]].
      case Selection(Ands(Seq(
        `nParam`,
        `nProp`,
        `nFooBar`,
        `otherLabel`
      )),
        NodeByLabelScan("n", LabelName("Label"), `noArgs`, IndexOrderNone)
      ) => ()
    }
  }

  test("should pick more selective predicate first, even if it accesses a deeply nested property") {
    val nProp = greaterThan(prop("n", "prop"), literalInt(2))
    val nPropChain = equals(
      prop(prop(prop(prop(prop(prop(prop("n", "foo"), "bar"), "baz"), "blob"), "boing"), "peng"), "brrt"),
      literalInt(2))

    val plan = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 10)
      .setLabelCardinality("M", 9)
      .build()
      .plan("MATCH (n:N) WHERE n.foo.bar.baz.blob.boing.peng.brrt = 2 AND n.prop > 2 RETURN n")

    val noArgs = Set.empty[String]

    plan.stripProduceResults should beLike {
      // We cannot use "plan should equal ..." because equality for [[Ands]] is overridden to not care about the order.
      // But unapply takes the order into account for [[Ands]].
      case Selection(Ands(Seq(
      `nPropChain`, // more selective, but needs to access deeply nested property
      `nProp`,
      )),
      NodeByLabelScan("n", LabelName("N"), `noArgs`, IndexOrderNone)
      ) => ()
    }
  }
}
