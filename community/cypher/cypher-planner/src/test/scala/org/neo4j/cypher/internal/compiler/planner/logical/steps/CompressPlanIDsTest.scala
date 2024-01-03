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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.planner.logical.steps.CompressPlanIDsTest.GapIdGen
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.NO_TRACING
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CompressPlanIDsTest extends CypherFunSuite with AstConstructionTestSupport {

  private val plan = logicalPlanBuilder()
    .produceResults("a")
    .apply()
    .|.allNodeScan("a", "b")
    .nestedPlanCollectExpressionProjection("x", "b.prop")
    .|.expand("(a)-->(b)")
    .|.allNodeScan("a")
    .allNodeScan("b")
    .build()

  test("should assign consecutive IDs starting from 0") {
    val p = compress(logicalPlanStateWithAttrributes(plan)).logicalPlan
    allPlans(p).map(_.id.x).toSet should equal((0 to 6).toSet)
  }

  test("should compress planning attributes") {
    val inState = logicalPlanStateWithAttrributes(plan)
    val originalPlans = allPlans(inState.logicalPlan)

    val outState = compress(inState)
    allPlans(outState.logicalPlan).foreach { p =>
      val originalPlan = originalPlans.find(_ == p).get // Plans are equal even if they have different IDs
      inState.planningAttributes.solveds.getOption(originalPlan.id).foreach(
        outState.planningAttributes.solveds(p.id) should equal(_)
      )
      inState.planningAttributes.cardinalities.getOption(originalPlan.id).foreach(
        outState.planningAttributes.cardinalities(p.id) should equal(_)
      )
      inState.planningAttributes.providedOrders.getOption(originalPlan.id).foreach(
        outState.planningAttributes.providedOrders(p.id) should equal(_)
      )
      inState.planningAttributes.leveragedOrders.getOption(originalPlan.id).foreach(
        outState.planningAttributes.leveragedOrders(p.id) should equal(_)
      )
    }
  }

  // plan.flatten does not find plans in NestedPlanExpressions
  private def allPlans(plan: LogicalPlan): Seq[LogicalPlan] = plan.folder.treeFold(Seq.empty[LogicalPlan]) {
    case plan: LogicalPlan => acc => TraverseChildren(acc :+ plan)
  }

  private def compress(state: LogicalPlanState): LogicalPlanState = {
    val plannerContext = mock[PlannerContext]
    when(plannerContext.tracer).thenReturn(NO_TRACING)
    when(plannerContext.cancellationChecker).thenReturn(CancellationChecker.NeverCancelled)
    CompressPlanIDs.transform(state, plannerContext)
  }

  private def logicalPlanStateWithAttrributes(plan: LogicalPlan): LogicalPlanState = {
    val state = LogicalPlanState(InitialState("", None, IDPPlannerName, new AnonymousVariableNameGenerator))
      .withMaybeLogicalPlan(Some(plan))
    allPlans(plan).foreach { p =>
      // Some plans do not get solved assigned during planning. That must still work with CompressIDs.
      if (p.id.x != GapIdGen.start + GapIdGen.inc) {
        state.planningAttributes.solveds.set(
          p.id,
          RegularSinglePlannerQuery(QueryGraph(patternNodes = Set(v"n${p.id.x}")))
        )
      }
      state.planningAttributes.cardinalities.set(p.id, Cardinality(p.id.x + 1))
      state.planningAttributes.providedOrders.set(p.id, ProvidedOrder.asc(varFor(s"v${p.id.x}")))
      // For leveraged order, do only assign it to some plans
      if (p.id.x == GapIdGen.start) {
        state.planningAttributes.leveragedOrders.set(p.id, true)
      } else if (p.id.x == GapIdGen.start + GapIdGen.inc) {
        state.planningAttributes.leveragedOrders.set(p.id, false)
      }
    }
    state
  }

  private def logicalPlanBuilder(): LogicalPlanBuilder = new LogicalPlanBuilder() {
    override val idGen: IdGen = new GapIdGen
  }
}

object CompressPlanIDsTest {

  class GapIdGen extends IdGen {
    private var i: Int = GapIdGen.start

    def id(): Id = {
      val id = Id(i)
      i += GapIdGen.inc
      id
    }
  }

  object GapIdGen {
    val start = 10
    val inc = 5
  }
}
