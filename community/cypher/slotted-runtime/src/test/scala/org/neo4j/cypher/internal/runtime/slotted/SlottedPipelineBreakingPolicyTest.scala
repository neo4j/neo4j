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
package org.neo4j.cypher.internal.runtime.slotted

import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SlottedPipelineBreakingPolicyTest extends CypherFunSuite {

  test("pipes that calls SlottedRow.compact is breaking") {
    implicit val idGen = new SequentialIdGen()
    val plansUsingCompact = Seq(
      Eager(Argument()),
      NodeHashJoin(Set.empty, Argument(), Argument()),
      LeftOuterHashJoin(Set.empty, Argument(), Argument()),
      RightOuterHashJoin(Set.empty, Argument(), Argument()),
      ValueHashJoin(Argument(), Argument(), null)
    )
    plansUsingCompact.foreach { plan =>
      withClue(s"${plan.getClass.getSimpleName} plans needs to be breaking because they use SlottedRow.compact(): ") {
        Range.inclusive(-1, 2).foreach { i =>
          SlottedPipelineBreakingPolicy.breakOn(plan, new PhysicalPlanningAttributes.ApplyPlans) shouldBe true
        }
      }
    }
  }

}
