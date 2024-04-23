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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.bottomUp

import scala.collection.mutable

/**
 * Runs through LogicalPlan and copies duplicate plans to make sure the plan doesn't contain elements that are ID
 * identical. This is needed by PROFILE which accumulates metrics by assuming that each plan has a unique ID. Plans
 * may end up with the same ID because IDP begins by figuring out the best leafs plans, and by computing the
 * initial node connections used as seeds by the IDP algorithm. Multiple of these seeds may contain the same leaf plan.
 */
case class removeIdenticalPlans(attributes: Attributes[LogicalPlan]) extends Rewriter {

  override def apply(input: AnyRef): AnyRef = {
    val seenIDs = mutable.Set.empty[Int]

    val rewriter: Rewriter = bottomUp(Rewriter.lift {
      case plan: LogicalPlan if seenIDs(plan.id.x) => plan.copyPlanWithIdGen(attributes.copy(plan.id))
      case plan: LogicalPlan                       => seenIDs += plan.id.x; plan
    })

    rewriter.apply(input)
  }
}
