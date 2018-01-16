/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.rewriter

import java.util
import java.util.Collections

import org.neo4j.cypher.internal.frontend.v3_3.{Rewriter, bottomUp}
import org.neo4j.cypher.internal.v3_3.logical.plans.LogicalPlan

/**
 * Runs through LogicalPlan and copies duplicate plans to make sure the
 * plan doesn't contain elements that are referentially identical.
 */
case object removeIdenticalPlans extends Rewriter {

  override def apply(input: AnyRef) = {
    val seenPlans = Collections.newSetFromMap(new util.IdentityHashMap[LogicalPlan, java.lang.Boolean]())

    val rewriter: Rewriter = bottomUp(Rewriter.lift {
      case plan: LogicalPlan if seenPlans.contains(plan) => plan.copyPlan()
      case plan: LogicalPlan =>
        seenPlans.add(plan)
        plan
    })

    rewriter.apply(input)
  }
}
