/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.util.v3_4.attribution.Attributes
import org.neo4j.cypher.internal.util.v3_4.{Rewriter, bottomUp}
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan

import scala.collection.mutable

/**
 * Runs through LogicalPlan and copies duplicate plans to make sure the
 * plan doesn't contain elements that are ID identical.
 */
case class removeIdenticalPlans(attributes:Attributes) extends Rewriter {

  override def apply(input: AnyRef) = {
    val seenIDs = mutable.Set.empty[Int]

    val rewriter: Rewriter = bottomUp(Rewriter.lift {
      case plan: LogicalPlan if seenIDs(plan.id.x) => plan.copyPlanWithIdGen(attributes.copy(plan.id))
      case plan: LogicalPlan => seenIDs += plan.id.x ; plan
    })

    rewriter.apply(input)
  }
}
