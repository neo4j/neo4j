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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription

import org.neo4j.cypher.internal.frontend.v3_3.IdentityMap
import org.neo4j.cypher.internal.v3_3.logical.plans.{LogicalPlan, TreeBuilder}

/*
The map of logical plan and ids is used to allow profiling to connect to the right part in the logical plan
to report db hits and rows passed through.
 */
object LogicalPlanIdentificationBuilder extends (LogicalPlan => Map[LogicalPlan, Id]) {
  def apply(plan: LogicalPlan): Map[LogicalPlan, Id] = {

    val builder = new IdAssigner
    builder.mapIds(plan)
  }

  private class IdAssigner extends TreeBuilder[Map[LogicalPlan, Id]] {
    def mapIds(plan: LogicalPlan): Map[LogicalPlan, Id] = create(plan)

    override protected def build(plan: LogicalPlan): Map[LogicalPlan, Id] = IdentityMap(plan -> new Id)

    override protected def build(plan: LogicalPlan, source: Map[LogicalPlan, Id]): Map[LogicalPlan, Id] = source + (plan -> new Id)

    override protected def build(plan: LogicalPlan, lhs: Map[LogicalPlan, Id], rhs: Map[LogicalPlan, Id]): Map[LogicalPlan, Id] = lhs ++ rhs + (plan -> new Id)
  }
}
