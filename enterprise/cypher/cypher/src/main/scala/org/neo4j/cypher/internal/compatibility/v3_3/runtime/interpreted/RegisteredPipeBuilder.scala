/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.PipeBuilder
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.Pipe
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.LogicalPlan

class RegisteredPipeBuilder(fallback: PipeBuilder) extends PipeBuilder {
  override def build(plan: LogicalPlan): Pipe =
    plan match {
      case _ => fallback.build(plan)
    }

  override def build(plan: LogicalPlan, source: Pipe): Pipe =
    plan match {
      case _ => fallback.build(plan, source)
    }

  override def build(plan: LogicalPlan, lhs: Pipe, rhs: Pipe): Pipe =
    plan match {
      case _ => fallback.build(plan, lhs, rhs)
    }
}
