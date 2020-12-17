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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.ir.EagerMode
import org.neo4j.cypher.internal.ir.LazyMode
import org.neo4j.cypher.internal.ir.StrictnessMode

/**
 * A plan that which limits selectivity on child plans.
 */
trait LimitingLogicalPlan {
  self: LazyLogicalPlan =>

  val source: LogicalPlan
}

/**
 * A plan that eventually exhausts all input from LHS.
 */
trait ExhaustiveLogicalPlan {
  self: LogicalPlan =>
}

/**
 * A plan that does not necessarily consume all input from LHS.
 */
trait LazyLogicalPlan {
  self: LogicalPlan =>

  override def strictness: StrictnessMode = LazyMode
}

/**
 * A plan that exhausts all input from LHS before producing it's first output.
 */
trait EagerLogicalPlan extends ExhaustiveLogicalPlan {
  self: LogicalPlan =>

  override def strictness: StrictnessMode = EagerMode
}
