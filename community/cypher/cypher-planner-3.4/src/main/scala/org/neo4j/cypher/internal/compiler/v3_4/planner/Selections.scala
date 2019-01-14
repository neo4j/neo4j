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
package org.neo4j.cypher.internal.compiler.v3_4.planner

import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.ir.v3_4.Selections
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.v3_4.expressions.Expression

case class unsolvedPreds(solveds: Solveds) extends ((Selections, LogicalPlan) => Seq[Expression]) {
  def apply(s: Selections, l: LogicalPlan): Seq[Expression] =
    s.scalarPredicatesGiven(l.availableSymbols)
    .filterNot(predicate => solveds.get(l.id).exists(_.queryGraph.selections.contains(predicate)))
}


