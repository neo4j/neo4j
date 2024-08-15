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
package org.neo4j.cypher.internal.compiler.test_helpers

import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPQueryGraphSolverMonitor
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

class NoopIDPQueryGraphSolverMonitor extends IDPQueryGraphSolverMonitor {
  override def noIDPIterationFor(graph: QueryGraph, result: LogicalPlan): Unit = ()
  override def initTableFor(graph: QueryGraph): Unit = ()
  override def startIDPIterationFor(graph: QueryGraph): Unit = ()
  override def endIDPIterationFor(graph: QueryGraph, result: LogicalPlan): Unit = ()
  override def emptyComponentPlanned(graph: QueryGraph, plan: LogicalPlan): Unit = ()
  override def startConnectingComponents(graph: QueryGraph): Unit = ()
  override def endConnectingComponents(graph: QueryGraph, result: LogicalPlan): Unit = ()
  override def startIteration(iteration: Int): Unit = ()
  override def endIteration(iteration: Int, depth: Int, tableSize: Int): Unit = ()
  override def foundPlanAfter(iterations: Int): Unit = ()
}
