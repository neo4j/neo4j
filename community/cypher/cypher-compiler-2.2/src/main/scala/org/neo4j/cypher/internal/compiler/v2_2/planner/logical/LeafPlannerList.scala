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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.pipes.LazyLabel
import org.neo4j.cypher.internal.compiler.v2_2.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.{IndexHintException, LabelScanHintException}

case class LeafPlannerList(leafPlanners: LeafPlanner*) {
  def candidates(qg: QueryGraph, f: (LogicalPlan, QueryGraph) => LogicalPlan = (plan, _) => plan )(implicit context: LogicalPlanningContext): Iterable[Seq[LogicalPlan]] = {
    val logicalPlans = leafPlanners.flatMap(_(qg)).map(f(_,qg))
    //check so that we respect the provided hints
    assertHints(qg, logicalPlans)
    logicalPlans.groupBy(_.availableSymbols).values
  }

  //Check so that there are leaf plans satisfying all provided hints, otherwise fail
  private def assertHints(queryGraph: QueryGraph, plans: Iterable[LogicalPlan]) = {
    queryGraph.hints.foreach {
      // using index name:label(property)
      case UsingIndexHint(Identifier(name), LabelName(label), Identifier(property)) =>
        val satisfied = plans.exists {

          case NodeIndexSeek(IdName(n), LabelToken(l, _), PropertyKeyToken(p, _), _, _) =>
            n == name && l == label && p == property

          case NodeIndexUniqueSeek(IdName(n), LabelToken(l, _), PropertyKeyToken(p, _), _, _) =>
            n == name && l == label && p == property

          case _ => false
        }
        if (!satisfied) throw new IndexHintException(name, label, property, "No such index found.")

      // using scan name:label
      case UsingScanHint(Identifier(name), LabelName(label)) =>
        val satisfied = plans.exists {
          case NodeByLabelScan(IdName(n), LazyLabel(l), _) => n == name && l == label
          case _ => false
        }

        if (!satisfied) throw new LabelScanHintException(name, label, "No scan could be performed.")

      case _ => //do nothing, we're fine
    }
  }
}
