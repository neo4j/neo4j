/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps._
import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryGraph

case class PlanningStrategyConfiguration(
  leafPlanners: LeafPlannerList,
  applySelections: PlanTransformer[QueryGraph],
  pickBestCandidate: CandidateSelector
)

object PlanningStrategyConfiguration {
  val default = PlanningStrategyConfiguration(
    pickBestCandidate = pickBestPlan,
    applySelections = selectPatternPredicates(selectCovered),
    leafPlanners = LeafPlannerList(
      // arguments from the outside in case we are in a sub query,
      argumentLeafPlanner,

      // MATCH n WHERE id(n) = {id} RETURN n
      idSeekLeafPlanner,

      // MATCH n WHERE n.prop = {val} RETURN n
      uniqueIndexSeekLeafPlanner,

      // MATCH n WHERE n.prop = {val} RETURN n
      indexSeekLeafPlanner,

      // MATCH (n:Person) RETURN n
      labelScanLeafPlanner,

      // MATCH n RETURN n
      allNodesLeafPlanner
    )
  )
}


