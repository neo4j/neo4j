/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_1.ast.Expression
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.{LeafPlanner, CandidateList, LogicalPlanContext}

object generateLeafPlanCandidateLists {
  def apply()(implicit context: LogicalPlanContext): Iterable[CandidateList] = {
    val qg = context.queryGraph
    val predicates: Seq[Expression] = qg.selections.flatPredicates
    val labelPredicateMap = qg.selections.labelPredicates

    val leafPlanners: Seq[LeafPlanner] = Seq(
      // arguments from the outside in case we are in a sub query,
      argumentLeafPlanner(context.argumentIds),

      // MATCH n WHERE id(n) = {id} RETURN n
      idSeekLeafPlanner(predicates),

      // MATCH n WHERE n.prop = {val} RETURN n
      uniqueIndexSeekLeafPlanner(predicates, labelPredicateMap),

      // MATCH n WHERE n.prop = {val} RETURN n
      indexSeekLeafPlanner(predicates, labelPredicateMap),

      // MATCH (n:Person) RETURN n
      labelScanLeafPlanner(labelPredicateMap),

      // MATCH n RETURN n
      allNodesLeafPlanner()
    )


    val plans: Seq[LogicalPlan] = leafPlanners.flatMap(_.apply())
    plans.groupBy(_.coveredIds).values.map(CandidateList)
  }
}
