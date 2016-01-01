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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.{LogicalPlanningContext, Candidates, LeafPlanner}
import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps.QueryPlanProducer._
import org.neo4j.cypher.internal.compiler.v2_1.ast.PatternExpression

object argumentLeafPlanner extends LeafPlanner {
  def apply(qg: QueryGraph)(implicit ignored: LogicalPlanningContext, subQueriesLookupTable: Map[PatternExpression, QueryGraph]) = {
    val givenNodeIds = qg.argumentIds intersect qg.patternNodes
    if (givenNodeIds.isEmpty)
      Candidates()
    else
      // TODO: This looks suspiciously wrong
      Candidates(planArgumentRow(patternNodes = givenNodeIds, patternRels = Set.empty, other = Set.empty))
  }
}
