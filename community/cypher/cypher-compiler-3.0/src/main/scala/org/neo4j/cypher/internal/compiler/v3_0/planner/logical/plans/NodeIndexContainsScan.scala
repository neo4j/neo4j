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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans

import org.neo4j.cypher.internal.compiler.v3_0.planner.{CardinalityEstimation, PlannerQuery}
import org.neo4j.cypher.internal.frontend.v3_0.ast.{Expression, PropertyKeyToken, LabelToken}

/*
  * This operator does a full scan of an index, returning all entries that contain a string value
  *
  * It's much slower than an index seek, since all index entries must be examined, but also much faster than an
  * all-nodes scan or label scan followed by a property value filter.
  */
case class NodeIndexContainsScan(idName: IdName,
                                 label: LabelToken,
                                 propertyKey: PropertyKeyToken,
                                 valueExpr: Expression,
                                 argumentIds: Set[IdName])
                                (val solved: PlannerQuery with CardinalityEstimation)
  extends NodeLogicalLeafPlan {

  def availableSymbols = argumentIds + idName
}
