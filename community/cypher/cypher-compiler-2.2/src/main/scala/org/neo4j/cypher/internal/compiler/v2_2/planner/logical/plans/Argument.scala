/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans

import org.neo4j.cypher.internal.compiler.v2_2.planner.{CardinalityEstimation, PlannerQuery}
import org.neo4j.cypher.internal.compiler.v2_2.symbols.CypherType
import org.neo4j.cypher.internal.compiler.v2_2.symbols._

// Argument is used inside of an Apply to feed the row from the LHS of the Apply to the leaf of the RHS
case class Argument(argumentIds: Set[IdName])(val solved: PlannerQuery with CardinalityEstimation)
                    (val typeInfo: Map[String, CypherType] = argumentIds.map( id => id.name -> CTNode).toMap)
  extends LogicalLeafPlan with LogicalPlanWithoutExpressions {

  def availableSymbols = argumentIds

  override def dup(children: Seq[AnyRef]) = children.size match {
    case 1 =>
      copy(children.head.asInstanceOf[Set[IdName]])(solved)(typeInfo).asInstanceOf[this.type]
  }
}
