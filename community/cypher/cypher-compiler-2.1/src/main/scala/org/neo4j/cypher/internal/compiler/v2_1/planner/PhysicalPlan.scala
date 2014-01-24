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
package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.neo4j.cypher.internal.compiler.v2_1.runtime.Operator
import org.neo4j.graphdb.Direction

/**
 * The physical plan. Stateless thing that knows how to create the operator tree which
 * is actually execute.
 */
trait PhysicalPlan {
  def lhs: Option[PhysicalPlan]
  def rhs: Option[PhysicalPlan]

  def createPhysicalPlan(): Operator = ???

  def effort: Cost
}


trait PhysicalPlanLeaf {
  self: PhysicalPlan =>

  def lhs: Option[PhysicalPlan] = None
  def rhs: Option[PhysicalPlan] = None
}


case class AllNodesScan(id: Id, effort: Cost) extends PhysicalPlan with PhysicalPlanLeaf

case class LabelScan(id: Id, label: Token, effort: Cost) extends PhysicalPlan with PhysicalPlanLeaf

case class ExpandRelationships(left: PhysicalPlan, direction: Direction, effort: Cost) extends PhysicalPlan {
  def lhs: Option[PhysicalPlan] = Some(left)
  def rhs: Option[PhysicalPlan] = None
}
