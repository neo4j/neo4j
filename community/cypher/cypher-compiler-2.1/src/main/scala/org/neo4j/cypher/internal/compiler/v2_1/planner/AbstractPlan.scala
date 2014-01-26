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

import org.neo4j.cypher.Direction

/**
 * The abstract physical plan. Objects of this type are responsible for
 * keeping the data necessary to create an actual, runnable query plan,
 * but also the data presented to the user when she wants to know the
 * query plan.
 */
trait AbstractPlan {
  def lhs: Option[AbstractPlan]

  def rhs: Option[AbstractPlan]

  def effort: Cost

  def coveredIds: Set[Id]

  def covers(other: AbstractPlan): Boolean = (other.coveredIds -- coveredIds).isEmpty
}


trait PhysicalPlanLeaf {
  self: AbstractPlan =>

  def lhs: Option[AbstractPlan] = None

  def rhs: Option[AbstractPlan] = None
}


abstract class Scan(id: Id) extends AbstractPlan with PhysicalPlanLeaf {
  def coveredIds: Set[Id] = Set(id)
}

case class AllNodesScan(id: Id, effort: Cost) extends Scan(id)

case class LabelScan(id: Id, label: Token, effort: Cost) extends Scan(id)

case class ExpandRelationships(left: AbstractPlan, direction: Direction, effort: Cost, expandsTo: Id) extends AbstractPlan {
  def lhs: Option[AbstractPlan] = Some(left)

  def rhs: Option[AbstractPlan] = None

  def coveredIds: Set[Id] = left.coveredIds + expandsTo
}
