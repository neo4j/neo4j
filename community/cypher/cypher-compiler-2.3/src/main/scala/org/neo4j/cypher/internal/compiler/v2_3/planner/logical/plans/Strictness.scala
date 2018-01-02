/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans

sealed trait StrictnessMode extends (Strictness => Boolean) {
  self: Product =>

  def apply(havingStrictness: Strictness) = havingStrictness.strictness == self

  override def toString: String = self.productPrefix
}

case object LazyMode extends StrictnessMode

case object EagerMode extends StrictnessMode

trait Strictness {
  def strictness: StrictnessMode
}

trait LazyLogicalPlan {
  self: LogicalPlan =>

  override def strictness: StrictnessMode = LazyMode
}

trait EagerLogicalPlan {
  self: LogicalPlan =>

  override def strictness: StrictnessMode = EagerMode
}
