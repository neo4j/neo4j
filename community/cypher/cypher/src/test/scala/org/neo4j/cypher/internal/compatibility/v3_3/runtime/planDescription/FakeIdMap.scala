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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription

import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.LogicalPlan

class FakeIdMap extends Map[LogicalPlan, Id] {
  override def +[B1 >: Id](kv: (LogicalPlan, B1)): Map[LogicalPlan, B1] = ???

  override def get(key: LogicalPlan): Option[Id] = Some(new Id)

  override def iterator: Iterator[(LogicalPlan, Id)] = ???

  override def -(key: LogicalPlan): Map[LogicalPlan, Id] = ???
}
