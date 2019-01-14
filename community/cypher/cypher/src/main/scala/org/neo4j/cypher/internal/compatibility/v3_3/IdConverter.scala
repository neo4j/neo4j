/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compatibility.v3_3

import org.neo4j.cypher.internal.util.v3_4.attribution._
import org.neo4j.cypher.internal.v3_3.logical.plans.{LogicalPlan => LogicalPlanV3_3}

trait IdConverter {
  def convertId(plan:LogicalPlanV3_3): IdGen
}

/**
  * Converts ids while keeping track of the maximum encountered id. This id can
  * then be used to create a new IdGen which continues with the next available id.
  */
class MaxIdConverter extends IdConverter {

  private var _maxId: Int = 0

  def maxId: Int = _maxId

  override def convertId(plan: LogicalPlanV3_3): IdGen = {
    val id = plan.assignedId.underlying
    _maxId = math.max(_maxId, id)
    SameId(Id(id))
  }

  def idGenFromMax: IdGen = new SequentialIdGen(_maxId + 1)
}
