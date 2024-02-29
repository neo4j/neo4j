/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.scheduling

import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.attribution.Id

object WorkIdentity {

  def fromPlan(plan: LogicalPlan, postfix: String = ""): WorkIdentity =
    WorkIdentityImpl(plan.id, plan.getClass.getSimpleName + postfix)

  def fromFusedPlans(fusedPlans: Iterable[LogicalPlan]): WorkIdentity = {
    WorkIdentityImpl(fusedPlans.head.id, s"Fused(${fusedPlans.map(_.getClass.getSimpleName).mkString("-")})")
  }
}

trait HasWorkIdentity {
  def workIdentity: WorkIdentity
}

trait WorkIdentity {

  /**
   * Identifies the work/computation performed by this task, as opposed to identifying the task itself.
   * If multiple different tasks all execute the same logic (e.g., operator pipeline) they should return the same <code>workId</code>.
   */
  def workId: Id

  /**
   * Describes the work/computation performed by this task, as opposed to describing the task itself.
   * Multiple tasks that each execute the same logic (e.g., operator pipeline) should return the same value.
   * E.g., OperatorPipeline[AllNodesScan].
   * Two tasks may return the same value <code>workDescription</code> but different values for <code>workId</code>.
   */
  def workDescription: String

  override def toString: String = s"$workDescription-${workId.x}"
}

trait WorkIdentityMutableDescription extends WorkIdentity {
  def updateDescription(newDescription: String): Unit
}

case class WorkIdentityImpl(workId: Id, workDescription: String) extends WorkIdentity with HasWorkIdentity {
  override def workIdentity: WorkIdentity = this
}

case class WorkIdentityMutableDescriptionImpl(workId: Id, prefix: String, description: String)
    extends WorkIdentityMutableDescription with HasWorkIdentity {

  private[this] var _workDescription: String = prefix + description

  override def workIdentity: WorkIdentity = this

  override def workDescription: String = _workDescription

  override def updateDescription(newDescription: String): Unit = _workDescription = prefix + newDescription
}
