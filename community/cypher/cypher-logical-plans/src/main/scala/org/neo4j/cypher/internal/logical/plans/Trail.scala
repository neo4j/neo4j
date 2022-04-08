/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.util.attribution.IdGen

/**
 * Used for solve queries like: `(start) [(innerStart)-->(innerEnd)]{i, j} (end)`
 *
 * @param left                  source plan
 * @param right                 inner plan to repeat
 * @param repetitions           how many times to repeat the RHS on each partial result
 * @param start                 the outside node variable where the quantified pattern
 *                              starts. Assumed to be present in the output of `left`
 * @param end                   the outside node variable where the quantified pattern
 *                              ends. Projected in output if present.
 * @param innerStart            the node variable where the inner pattern starts
 * @param innerEnd              the node variable where the inner pattern ends
 * @param groupNodes            node variables to aggregate
 * @param groupRelationships    relationship variables to aggregate
 * @param allRelationships      these are a superset of all relationship variables in the inner pattern.
 *                              relationship uniqueness must be enforced between these relationships and those in [[allRelationshipGroups]]
 * @param allRelationshipGroups relationship group variables originating from previous [[Trail]] operators.
 *                              relationship uniqueness must be enforced between these relationships and those in [[allRelationships]].
 */
case class Trail(override val left: LogicalPlan,
                 override val right: LogicalPlan,
                 repetitions: Repetitions,
                 start: String,
                 end: Option[String],
                 innerStart: String,
                 innerEnd: String,
                 groupNodes: Set[GroupEntity],
                 groupRelationships: Set[GroupEntity],
                 allRelationships: Set[String],
                 allRelationshipGroups: Set[String])(implicit idGen: IdGen)
  extends LogicalBinaryPlan(idGen) with ApplyPlan {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(left = newLHS)(idGen)
  override def withRhs(newRHS: LogicalPlan)(idGen: IdGen): LogicalBinaryPlan = copy(right = newRHS)(idGen)
  override val availableSymbols: Set[String] = left.availableSymbols ++ right.availableSymbols
}

case class Repetitions(min: Int, max: UpperBound)
sealed trait UpperBound
case object Unlimited extends UpperBound
case class Limited(n: Int) extends UpperBound

case class GroupEntity(innerName: String, outerName: String)