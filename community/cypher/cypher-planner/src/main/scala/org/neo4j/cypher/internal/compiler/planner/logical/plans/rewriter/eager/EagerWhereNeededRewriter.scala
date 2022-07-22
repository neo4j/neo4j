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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager

import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.BestPositionFinder.pickPlansToEagerize
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.CandidateListFinder.findCandidateLists
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ConflictFinder.findConflictingPlans
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ReadsAndWritesFinder.collectReadsAndWrites
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.bottomUp

/**
 * Insert Eager only where it's needed to maintain correct semantics.
 */
case class EagerWhereNeededRewriter(cardinalities: Cardinalities, attributes: Attributes[LogicalPlan])
    extends EagerRewriter(attributes) {

  override def eagerize(plan: LogicalPlan): LogicalPlan = {
    // Step 1: Find reads and writes
    val readsAndWrites = collectReadsAndWrites(plan)
    println()
    println(readsAndWrites)

    // Step 2: Find conflicting plans
    val conflicts = findConflictingPlans(readsAndWrites, plan)
    println()
    println(conflicts)

    // Step 3: Find candidate lists where Eager can be planned
    val candidateLists = findCandidateLists(plan, conflicts)
    println()
    println(candidateLists)

    // Step 4: Pick the best candidate in each sequence.
    val plansToEagerize = pickPlansToEagerize(cardinalities, candidateLists)

    // Step 5: Actually insert Eager operators
    plan.endoRewrite(bottomUp(Rewriter.lift {
      case p: LogicalPlan if plansToEagerize.contains(p.id) =>
        eagerOnTopOf(p, plansToEagerize(p.id))
    }))
  }
}
