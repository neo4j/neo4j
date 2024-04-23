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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.planner.logical.steps.skipAndLimit.planLimitOnTopOf
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.EagerLogicalPlan
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LoadCSV
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalUnaryPlan
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.Rewriter.BottomUpMergeableRewriter
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.bottomUp

case class cleanUpEager(cardinalities: Cardinalities, attributes: Attributes[LogicalPlan]) extends Rewriter
    with BottomUpMergeableRewriter {

  override val innerRewriter: Rewriter = Rewriter.lift {

    // E E L => E L
    case Eager(inner: LogicalUnaryPlan with EagerLogicalPlan, _) => inner

    // E U => U E
    case eager @ Eager(unwind @ UnwindCollection(source, _, _), reasons) =>
      val newEager = eager.copy(source = source, reasons = reasons)(attributes.copy(eager.id))
      cardinalities.copy(source.id, newEager.id)
      unwind.copy(source = newEager)(SameId(unwind.id))

    // E LCSV => LCSV E
    case eager @ Eager(loadCSV @ LoadCSV(source, _, _, _, _, _, _), reasons) =>
      val newEager = eager.copy(source = source, reasons = reasons)(attributes.copy(eager.id))
      cardinalities.copy(source.id, newEager.id)
      loadCSV.copy(source = newEager)(SameId(loadCSV.id))

    // LIMIT E => E LIMIT
    case limit @ Limit(eager @ Eager(source, reasons), _) =>
      val newLimit = planLimitOnTopOf(source, limit.count)(SameId(limit.id))
      val newEager = eager.copy(source = newLimit, reasons)(attributes.copy(eager.id))
      cardinalities.copy(limit.id, newEager.id)
      newEager
  }

  private val instance: Rewriter = bottomUp(innerRewriter)

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
