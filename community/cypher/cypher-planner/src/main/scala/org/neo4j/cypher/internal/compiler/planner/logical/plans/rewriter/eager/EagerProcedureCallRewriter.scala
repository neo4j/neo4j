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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.logical.steps.skipAndLimit.planLimitOnTopOf
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.collection.immutable.ListSet

/**
 * Insert Eager before and after an eagerized procedure call.
 */
case class EagerProcedureCallRewriter(
  cardinalities: Cardinalities,
  attributesWithoutCardinalities: Attributes[LogicalPlan]
) extends EagerRewriter(attributesWithoutCardinalities.withAlso(cardinalities)) {

  /**
   * Eagerize a plan with reason ProcedureCallEager
   */
  private def eager(p: LogicalPlan): Eager = eagerOnTopOf(p, ListSet(EagernessReason.ProcedureCallEager))

  override def eagerize(
    plan: LogicalPlan,
    semanticTable: SemanticTable,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator
  ): LogicalPlan = {
    plan.endoRewrite(bottomUp(Rewriter.lift {
      // ProcedureCall => E ProcedureCall E
      case pc @ ProcedureCall(_, call) if call.signature.eager =>
        eager(pc.withLhs(eager(pc.source))(SameId(pc.id)))

      // LIMIT E => E LIMIT
      case limit @ Limit(eager @ Eager(source, reasons), _) =>
        val newLimit = planLimitOnTopOf(source, limit.count)(SameId(limit.id))
        val newEager = eager.copy(source = newLimit, reasons)(attributesWithoutCardinalities.copy(eager.id))
        cardinalities.copy(limit.id, newEager.id)
        newEager
    }))
  }
}
