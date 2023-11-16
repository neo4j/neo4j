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
package org.neo4j.cypher.internal.compiler.phases

import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.RewriteProcedureCalls
import org.neo4j.cypher.internal.rewriting.conditions.CallInvocationsResolved
import org.neo4j.cypher.internal.rewriting.conditions.FunctionInvocationsResolved
import org.neo4j.cypher.internal.util.StepSequencer

/**
 * Given a way to lookup procedure signatures, this phase rewrites unresolved calls into resolved calls.
 */
case object RewriteProcedureCalls extends Phase[PlannerContext, BaseState, BaseState] with RewriteProcedureCalls {

  override def phase = AST_REWRITE

  override def process(from: BaseState, context: PlannerContext): BaseState = process(from, context.planContext)

  override def postConditions: Set[StepSequencer.Condition] =
    Set(CallInvocationsResolved, FunctionInvocationsResolved)

}
