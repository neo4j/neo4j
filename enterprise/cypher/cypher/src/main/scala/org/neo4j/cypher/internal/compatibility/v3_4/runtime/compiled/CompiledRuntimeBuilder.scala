/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled

import org.neo4j.cypher.internal.util.v3_4.InvalidArgumentException
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.phases.CompilationState
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{CompiledRuntimeName, _}
import org.neo4j.cypher.internal.compiler.v3_4.phases.LogicalPlanState
import org.neo4j.cypher.internal.frontend.v3_4.notification.RuntimeUnsupportedNotification
import org.neo4j.cypher.internal.frontend.v3_4.phases.{Do, If, Transformer}

class CompiledRuntimeBuilder extends RuntimeBuilder[Transformer[EnterpriseRuntimeContext, LogicalPlanState, CompilationState]] {

  override def create(runtimeName: Option[RuntimeName], useErrorsOverWarnings: Boolean): Transformer[EnterpriseRuntimeContext, LogicalPlanState, CompilationState] =
    runtimeName match {
      case None =>
        BuildCompiledExecutionPlan andThen
          If[EnterpriseRuntimeContext, LogicalPlanState, CompilationState](_.maybeExecutionPlan.isFailure)(
            BuildInterpretedExecutionPlan
          )

      case Some(InterpretedRuntimeName) =>
        BuildInterpretedExecutionPlan

      case Some(CompiledRuntimeName) if useErrorsOverWarnings =>
        BuildCompiledExecutionPlan andThen
          If[EnterpriseRuntimeContext, LogicalPlanState, CompilationState](_.maybeExecutionPlan.isFailure)(
            Do[EnterpriseRuntimeContext, LogicalPlanState, CompilationState]((_, _) => throw new InvalidArgumentException("The given query is not currently supported in the selected runtime"))
          )

      case Some(CompiledRuntimeName) =>
        BuildCompiledExecutionPlan andThen
          If[EnterpriseRuntimeContext, LogicalPlanState, CompilationState](_.maybeExecutionPlan.isFailure)(
            Do((ctx: EnterpriseRuntimeContext) => warnThatCompiledRuntimeDoesNotYetSupportQuery(ctx)) andThen
              BuildInterpretedExecutionPlan
          )
    }

  private def warnThatCompiledRuntimeDoesNotYetSupportQuery(ctx: EnterpriseRuntimeContext) =
    ctx.notificationLogger.log(RuntimeUnsupportedNotification)
}
