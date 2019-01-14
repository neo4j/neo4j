/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
