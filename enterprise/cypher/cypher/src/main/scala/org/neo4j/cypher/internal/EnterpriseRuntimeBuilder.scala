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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.compatibility.v3_4.runtime._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.{BuildCompiledExecutionPlan, EnterpriseRuntimeContext}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.phases.CompilationState
import org.neo4j.cypher.internal.compiler.v3_4.phases.LogicalPlanState
import org.neo4j.cypher.internal.frontend.v3_4.notification.RuntimeUnsupportedNotification
import org.neo4j.cypher.internal.frontend.v3_4.phases.{Do, If, Transformer}
import org.neo4j.cypher.internal.util.v3_4.InvalidArgumentException

import scala.util.{Failure, Success}

object EnterpriseRuntimeBuilder extends RuntimeBuilder[Transformer[EnterpriseRuntimeContext, LogicalPlanState, CompilationState]] {

  type CompilationStateTransformer = Transformer[EnterpriseRuntimeContext, CompilationState, CompilationState]

  private val AssertExecutionPlan: CompilationStateTransformer = AssertExecutionPlan(false)
  private val AssertExecutionPlanAndWrapException: CompilationStateTransformer = AssertExecutionPlan(true)
  private def AssertExecutionPlan(wrapException: Boolean): CompilationStateTransformer =
    Do((state, ctx) => state.maybeExecutionPlan match {
      case Success(_) => state
      case Failure(t) =>
        if (wrapException)
          throw new InvalidArgumentException("The given query is not currently supported in the selected runtime", t)
        else throw t
    })

  private def Fallback(fallback: Transformer[EnterpriseRuntimeContext, LogicalPlanState, CompilationState]
                      ): CompilationStateTransformer =
    If[EnterpriseRuntimeContext, LogicalPlanState, CompilationState](_.maybeExecutionPlan.isFailure) { fallback }

  private def FallbackWithNotification(fallback: Transformer[EnterpriseRuntimeContext, LogicalPlanState, CompilationState]
                                      ): CompilationStateTransformer =
    Fallback(
      Do((_: EnterpriseRuntimeContext).notificationLogger.log(RuntimeUnsupportedNotification))
        andThen fallback
    )

  def create(runtimeName: Option[RuntimeName], useErrorsOverWarnings: Boolean): Transformer[EnterpriseRuntimeContext, LogicalPlanState, CompilationState] = {

    def pickInterpretedExecutionPlan() =
      BuildSlottedExecutionPlan andThen Fallback(BuildInterpretedExecutionPlan)

    runtimeName match {
      case None =>
        BuildCompiledExecutionPlan andThen Fallback(pickInterpretedExecutionPlan())

      case Some(InterpretedRuntimeName) =>
        BuildInterpretedExecutionPlan andThen AssertExecutionPlan

      case Some(MorselRuntimeName) if useErrorsOverWarnings =>
        BuildVectorizedExecutionPlan andThen AssertExecutionPlanAndWrapException

      case Some(MorselRuntimeName) =>
        BuildVectorizedExecutionPlan andThen FallbackWithNotification(pickInterpretedExecutionPlan())

      case Some(SlottedRuntimeName) if useErrorsOverWarnings =>
        BuildSlottedExecutionPlan andThen AssertExecutionPlan

      case Some(SlottedRuntimeName) =>
        BuildSlottedExecutionPlan andThen
          FallbackWithNotification(BuildInterpretedExecutionPlan)

      case Some(CompiledRuntimeName) if useErrorsOverWarnings =>
        BuildCompiledExecutionPlan andThen AssertExecutionPlanAndWrapException

      case Some(CompiledRuntimeName) =>
        BuildCompiledExecutionPlan andThen FallbackWithNotification(pickInterpretedExecutionPlan())

      case Some(x) =>
        throw new InvalidArgumentException(s"This version of Neo4j does not support requested runtime: $x")
    }
  }
}
