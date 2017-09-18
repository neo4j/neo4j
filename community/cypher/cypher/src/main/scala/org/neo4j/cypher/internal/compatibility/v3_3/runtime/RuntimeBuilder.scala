/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.phases.CompilationState
import org.neo4j.cypher.internal.compiler.v3_3.phases._
import org.neo4j.cypher.internal.frontend.v3_3.InvalidArgumentException
import org.neo4j.cypher.internal.frontend.v3_3.notification.RuntimeUnsupportedNotification
import org.neo4j.cypher.internal.frontend.v3_3.phases.{Do, Transformer}

trait RuntimeBuilder[T <: Transformer[_, _, _]] {
  def create(runtimeName: Option[RuntimeName], useErrorsOverWarnings: Boolean): T
}

object CommunityRuntimeBuilder extends RuntimeBuilder[Transformer[CommunityRuntimeContext, LogicalPlanState, CompilationState]] {
  override def create(runtimeName: Option[RuntimeName], useErrorsOverWarnings: Boolean): Transformer[CommunityRuntimeContext, LogicalPlanState, CompilationState] =
    runtimeName match {
    case None | Some(InterpretedRuntimeName) =>
      BuildInterpretedExecutionPlan

    case Some(x) if useErrorsOverWarnings =>
      throw new InvalidArgumentException(s"This version of Neo4j does not support requested runtime: $x")

    case _ =>
      Do((_: CompilerContext).notificationLogger.log(RuntimeUnsupportedNotification)) andThen
        BuildInterpretedExecutionPlan
  }
}
