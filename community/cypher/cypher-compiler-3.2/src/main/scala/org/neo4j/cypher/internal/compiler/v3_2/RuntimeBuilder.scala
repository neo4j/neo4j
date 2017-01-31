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
package org.neo4j.cypher.internal.compiler.v3_2

import org.neo4j.cypher.internal.compiler.v3_2.phases._
import org.neo4j.cypher.internal.frontend.v3_2.InvalidArgumentException
import org.neo4j.cypher.internal.frontend.v3_2.notification.RuntimeUnsupportedNotification

object RuntimeBuilder {
  def create(runtimeName: Option[RuntimeName], useErrorsOverWarnings: Boolean): Transformer[Context] = runtimeName match {
    case None | Some(InterpretedRuntimeName) =>
      BuildInterpretedExecutionPlan

    case Some(CompiledRuntimeName) if useErrorsOverWarnings =>
      BuildCompiledExecutionPlan andThen
      If(_.maybeExecutionPlan.isEmpty)(
        Do(_ => throw new InvalidArgumentException("The given query is not currently supported in the selected runtime"))
      )

    case Some(CompiledRuntimeName) =>
      BuildCompiledExecutionPlan andThen
      If(_.maybeExecutionPlan.isEmpty)(
        Do { (c:Context) => c.notificationLogger.log(RuntimeUnsupportedNotification) } andThen
        BuildInterpretedExecutionPlan
      )

  }
}
