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
package org.neo4j.cypher.internal.administration

import org.neo4j.cypher.internal.AdministrationCommandRuntime.followerError
import org.neo4j.cypher.internal.AdministrationCommandRuntime.getNameFields
import org.neo4j.cypher.internal.AdministrationCommandRuntime.runtimeStringValue
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.procs.QueryHandler
import org.neo4j.cypher.internal.procs.UpdatingSystemCommandExecutionPlan
import org.neo4j.exceptions.DatabaseAdministrationOnFollowerException
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.exceptions.Status.HasStatus
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

case class DoNothingExecutionPlanner(normalExecutionEngine: ExecutionEngine, securityAuthorizationHandler: SecurityAuthorizationHandler) {

  def planDoNothingIfNotExists(label: String,
                               labelDescription: String,
                               name: Either[String, Parameter],
                               valueMapper: String => String,
                               operation: String,
                               sourcePlan: Option[ExecutionPlan]): ExecutionPlan =
    planDoNothing("DoNothingIfNotExists",
      label, name, valueMapper,
      QueryHandler
        .ignoreNoResult()
        .handleError(handleErrorFn(operation, label, labelDescription, name)),
      sourcePlan
    )

  def planDoNothingIfExists(label: String, labelDescription: String, name: Either[String, Parameter], valueMapper: String => String, sourcePlan: Option[ExecutionPlan]): ExecutionPlan =
    planDoNothing("DoNothingIfExists",
      label, name, valueMapper,
      QueryHandler
        .ignoreOnResult()
        .handleError(handleErrorFn("create", label, labelDescription, name)),
      sourcePlan)

  private def planDoNothing(planName: String,
                            label: String,
                            name: Either[String, Parameter],
                            valueMapper: String => String,
                            queryHandler: QueryHandler,
                            sourcePlan: Option[ExecutionPlan]): ExecutionPlan = {
    val nameFields = getNameFields("name", name, valueMapper = valueMapper)
    UpdatingSystemCommandExecutionPlan(planName,
      normalExecutionEngine,
      securityAuthorizationHandler,
      s"""
         |MATCH (node:$label {name: $$`${nameFields.nameKey}`})
         |RETURN node.name AS name
        """.stripMargin,
      VirtualValues.map(Array(nameFields.nameKey), Array(nameFields.nameValue)),
      queryHandler,
      sourcePlan,
      parameterConverter = nameFields.nameConverter
    )
  }

  private def handleErrorFn(operation: String, label: String, labelDescription: String, name: Either[String, Parameter]): (Throwable, MapValue) => Throwable = {
    case (error: HasStatus, p) if error.status() == Status.Cluster.NotALeader =>
      new DatabaseAdministrationOnFollowerException(s"Failed to $operation the specified ${labelDescription.toLowerCase} '${runtimeStringValue(name, p)}': $followerError", error)
    case (error, p) => new IllegalStateException(s"Failed to $operation the specified ${labelDescription.toLowerCase} '${runtimeStringValue(name, p)}'.", error) // should not get here but need a default case
  }

}
