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
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.exceptions.Status.HasStatus
import org.neo4j.values.virtual.VirtualValues

case class EnsureNodeExistsExecutionPlanner(normalExecutionEngine: ExecutionEngine, securityAuthorizationHandler: SecurityAuthorizationHandler) {

  def planEnsureNodeExists(label: String,
                           name: Either[String, Parameter],
                           valueMapper: String => String,
                           extraFilter: String => String,
                           labelDescription: String,
                           action:String,
                           sourcePlan: Option[ExecutionPlan]): ExecutionPlan = {
    val nameFields = getNameFields("name", name, valueMapper = valueMapper)
    UpdatingSystemCommandExecutionPlan("EnsureNodeExists",
      normalExecutionEngine,
      securityAuthorizationHandler,
      s"""MATCH (node:$label {name: $$`${nameFields.nameKey}`})
         |${extraFilter("node")}
         |RETURN node""".stripMargin,
      VirtualValues.map(Array(nameFields.nameKey), Array(nameFields.nameValue)),
      QueryHandler
        .handleNoResult(p => Some(new InvalidArgumentException(
          s"Failed to $action the specified ${labelDescription.toLowerCase} '${runtimeStringValue(name, p)}': $labelDescription does not exist.")))
        .handleError {
          case (error: HasStatus, p) if error.status() == Status.Cluster.NotALeader =>
            new DatabaseAdministrationOnFollowerException(s"Failed to $action the specified ${labelDescription.toLowerCase} '${runtimeStringValue(name, p)}': $followerError", error)
          case (error, p) => new IllegalStateException(s"Failed to $action the specified ${labelDescription.toLowerCase} '${runtimeStringValue(name, p)}'.", error) // should not get here but need a default case
        },
      sourcePlan,
      parameterConverter = nameFields.nameConverter
    )
  }

}
