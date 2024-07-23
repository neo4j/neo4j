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
package org.neo4j.cypher.internal.administration

import org.neo4j.cypher.internal.AdministrationCommandRuntime.authRelType
import org.neo4j.cypher.internal.AdministrationCommandRuntime.followerError
import org.neo4j.cypher.internal.AdministrationCommandRuntime.getNameFields
import org.neo4j.cypher.internal.AdministrationCommandRuntime.runtimeStringValue
import org.neo4j.cypher.internal.AdministrationCommandRuntime.userLabel
import org.neo4j.cypher.internal.AdministrationCommandRuntime.userNamePropKey
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.procs.ParameterTransformer
import org.neo4j.cypher.internal.procs.QueryHandler
import org.neo4j.cypher.internal.procs.UpdatingSystemCommandExecutionPlan
import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.exceptions.DatabaseAdministrationOnFollowerException
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.exceptions.Status.HasStatus
import org.neo4j.values.virtual.VirtualValues

case class DropUserExecutionPlanner(
  normalExecutionEngine: ExecutionEngine,
  securityAuthorizationHandler: SecurityAuthorizationHandler
) {

  def planDropUser(userName: Either[String, Parameter], sourcePlan: Option[ExecutionPlan]): ExecutionPlan = {
    val userNameFields = getNameFields("username", userName)
    UpdatingSystemCommandExecutionPlan(
      "DropUser",
      normalExecutionEngine,
      securityAuthorizationHandler,
      s"""MATCH (user:$userLabel {$userNamePropKey: $$`${userNameFields.nameKey}`})
         |CALL {
         |  WITH user
         |  OPTIONAL MATCH (user)-[:$authRelType]->(auth)
         |  DETACH DELETE auth
         |}
         |DETACH DELETE user
         |RETURN 1 AS ignore""".stripMargin,
      VirtualValues.map(Array(userNameFields.nameKey), Array(userNameFields.nameValue)),
      QueryHandler
        .handleError {
          case (error: HasStatus, p) if error.status() == Status.Cluster.NotALeader =>
            new DatabaseAdministrationOnFollowerException(
              s"Failed to delete the specified user '${runtimeStringValue(userName, p)}': $followerError",
              error
            )
          case (error, p) => new CypherExecutionException(
              s"Failed to delete the specified user '${runtimeStringValue(userName, p)}'.",
              error
            )
        },
      sourcePlan,
      parameterTransformer = ParameterTransformer().convert(userNameFields.nameConverter)
    )
  }
}
