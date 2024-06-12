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

import org.neo4j.configuration.Config
import org.neo4j.cypher.internal.AdministrationCommandRuntime.makeAlterUserExecutionPlan
import org.neo4j.cypher.internal.AdministrationCommandRuntime.runtimeStringValue
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.ast.RemoveAuth
import org.neo4j.cypher.internal.logical.plans.AlterUser
import org.neo4j.cypher.internal.procs.PredicateExecutionPlan
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.server.security.systemgraph.UserSecurityGraphComponent

case class AlterUserExecutionPlanner(
  normalExecutionEngine: ExecutionEngine,
  securityAuthorizationHandler: SecurityAuthorizationHandler,
  userSecurityGraphComponent: UserSecurityGraphComponent,
  config: Config
) {

  def planAlterUser(alterUser: AlterUser, sourcePlan: Option[ExecutionPlan]): ExecutionPlan = {
    def failWithError(commands: String*): PredicateExecutionPlan = {
      val commandString = commands.mkString(", ")
      val verb = if (commands.size == 1) "is" else "are"
      new PredicateExecutionPlan(
        (_, _) => false,
        sourcePlan,
        (params, _, _) => {
          val user = runtimeStringValue(alterUser.userName, params)
          throw new CantCompileQueryException(
            s"Failed to alter the specified user '$user': $commandString $verb not available in community edition."
          )
        }
      )
    }

    if (alterUser.suspended.isDefined) { // Users are always active in community
      failWithError("'SET STATUS'")
    } else if (alterUser.defaultDatabase.isDefined) {
      failWithError("'HOME DATABASE'")
    } else if (alterUser.externalAuths.nonEmpty) { // There is no external auth in community
      val disallowedAuths = alterUser.externalAuths.map(e => s"`SET AUTH '${e.provider}'`")
      failWithError(disallowedAuths: _*)
    } else if (!alterUser.removeAuth.isEmpty) { // Not allowed to remove auth in community
      failWithError("`REMOVE AUTH`")
    } else {
      makeAlterUserExecutionPlan(
        userName = alterUser.userName,
        suspended = None,
        homeDatabase = None,
        nativeAuth = alterUser.nativeAuth,
        externalAuths = Seq.empty,
        removeAuths = RemoveAuth(all = false, List.empty)
      )(sourcePlan, normalExecutionEngine, securityAuthorizationHandler, userSecurityGraphComponent, config)
    }
  }

}
