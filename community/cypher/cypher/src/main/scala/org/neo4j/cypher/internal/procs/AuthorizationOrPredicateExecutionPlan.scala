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
package org.neo4j.cypher.internal.procs

import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.ast.AdministrationAction
import org.neo4j.cypher.internal.procs.AuthorizationOrPredicateExecutionPlan.buildMessage
import org.neo4j.cypher.internal.procs.AuthorizationOrPredicateExecutionPlan.checkToPredicate
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED
import org.neo4j.internal.kernel.api.security.PermissionState
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.values.virtual.MapValue

case class AuthorizationOrPredicateExecutionPlan(
  securityAuthorizationHandler: SecurityAuthorizationHandler,
  check: (MapValue, SecurityContext) => Seq[(AdministrationAction, PermissionState)],
  source: Option[ExecutionPlan] = None,
  violationMessage: (PermissionState, Seq[AdministrationAction]) => String = (_, _) => PERMISSION_DENIED
) extends PredicateExecutionPlan(
      checkToPredicate(_, _, check),
      source,
      buildMessage(securityAuthorizationHandler, _, _, check, violationMessage)
    )

object AuthorizationOrPredicateExecutionPlan {

  private def buildMessage(
    securityAuthorizationHandler: SecurityAuthorizationHandler,
    params: MapValue,
    securityContext: SecurityContext,
    check: (MapValue, SecurityContext) => Seq[(AdministrationAction, PermissionState)],
    messageGenerator: (PermissionState, Seq[AdministrationAction]) => String
  ): Exception = {

    val permissionStates = check.apply(params, securityContext)
    val errorStates = permissionStates.groupBy(_._2).filterKeys(state => state != PermissionState.EXPLICIT_GRANT)

    // In case an action requires privileges A OR B and the check returns EXPLICIT_DENY for A and NOT_GRANTED for B,
    // we only want to warn about the EXPLICIT_DENY since we do not have enough information to know whether there is also a GRANT on A,
    // in which case the missing GRANT on B is not relevant.
    val filteredErrorStates =
      if (errorStates.exists(state => state._1 == PermissionState.EXPLICIT_DENY)) {
        errorStates.filterKeys(state => state != PermissionState.NOT_GRANTED)
      } else {
        errorStates
      }

    val errorMsgs = filteredErrorStates.map { case (state, actions) =>
      messageGenerator(state, actions.map(_._1))
    }.mkString("; ")
    securityAuthorizationHandler.logAndGetAuthorizationException(securityContext, errorMsgs)
    throw new AuthorizationViolationException(errorMsgs)
  }

  private def checkToPredicate(
    params: MapValue,
    sc: SecurityContext,
    check: (MapValue, SecurityContext) => Seq[(AdministrationAction, PermissionState)]
  ): Boolean = {
    val permissionStates = check.apply(params, sc)

    // At least one of the privileges must be granted and none denied
    permissionStates.exists { case (_, state) => state == PermissionState.EXPLICIT_GRANT } &&
    !permissionStates.exists { case (_, state) => state == PermissionState.EXPLICIT_DENY }
  }
}
