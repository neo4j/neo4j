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
package org.neo4j.cypher.internal.procs

import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.ast.AdministrationAction
import org.neo4j.cypher.internal.procs.AuthorizationAndPredicateExecutionPlan.buildMessage
import org.neo4j.cypher.internal.procs.AuthorizationAndPredicateExecutionPlan.checkToPredicate
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED
import org.neo4j.internal.kernel.api.security.PermissionState
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.values.virtual.MapValue

case class AuthorizationAndPredicateExecutionPlan(
  securityAuthorizationHandler: SecurityAuthorizationHandler,
  check: (MapValue, TransactionalContext, SecurityContext) => Seq[(AdministrationAction, PermissionState)],
  source: Option[ExecutionPlan],
  violationMessage: (PermissionState, Seq[AdministrationAction]) => String
) extends PredicateExecutionPlan(
      DatabaseSecurityPredicate(checkToPredicate(_, _, _, check)),
      source,
      buildMessage(securityAuthorizationHandler, _, _, _, check, violationMessage)
    )

object AuthorizationAndPredicateExecutionPlan {

  def apply(
    securityAuthorizationHandler: SecurityAuthorizationHandler,
    check: (MapValue, SecurityContext) => Seq[(AdministrationAction, PermissionState)],
    source: Option[ExecutionPlan] = None,
    violationMessage: (PermissionState, Seq[AdministrationAction]) => String = (_, _) => PERMISSION_DENIED
  ): AuthorizationAndPredicateExecutionPlan = {
    AuthorizationAndPredicateExecutionPlan(
      securityAuthorizationHandler,
      (params: MapValue, _: TransactionalContext, sc: SecurityContext) => check(params, sc),
      source,
      (permissionState: PermissionState, actions: Seq[AdministrationAction]) =>
        violationMessage(permissionState, actions)
    )
  }

  private def buildMessage(
    securityAuthorizationHandler: SecurityAuthorizationHandler,
    params: MapValue,
    transaction: TransactionalContext,
    securityContext: SecurityContext,
    check: (MapValue, TransactionalContext, SecurityContext) => Seq[(AdministrationAction, PermissionState)],
    messageGenerator: (PermissionState, Seq[AdministrationAction]) => String
  ): Exception = {
    val errorMsgs = check(params, transaction, securityContext)
      .groupBy(_._2)
      .view
      .filterKeys(state => state != PermissionState.EXPLICIT_GRANT).map { case (state, actions) =>
        messageGenerator(state, actions.map(_._1))
      }.mkString("; ")
    securityAuthorizationHandler.logAndGetAuthorizationException(securityContext, errorMsgs)
    throw AuthorizationViolationException.authorizationViolation(errorMsgs)
  }

  private def checkToPredicate(
    params: MapValue,
    tx: TransactionalContext,
    sc: SecurityContext,
    check: (MapValue, TransactionalContext, SecurityContext) => Seq[(AdministrationAction, PermissionState)]
  ): Boolean = {
    check.apply(params, tx, sc).forall { case (_, state) => state == PermissionState.EXPLICIT_GRANT }
  }
}
