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

import org.neo4j.cypher.internal.AdministrationCommandRuntime.authIdPropKey
import org.neo4j.cypher.internal.AdministrationCommandRuntime.authProviderPropKey
import org.neo4j.cypher.internal.AdministrationCommandRuntime.authRelType
import org.neo4j.cypher.internal.AdministrationCommandRuntime.internalKey
import org.neo4j.cypher.internal.AdministrationCommandRuntime.userLabel
import org.neo4j.cypher.internal.AdministrationCommandRuntime.userNamePropKey
import org.neo4j.cypher.internal.AdministrationCommandRuntime.userPwChangeReqPropKey
import org.neo4j.cypher.internal.AdministrationShowCommandUtils
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.administration.ShowUsersExecutionPlanner.getAuthCypher
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.procs.ParameterTransformer
import org.neo4j.cypher.internal.procs.SystemCommandExecutionPlan
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.server.security.systemgraph.SecurityGraphHelper.NATIVE_AUTH
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

case class ShowUsersExecutionPlanner(
  normalExecutionEngine: ExecutionEngine,
  securityAuthorizationHandler: SecurityAuthorizationHandler
) {

  def planShowUsers(
    symbols: List[String],
    withAuth: Boolean,
    yields: Option[Yield],
    returns: Option[Return],
    sourcePlan: Option[ExecutionPlan]
  ): ExecutionPlan = {

    // Community should only have native auth,
    // but if for some reason there is external auth it might be nice to show regardless
    val (authMatch, authColumns) = if (withAuth) getAuthCypher("u") else ("", "")

    SystemCommandExecutionPlan(
      "ShowUsers",
      normalExecutionEngine,
      securityAuthorizationHandler,
      s"""MATCH (u:$userLabel)
         |$authMatch
         |WITH u.$userNamePropKey as user, null as roles, u.$userPwChangeReqPropKey AS passwordChangeRequired, null as suspended, null as home
         |$authColumns
         |${AdministrationShowCommandUtils.generateReturnClause(symbols, yields, returns, Seq("user"))}
         |""".stripMargin,
      VirtualValues.EMPTY_MAP,
      source = sourcePlan
    )
  }

  def planShowCurrentUser(symbols: List[String], yields: Option[Yield], returns: Option[Return]): ExecutionPlan = {
    val currentUserKey = internalKey("currentUser")
    SystemCommandExecutionPlan(
      "ShowCurrentUser",
      normalExecutionEngine,
      securityAuthorizationHandler,
      s"""MATCH (u:$userLabel)
         |WITH u.$userNamePropKey as user, null as roles, u.$userPwChangeReqPropKey AS passwordChangeRequired, null as suspended, null as home
         |WHERE user = $$`$currentUserKey`
         |${AdministrationShowCommandUtils.generateReturnClause(symbols, yields, returns, Seq("user"))}
         |""".stripMargin,
      VirtualValues.EMPTY_MAP,
      parameterTransformer = ParameterTransformer((_, securityContext, _) =>
        VirtualValues.map(
          Array(currentUserKey),
          Array(Values.utf8Value(securityContext.subject().executingUser()))
        )
      )
    )
  }
}

object ShowUsersExecutionPlanner {

  /** Get auth Cypher strings for show users, both match statement and additional return columns
   *
   * @param userVariable the user node variable
   * @return return tuple of strings, the first one is the match statement and the second is the return columns (starting with ,)
   */
  def getAuthCypher(userVariable: String): (String, String) = {
    val authProviderExpression =
      s"""CASE
         | WHEN auth.$authProviderPropKey IS NULL THEN
         |  CASE
         |   WHEN $userVariable.$userPwChangeReqPropKey IS NULL THEN null
         |   ELSE '$NATIVE_AUTH'
         |  END
         | ELSE auth.$authProviderPropKey
         |END""".stripMargin
    val authExpression =
      s"""CASE
         | WHEN auth.$authProviderPropKey IS NULL THEN
         |  CASE
         |   WHEN $userVariable.$userPwChangeReqPropKey IS NULL THEN null
         |   ELSE {password: '***', changeRequired: $userVariable.$userPwChangeReqPropKey}
         |  END
         | WHEN auth.$authProviderPropKey = '$NATIVE_AUTH' THEN {password: '***', changeRequired: $userVariable.$userPwChangeReqPropKey}
         | ELSE {id: auth.$authIdPropKey}
         |END""".stripMargin

    (
      s"""OPTIONAL MATCH ($userVariable)-[:$authRelType]->(auth)
         |WITH *, $authProviderExpression AS provider, $authExpression AS auth""".stripMargin,
      ", provider, auth"
    )
  }
}
