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

import org.neo4j.cypher.internal.AdministrationCommandRuntime.internalKey
import org.neo4j.cypher.internal.AdministrationCommandRuntimeContext
import org.neo4j.cypher.internal.AdministrationShowCommandUtils
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.procs.ParameterTransformer
import org.neo4j.cypher.internal.procs.SystemCommandExecutionPlan
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_CREDENTIALS_EXPIRED_PROPERTY
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_NAME_PROPERTY
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_TAGS_PROPERTY
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

case class ShowUsersExecutionPlanner(
  normalExecutionEngine: ExecutionEngine,
  securityAuthorizationHandler: SecurityAuthorizationHandler
) {

  def planShowCurrentUser(
    symbols: List[LogicalVariable],
    yields: Option[Yield],
    returns: Option[Return],
    context: AdministrationCommandRuntimeContext
  ): ExecutionPlan = {
    val currentUserKey = internalKey("currentUser")
    SystemCommandExecutionPlan(
      "ShowCurrentUser",
      normalExecutionEngine,
      securityAuthorizationHandler,
      s"""MATCH (u:$USER)
         |WITH u.$USER_NAME_PROPERTY as user, null as roles, u.$USER_CREDENTIALS_EXPIRED_PROPERTY AS passwordChangeRequired, null as suspended, null as home, [t IN coalesce(u.$USER_TAGS_PROPERTY, []) | t] as tags
         |WHERE user = $$`$currentUserKey`
         |${AdministrationShowCommandUtils.generateReturnClause(symbols, yields, returns, Seq("user"))}
         |""".stripMargin,
      VirtualValues.EMPTY_MAP,
      parameterTransformer = ParameterTransformer((_, securityContext, _) =>
        VirtualValues.map(
          Array(currentUserKey),
          Array(Values.utf8Value(securityContext.subject().executingUser()))
        )
      ),
      cypherVersion = context.runtimeContext.cypherVersion
    )
  }
}

object ShowUsersExecutionPlanner {

  def getTagsColumnCypher(userVariable: String, allowedToSeeTagsKey: String): String =
    s"""CASE $$`$allowedToSeeTagsKey`
       |  WHEN true THEN [t IN coalesce($userVariable.$USER_TAGS_PROPERTY, []) | t]
       |  ELSE null
       |END AS tags""".stripMargin
}
