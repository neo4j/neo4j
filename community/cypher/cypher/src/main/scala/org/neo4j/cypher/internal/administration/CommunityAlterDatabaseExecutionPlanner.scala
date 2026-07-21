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

import org.neo4j.cypher.internal.AdministrationCommandRuntime.checkNamespaceExists
import org.neo4j.cypher.internal.AdministrationCommandRuntime.getDatabaseNameFields
import org.neo4j.cypher.internal.AdministrationCommandRuntime.getParameterName
import org.neo4j.cypher.internal.AdministrationCommandRuntime.internalKey
import org.neo4j.cypher.internal.AdministrationCommandRuntime.runtimeStringValue
import org.neo4j.cypher.internal.AdministrationCommandRuntimeContext
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.procs.Continue
import org.neo4j.cypher.internal.procs.ParameterTransformer
import org.neo4j.cypher.internal.procs.QueryHandler
import org.neo4j.cypher.internal.procs.ThrowException
import org.neo4j.cypher.internal.procs.UpdatingSystemCommandExecutionPlan
import org.neo4j.dbms.api.DatabaseNotFoundHelper
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModelConstants.DATABASE
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModelConstants.DATABASE_DEFAULT_LANGUAGE_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModelConstants.DATABASE_NAME
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModelConstants.DATABASE_NAME_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModelConstants.DATABASE_UPDATED_AT_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModelConstants.TARGETS
import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.exceptions.DatabaseAdministrationOnFollowerException
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.exceptions.Status.HasStatus
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

case class CommunityAlterDatabaseExecutionPlanner(
  normalExecutionEngine: ExecutionEngine,
  securityAuthorizationHandler: SecurityAuthorizationHandler
) {

  def planAlterDatabase(
    databaseName: DatabaseName,
    defaultLanguageVersion: CypherVersion,
    sourcePlan: Option[ExecutionPlan],
    context: AdministrationCommandRuntimeContext
  ): ExecutionPlan = {
    val defaultLanguageKey = internalKey("defaultLanguage")
    val defaultLanguageValue = Values.utf8Value(defaultLanguageVersion.persistedValue)
    val nameFields =
      getDatabaseNameFields("databaseName", databaseName)
    val parameterTransformer = ParameterTransformer()
      .convert(nameFields.nameConverter)
      .validate(checkNamespaceExists(nameFields, context))
    UpdatingSystemCommandExecutionPlan(
      "AlterDatabase",
      normalExecutionEngine,
      securityAuthorizationHandler,
      s"""CALL () {
         |  OPTIONAL MATCH (:$DATABASE_NAME ${nameFields.asNodeFilter(
          context.runtimeContext.cypherVersion
        )})-[:$TARGETS]->(aliasedDb:$DATABASE)
         |  RETURN aliasedDb as d
         |}
         |WITH d
         |SET d.$DATABASE_UPDATED_AT_PROPERTY = datetime()
         |SET d.$DATABASE_DEFAULT_LANGUAGE_PROPERTY = $$`$defaultLanguageKey`
         |RETURN d.$DATABASE_NAME_PROPERTY as dbName
        """.stripMargin,
      VirtualValues.map(
        nameFields.keys ++ Array(defaultLanguageKey),
        nameFields.values ++ Array(
          defaultLanguageValue
        )
      ),
      QueryHandler
        .handleResult((offset, value, params) => {
          if (offset == 0 && (value eq Values.NO_VALUE)) {
            ThrowException(DatabaseNotFoundHelper.failedAction(
              "alter",
              runtimeStringValue(databaseName, params),
              getParameterName(databaseName.asLegacyName).orNull
            ))
          } else {
            Continue
          }
        })
        .handleError {
          case (error: HasStatus, params) if error.status() == Status.Cluster.NotALeader =>
            DatabaseAdministrationOnFollowerException.notALeader(
              "ALTER DATABASE",
              s"Failed to alter the specified database '${runtimeStringValue(databaseName, params)}'",
              error
            )
          case (error, params) =>
            CypherExecutionException.failedToAlterDb(runtimeStringValue(databaseName, params), error)
        },
      source = sourcePlan,
      parameterTransformer =
        parameterTransformer
    )
  }
}
