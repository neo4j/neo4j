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

import org.neo4j.cypher.internal.AdministrationShowCommandUtils
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.procs.SystemCommandExecutionPlan
import org.neo4j.dbms.database.TopologyGraphDbmsModel.CONNECTS_WITH
import org.neo4j.dbms.database.TopologyGraphDbmsModel.DATABASE
import org.neo4j.dbms.database.TopologyGraphDbmsModel.DATABASE_NAME
import org.neo4j.dbms.database.TopologyGraphDbmsModel.DRIVER_SETTINGS
import org.neo4j.dbms.database.TopologyGraphDbmsModel.NAME_PROPERTY
import org.neo4j.dbms.database.TopologyGraphDbmsModel.PRIMARY_PROPERTY
import org.neo4j.dbms.database.TopologyGraphDbmsModel.REMOTE_DATABASE
import org.neo4j.dbms.database.TopologyGraphDbmsModel.TARGETS
import org.neo4j.dbms.database.TopologyGraphDbmsModel.TARGET_NAME_PROPERTY
import org.neo4j.dbms.database.TopologyGraphDbmsModel.URL_PROPERTY
import org.neo4j.dbms.database.TopologyGraphDbmsModel.USERNAME_PROPERTY
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.values.virtual.VirtualValues

case class ShowAliasesExecutionPlanner(normalExecutionEngine: ExecutionEngine, securityAuthorizationHandler: SecurityAuthorizationHandler) {

  def planShowAliasesForDatabase(sourcePlan: Option[ExecutionPlan], verbose: Boolean, symbols: List[String], yields: Option[Yield], returns: Option[Return]): ExecutionPlan = {
    // name | database | location | url | driver
    val returnStatement = AdministrationShowCommandUtils.generateReturnClause(symbols, yields, returns, Seq("name"))
    val verboseColumns = if (verbose) ", driverSettings{.*} as driver" else ""
    val query =
      s"""MATCH (alias:$DATABASE_NAME {$PRIMARY_PROPERTY: false})
         |OPTIONAL MATCH (alias)-[:$TARGETS]->(localDatabase:$DATABASE)<-[:$TARGETS]-(localPrimary:$DATABASE_NAME {$PRIMARY_PROPERTY: true})
         |OPTIONAL MATCH (alias)-[:$CONNECTS_WITH]->(driverSettings:$DRIVER_SETTINGS)
         |WITH
         |alias.$NAME_PROPERTY as name,
         |coalesce(localPrimary.$NAME_PROPERTY, alias.$TARGET_NAME_PROPERTY) as database,
         |CASE
         | WHEN "$REMOTE_DATABASE" in labels(alias) THEN "remote"
         | ELSE "local"
         |END AS location,
         |alias.$URL_PROPERTY as url,
         |alias.$USERNAME_PROPERTY as user
         |$verboseColumns
         |$returnStatement
         |""".stripMargin
    SystemCommandExecutionPlan("ShowAliasesForDatabase",
      normalExecutionEngine,
      securityAuthorizationHandler,
      query,
      VirtualValues.EMPTY_MAP,
      source = sourcePlan)
  }
}
