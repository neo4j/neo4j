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

import org.neo4j.cypher.internal.AdministrationCommandRuntime.DatabaseNameFields
import org.neo4j.cypher.internal.AdministrationCommandRuntime.IdentityConverter
import org.neo4j.cypher.internal.AdministrationCommandRuntime.checkNamespaceExists
import org.neo4j.cypher.internal.AdministrationCommandRuntime.getDatabaseNameFields
import org.neo4j.cypher.internal.AdministrationShowCommandUtils
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.ParameterName
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.procs.SystemCommandExecutionPlan
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.ALIAS_PROPERTIES
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.CONNECTS_WITH
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DISPLAY_NAME_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DRIVER_SETTINGS
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.NAMESPACE_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.NAME_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.PRIMARY_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.PROPERTIES
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.REMOTE_DATABASE
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.TARGETS
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.TARGET_NAME_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.URL_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.USERNAME_PROPERTY
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.values.virtual.VirtualValues

case class ShowAliasesExecutionPlanner(
  normalExecutionEngine: ExecutionEngine,
  securityAuthorizationHandler: SecurityAuthorizationHandler
) {

  def planShowAliasesForDatabase(
    sourcePlan: Option[ExecutionPlan],
    aliasName: Option[DatabaseName],
    verbose: Boolean,
    symbols: List[String],
    yields: Option[Yield],
    returns: Option[Return]
  ): ExecutionPlan = {
    // name | database | location | url | driver
    val returnStatement = AdministrationShowCommandUtils.generateReturnClause(symbols, yields, returns, Seq("name"))
    val verboseColumns = if (verbose) ", driverSettings{.*} as driver, properties{.*} as properties" else ""
    val (aliasNameFields, aliasPropertyFilter) = filterAliasByName(aliasName)

    val query =
      s"""MATCH (alias:$DATABASE_NAME)
         |$aliasPropertyFilter
         |OPTIONAL MATCH (alias)-[:$TARGETS]->(localDatabase:$DATABASE)<-[:$TARGETS]-(localPrimary:$DATABASE_NAME {$PRIMARY_PROPERTY: true})
         |OPTIONAL MATCH (alias)-[:$CONNECTS_WITH]->(driverSettings:$DRIVER_SETTINGS)
         |OPTIONAL MATCH (alias)-[:$PROPERTIES]->(properties:$ALIAS_PROPERTIES)
         |WITH alias.$DISPLAY_NAME_PROPERTY as name,
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
    SystemCommandExecutionPlan(
      "ShowAliasesForDatabase",
      normalExecutionEngine,
      securityAuthorizationHandler,
      query,
      aliasNameFields.map(anf => VirtualValues.map(anf.keys, anf.values)).getOrElse(VirtualValues.EMPTY_MAP),
      source = sourcePlan,
      parameterConverter = aliasNameFields.map(_.nameConverter).getOrElse(IdentityConverter),
      parameterValidator = aliasNameFields.map(checkNamespaceExists).getOrElse((_, p) => (p, Set.empty))
    )
  }

  private def filterAliasByName(aliasName: Option[DatabaseName]): (Option[DatabaseNameFields], String) = {
    val aliasNameFields =
      aliasName.map((name: DatabaseName) =>
        getDatabaseNameFields("aliasName", name, new NormalizedDatabaseName(_).name())
      )

    // If we have a literal, we know from the escaping whether this is an alias in a composite or not
    // If it is a parameter, it could be either something that should be escaped, or an alias in a composite (or both)
    def filter(anf: DatabaseNameFields) = aliasName match {
      case Some(NamespacedName(_, _)) =>
        s"""AND alias.$NAME_PROPERTY = $$`${anf.nameKey}` AND alias.$NAMESPACE_PROPERTY = $$`${anf.namespaceKey}`"""
      case Some(ParameterName(_)) => s"AND alias.$DISPLAY_NAME_PROPERTY = $$`${anf.displayNameKey}`"
      case None                   => s""
    }
    val aliasPropertyFilter = aliasNameFields
      .map(anf =>
        s"""
           | WHERE alias.$PRIMARY_PROPERTY = false ${filter(anf)}
           |""".stripMargin
      )
      .getOrElse(s"WHERE alias.$PRIMARY_PROPERTY = false")
    (aliasNameFields, aliasPropertyFilter)
  }

}
