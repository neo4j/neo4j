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

import org.neo4j.cypher.internal.AdministrationCommandRuntime.DatabaseNameFields
import org.neo4j.cypher.internal.AdministrationCommandRuntime.IdentityConverter
import org.neo4j.cypher.internal.AdministrationCommandRuntime.checkNamespaceExists
import org.neo4j.cypher.internal.AdministrationCommandRuntime.getDatabaseNameFields
import org.neo4j.cypher.internal.AdministrationCommandRuntime.internalKey
import org.neo4j.cypher.internal.AdministrationCommandRuntime.translateDefaultLanguagePropertyToShowOutput
import org.neo4j.cypher.internal.AdministrationCommandRuntimeContext
import org.neo4j.cypher.internal.AdministrationShowCommandUtils
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.administration.ShowAliasesExecutionPlanner.Alias
import org.neo4j.cypher.internal.administration.ShowAliasesExecutionPlanner.LOCAL
import org.neo4j.cypher.internal.administration.ShowAliasesExecutionPlanner.REMOTE
import org.neo4j.cypher.internal.administration.ShowAliasesExecutionPlanner.aliasTargetParameter
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.ParameterName
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ShowAliases.OIDC_CREDENTIAL_FORWARDING
import org.neo4j.cypher.internal.ast.ShowAliases.STORED_NATIVE_CREDENTIALS
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.procs.ParameterTransformer
import org.neo4j.cypher.internal.procs.SystemCommandExecutionPlan
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModelConstants.ALIAS_PROPERTIES
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModelConstants.CONNECTS_WITH
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModelConstants.DATABASE_NAME
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModelConstants.DEFAULT_NAMESPACE
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModelConstants.DISPLAY_NAME_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModelConstants.DRIVER_SETTINGS
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModelConstants.GRAPH_SHARD
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModelConstants.NAMESPACE_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModelConstants.NAME_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModelConstants.OIDC_CREDENTIAL_FORWARDING_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModelConstants.PROPERTIES
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModelConstants.REMOTE_USERNAME_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModelConstants.URL_PROPERTY
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.kernel.database.DatabaseReferenceRepository
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.jdk.OptionConverters.RichOptional

case class ShowAliasesExecutionPlanner(
  normalExecutionEngine: ExecutionEngine,
  securityAuthorizationHandler: SecurityAuthorizationHandler,
  referenceResolver: DatabaseReferenceRepository
) {

  def planShowAliasesForDatabase(
    sourcePlan: Option[ExecutionPlan],
    aliasName: Option[DatabaseName],
    verbose: Boolean,
    symbols: List[LogicalVariable],
    yields: Option[Yield],
    returns: Option[Return],
    context: AdministrationCommandRuntimeContext
  ): ExecutionPlan = {
    // name | composite | database | location | url | user | driver | defaultLanguage | properties
    val returnStatement = AdministrationShowCommandUtils.generateReturnClause(symbols, yields, returns, Seq("name"))
    val defaultLanguage = translateDefaultLanguagePropertyToShowOutput("aliasNode")
    val verboseColumns =
      if (verbose)
        s", driverSettings{.*} as driver, properties{.*} as properties, $defaultLanguage as defaultLanguage"
      else ""
    val (aliasNameFields, aliasPropertyFilter) = filterAliasByName(aliasName, context.runtimeContext.cypherVersion)

    val aliasNameNodeFilter = context.runtimeContext.cypherVersion match {
      case CypherVersion.Cypher5 =>
        s"{$NAME_PROPERTY: alias.name, $NAMESPACE_PROPERTY: alias.namespace}"
      case _ => s"{$DISPLAY_NAME_PROPERTY: alias.displayName}"
    }

    val query =
      s"""UNWIND $$$aliasTargetParameter AS alias
         |WITH alias $aliasPropertyFilter
         |MATCH (aliasNode:$DATABASE_NAME&!$GRAPH_SHARD $aliasNameNodeFilter)
         |OPTIONAL MATCH (aliasNode)-[:$CONNECTS_WITH]->(driverSettings:$DRIVER_SETTINGS)
         |OPTIONAL MATCH (aliasNode)-[:$PROPERTIES]->(properties:$ALIAS_PROPERTIES)
         |WITH alias.displayName as name,
         |alias.composite as composite,
         |alias.database as database,
         |alias.location as location,
         |aliasNode.$URL_PROPERTY as url,
         |CASE
         |  WHEN aliasNode.$OIDC_CREDENTIAL_FORWARDING_PROPERTY THEN '$OIDC_CREDENTIAL_FORWARDING'
         |  WHEN aliasNode.$REMOTE_USERNAME_PROPERTY IS NOT NULL THEN '$STORED_NATIVE_CREDENTIALS'
         |  ELSE null
         |END as credentials,
         |aliasNode.$REMOTE_USERNAME_PROPERTY as user
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
      parameterTransformer =
        ParameterTransformer((_, sc, _) => generateVisibleAliases(sc)).convert(
          aliasNameFields.map(_.nameConverter).getOrElse(IdentityConverter)
        )
          .validate(aliasNameFields.map(aliasNameFields =>
            checkNamespaceExists(aliasNameFields, context)(_, _)
          ).getOrElse((_, p) => (p, Set.empty))),
      cypherVersion = context.runtimeContext.cypherVersion
    )
  }

  private def filterAliasByName(
    aliasName: Option[DatabaseName],
    cypherVersion: CypherVersion
  ): (Option[DatabaseNameFields], String) = {
    val aliasNameFields =
      aliasName.map((name: DatabaseName) =>
        getDatabaseNameFields("aliasName", name)
      )

    // If we have a literal, we know from the escaping whether this is an alias in a composite or not
    // If it is a parameter, it could be either something that should be escaped, or an alias in a composite (or both)
    def filter(anf: DatabaseNameFields) = cypherVersion match {
      case CypherVersion.Cypher5 => aliasName match {
          case Some(NamespacedName(_, _)) =>
            s"""WHERE alias.$NAME_PROPERTY = $$`${anf.nameKey}` AND alias.$NAMESPACE_PROPERTY = $$`${anf.namespaceKey}`"""
          case Some(ParameterName(_)) => s"WHERE alias.$DISPLAY_NAME_PROPERTY = $$`${anf.displayNameKey}`"
          case None                   => ""
        }
      case _ => s"WHERE alias.$DISPLAY_NAME_PROPERTY = $$`${anf.displayNameKey}`"
    }
    val aliasPropertyFilter = aliasNameFields
      .map(anf => filter(anf)).getOrElse("")
    (aliasNameFields, aliasPropertyFilter)
  }

  private def generateVisibleAliases(sc: SecurityContext): MapValue = {

    def referencesToAlias(alias: DatabaseReference): Iterable[Alias] = alias match {
      case a: DatabaseReferenceImpl.External => Seq(Alias(
          a.alias().name(),
          a.namespace().toScala.map(_.name()).getOrElse(DEFAULT_NAMESPACE),
          Some(a.targetAlias().name()),
          REMOTE
        ))
      case c: DatabaseReferenceImpl.Composite => c.constituents().asScala.flatMap(referencesToAlias)
      case a: DatabaseReferenceImpl.Internal if !a.isPrimary =>
        val primary = referenceResolver.getByAlias(a.namedDatabaseId().name()).toScala.collect {
          case ref if sc.databaseAccessMode().canSeeDatabase(ref) => ref.alias().name()
        }
        Seq(Alias(a.alias().name(), a.namespace().toScala.map(_.name()).getOrElse(DEFAULT_NAMESPACE), primary, LOCAL))
      case _ => Seq.empty
    }

    val namedDatabasesAndAliases: List[AnyValue] = referenceResolver.getAllDatabaseReferences.asScala
      .flatMap(a => referencesToAlias(a).map(_.asMapValue)).toList
    VirtualValues.map(
      Array(aliasTargetParameter),
      Array(VirtualValues.fromList(namedDatabasesAndAliases.asJava))
    )
  }
}

object ShowAliasesExecutionPlanner {
  private val aliasTargetParameter = internalKey("aliasTargets")
  private val LOCAL = "local"
  private val REMOTE = "remote"

  private case class Alias(name: String, namespace: String, database: Option[String], location: String) {
    private val displayName = if (namespace == DEFAULT_NAMESPACE) name else s"$namespace.$name"

    def asMapValue: MapValue = {
      val composite = if (namespace == DEFAULT_NAMESPACE) None else Some(namespace)
      VirtualValues.map(
        Array("name", "namespace", "composite", "database", "displayName", "location"),
        Array(
          Values.stringValue(name),
          Values.stringValue(namespace),
          composite.map(Values.stringValue).getOrElse(Values.NO_VALUE),
          database.map(Values.stringValue).getOrElse(Values.NO_VALUE),
          Values.stringValue(displayName),
          Values.stringValue(location)
        )
      )
    }
  }
}
