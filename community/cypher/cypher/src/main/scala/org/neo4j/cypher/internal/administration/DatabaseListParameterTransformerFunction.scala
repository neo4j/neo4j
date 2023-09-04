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
import org.neo4j.cypher.internal.administration.DatabaseListParameterTransformerFunction.detailLevels
import org.neo4j.cypher.internal.administration.ShowDatabaseExecutionPlanner.accessibleDbsKey
import org.neo4j.cypher.internal.ast.DatabaseScope
import org.neo4j.cypher.internal.ast.DefaultDatabaseScope
import org.neo4j.cypher.internal.ast.HomeDatabaseScope
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.ParameterName
import org.neo4j.cypher.internal.ast.ShowDatabase.DATABASE_ID_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.LAST_COMMITTED_TX_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.REPLICATION_LAG_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.STORE_COL
import org.neo4j.cypher.internal.ast.SingleNamedDatabaseScope
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.procs.ParameterTransformer.ParameterTransformerOutput
import org.neo4j.cypher.internal.procs.ParameterTransformerFunction
import org.neo4j.cypher.internal.util.AssertionRunner
import org.neo4j.cypher.internal.util.DeprecatedDatabaseNameNotification
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.dbms.database.TopologyInfoService
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_DEFAULT_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_LABEL
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DEFAULT_NAMESPACE
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.NotFoundException
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.kernel.database.DatabaseReferenceRepository
import org.neo4j.kernel.database.DefaultDatabaseResolver
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.jdk.CollectionConverters.SetHasAsJava

class DatabaseListParameterTransformerFunction(
  referenceResolver: DatabaseReferenceRepository,
  defaultDatabaseResolver: DefaultDatabaseResolver,
  infoService: TopologyInfoService,
  maybeYield: Option[Yield],
  verbose: Boolean,
  scope: DatabaseScope
) extends ParameterTransformerFunction {

  override def transform(
    transaction: Transaction,
    securityContext: SecurityContext,
    systemParams: MapValue,
    userParams: MapValue
  ): ParameterTransformerOutput = {
    val allReferences = referenceResolver.getAllDatabaseReferences.asScala.toSet
    val (filteredReferences, notifications): (Set[DatabaseReference], Set[InternalNotification]) = scope match {
      case _: DefaultDatabaseScope =>
        try {
          val defaultDatabaseNode: Node = transaction.findNode(DATABASE_LABEL, DATABASE_DEFAULT_PROPERTY, true)
          if (defaultDatabaseNode != null) {
            (
              allReferences.filter(ref =>
                ref.isPrimary && ref.alias().name().equals(defaultDatabaseNode.getProperty(DATABASE_NAME_PROPERTY))
              ),
              Set.empty
            )
          } else {
            (Set.empty, Set.empty)
          }
        } catch {
          case _: NotFoundException => (Set.empty, Set.empty)
        }
      case _: HomeDatabaseScope =>
        val homeDatabase = defaultDatabaseResolver.defaultDatabase(securityContext.subject().executingUser())
        (allReferences.filter(ref => ref.isPrimary && ref.alias().name().equals(homeDatabase)), Set.empty)
      case namedDatabaseScope: SingleNamedDatabaseScope =>
        filterReferencesByName(allReferences, namedDatabaseScope, userParams)
      case _ =>
        (allReferences, Set.empty)
    }

    val accessibleDatabases = filteredReferences
      .collect {
        case db if db.isPrimary && securityContext.databaseAccessMode().canSeeDatabase(db) =>
          DatabaseIdFactory.from(db.alias().name(), db.id())
      }

    val dbMetadata = {
      val dbInfos = infoService.databases(transaction, accessibleDatabases.asJava, detailLevels(verbose, maybeYield))
      dbInfos.asScala.map(info => DatabaseDetailsMapper.toMapValue(info)).toList.asJava
    }

    (
      safeMergeParameters(
        systemParams,
        userParams,
        VirtualValues.map(
          Array(accessibleDbsKey),
          Array(VirtualValues.fromList(dbMetadata))
        ).updatedWith(generateUsernameParameter(securityContext))
      ),
      notifications
    )
  }

  private def filterReferencesByName(
    databaseReferences: Set[DatabaseReference],
    namedDatabaseScope: SingleNamedDatabaseScope,
    params: MapValue
  ): (Set[DatabaseReference], Set[InternalNotification]) = {
    val (name, namespace, notifications)
      : (NormalizedDatabaseName, Option[NormalizedDatabaseName], Set[InternalNotification]) =
      namedDatabaseScope.database match {
        case nn @ NamespacedName(_, namespace) =>
          val normalizedNamespace = namespace.map(new NormalizedDatabaseName(_))
          normalizedNamespace match {
            case None => (new NormalizedDatabaseName(nn.name), None, Set.empty[InternalNotification])
            case Some(ns) =>
              val deprecatedName = ns.name() + "." + nn.name
              databaseReferences.find(dr => dr.isComposite && dr.alias().equals(ns))
                .map(_ => (new NormalizedDatabaseName(nn.name), normalizedNamespace, Set.empty[InternalNotification]))
                // This is the deprecated case of "SHOW DATABASE a.b" with no composite. Should really be `a.b`, so warn
                .getOrElse((
                  new NormalizedDatabaseName(deprecatedName),
                  None,
                  Set(DeprecatedDatabaseNameNotification(deprecatedName, None))
                ))
          }
        case pn: ParameterName =>
          val (namespace, name, _) = pn.getNameParts(params, DEFAULT_NAMESPACE)
          val normalizedNamespace = namespace.map(new NormalizedDatabaseName(_))
          normalizedNamespace match {
            case None => (new NormalizedDatabaseName(name), None, Set.empty[InternalNotification])
            case Some(ns) =>
              databaseReferences.find(dr => dr.isComposite && dr.alias().equals(ns))
                .map(_ => (new NormalizedDatabaseName(name), normalizedNamespace, Set.empty[InternalNotification]))
                .getOrElse((new NormalizedDatabaseName(ns.name() + "." + name), None, Set.empty[InternalNotification]))
          }
      }
    val filteredReferences: Set[DatabaseReference] = namespace match {
      case None => databaseReferences.collect {
          case ref if ref.isPrimary && ref.alias().equals(name) => Set(ref)
          case ref: DatabaseReferenceImpl.Internal if ref.alias().equals(name) =>
            databaseReferences.filter(pr => pr.isPrimary && pr.id() == ref.id())
        }.flatten
      case Some(namespace) => databaseReferences.collect {
          case c: DatabaseReferenceImpl.Composite if c.alias().equals(namespace) =>
            val constituentAliases = c.constituents().asScala.filter(r => r.alias().equals(name))
            constituentAliases.flatMap(dr => databaseReferences.filter(_.id() == dr.id()))
        }.flatten
    }
    if (AssertionRunner.isAssertionsEnabled && filteredReferences.size > 1) {
      throw new IllegalStateException("SHOW DATABASE by name should only return 0 or 1 databases")
    }
    (filteredReferences, notifications)
  }

  private def generateUsernameParameter(securityContext: SecurityContext): MapValue = {
    val username = Option(securityContext.subject().executingUser()) match {
      case None       => Values.NO_VALUE
      case Some("")   => Values.NO_VALUE
      case Some(user) => Values.stringValue(user)
    }

    VirtualValues.map(
      Array(internalKey("username")),
      Array(username)
    )
  }
}

object DatabaseListParameterTransformerFunction {

  private val txCols = Set(
    LAST_COMMITTED_TX_COL,
    REPLICATION_LAG_COL
  )

  private val storeIdCols = Set(
    STORE_COL,
    DATABASE_ID_COL
  )

  private def detailLevels(verbose: Boolean, maybeYield: Option[Yield]): TopologyInfoService.RequestedExtras = {
    if (verbose && maybeYield.isDefined) {
      if (maybeYield.get.returnItems.includeExisting) {
        TopologyInfoService.RequestedExtras.ALL
      } else {
        val (lastTxSpecified, storeIdSpecified) =
          maybeYield.get.returnItems.items.map(_.expression).foldLeft((false, false))((acc, expr) => {
            expr match {
              case Variable(name) => (acc._1 || txCols.contains(name), acc._2 || storeIdCols.contains(name))
              case _              => acc
            }
          })
        new TopologyInfoService.RequestedExtras(lastTxSpecified, storeIdSpecified)
      }
    } else {
      TopologyInfoService.RequestedExtras.NONE
    }
  }
}
