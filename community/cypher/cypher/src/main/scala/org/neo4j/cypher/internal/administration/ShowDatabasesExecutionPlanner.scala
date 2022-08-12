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

import org.neo4j.common.DependencyResolver
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.AdministrationCommandRuntime.IdentityConverter
import org.neo4j.cypher.internal.AdministrationCommandRuntime.checkNamespaceExists
import org.neo4j.cypher.internal.AdministrationCommandRuntime.getDatabaseNameFields
import org.neo4j.cypher.internal.AdministrationCommandRuntime.internalKey
import org.neo4j.cypher.internal.AdministrationShowCommandUtils
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.ast.DatabaseScope
import org.neo4j.cypher.internal.ast.DefaultDatabaseScope
import org.neo4j.cypher.internal.ast.HomeDatabaseScope
import org.neo4j.cypher.internal.ast.NamedDatabaseScope
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ShowDatabase.ACCESS_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.ADDRESS_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.ALIASES_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.CONSTITUENTS_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.CREATION_TIME_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.CURRENT_PRIMARIES_COUNT_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.CURRENT_SECONDARIES_COUNT_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.CURRENT_STATUS_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.DATABASE_ID_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.DEFAULT_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.HOME_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.LAST_COMMITTED_TX_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.LAST_START_TIME_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.LAST_STOP_TIME_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.NAME_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.REPLICATION_LAG_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.REQUESTED_PRIMARIES_COUNT_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.REQUESTED_SECONDARIES_COUNT_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.REQUESTED_STATUS_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.ROLE_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.SERVER_ID_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.STATUS_MSG_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.STORE_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.TYPE_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.WRITER_COL
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.procs.SystemCommandExecutionPlan
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.dbms.database.DatabaseInfo
import org.neo4j.dbms.database.DatabaseInfoService
import org.neo4j.dbms.database.ExtendedDatabaseInfo
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.COMPOSITE_DATABASE
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.COMPOSITE_DATABASE_LABEL
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_CREATED_AT_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_DEFAULT_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_LABEL
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME_LABEL
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_PRIMARIES_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_SECONDARIES_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_STARTED_AT_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_STATUS_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_STOPPED_AT_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_UUID_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_VIRTUAL_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DEFAULT_NAMESPACE
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DISPLAY_NAME_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DatabaseAccess
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.NAMESPACE_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.NAME_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.PRIMARY_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.TARGETS
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.TARGETS_RELATIONSHIP
import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType.withName
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.helpers.collection.Iterables
import org.neo4j.internal.kernel.api.security.AdminActionOnResource
import org.neo4j.internal.kernel.api.security.PrivilegeAction
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.internal.kernel.api.security.Segment
import org.neo4j.kernel.database.DatabaseId
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.kernel.database.DefaultDatabaseResolver
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.storageengine.api.StoreIdProvider
import org.neo4j.storageengine.util.StoreIdDecodeUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

import java.util.UUID

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.jdk.CollectionConverters.SetHasAsJava
import scala.jdk.OptionConverters.RichOptional
import scala.util.Try
import scala.util.Using

case class ShowDatabasesExecutionPlanner(
  resolver: DependencyResolver,
  defaultDatabaseResolver: DefaultDatabaseResolver,
  normalExecutionEngine: ExecutionEngine,
  securityAuthorizationHandler: SecurityAuthorizationHandler
)(implicit extendedDatabaseInfoMapper: DatabaseInfoMapper[ExtendedDatabaseInfo]) {

  private val accessibleDbsKey = internalKey("accessibleDbs")
  private val dbms = resolver.resolveDependency(classOf[DatabaseManagementService])
  private val infoService = resolver.resolveDependency(classOf[DatabaseInfoService])
  private val noOpValidator: (Transaction, MapValue) => (MapValue, Set[InternalNotification]) = (_, p) => (p, Set.empty)

  def planShowDatabases(
    scope: DatabaseScope,
    verbose: Boolean,
    symbols: List[String],
    yields: Option[Yield],
    returns: Option[Return]
  ): ExecutionPlan = {
    val usernameKey = internalKey("username")
    val paramGenerator: (Transaction, SecurityContext) => MapValue =
      (tx, securityContext) => generateShowAccessibleDatabasesParameter(tx, securityContext, yields, verbose)

    val (extraFilter, params, paramConverter, paramValidator) = scope match {
      // show default database
      case _: DefaultDatabaseScope =>
        (s"WHERE default = true", VirtualValues.EMPTY_MAP, IdentityConverter, noOpValidator)
      // show home database
      case _: HomeDatabaseScope => (s"WHERE home = true", VirtualValues.EMPTY_MAP, IdentityConverter, noOpValidator)
      // show database name
      case NamedDatabaseScope(p) =>
        val nameFields =
          getDatabaseNameFields("databaseName", p)
        (
          s"WHERE any(a in aliases WHERE $$`${nameFields.nameKey}` = a.$NAME_PROPERTY AND  $$`${nameFields.namespaceKey}` = a.$NAMESPACE_PROPERTY)",
          VirtualValues.map(nameFields.keys, nameFields.values),
          nameFields.nameConverter,
          checkNamespaceExists(nameFields)(_, _)
        )
      // show all databases
      case _ => ("", VirtualValues.EMPTY_MAP, IdentityConverter, noOpValidator)
    }

    val verboseColumns =
      if (verbose) {
        s""", props.$DATABASE_ID_COL as $DATABASE_ID_COL,
           |props.$CURRENT_PRIMARIES_COUNT_COL as $CURRENT_PRIMARIES_COUNT_COL,
           |props.$CURRENT_SECONDARIES_COUNT_COL as $CURRENT_SECONDARIES_COUNT_COL,
           |d.$DATABASE_PRIMARIES_PROPERTY as $REQUESTED_PRIMARIES_COUNT_COL,
           |d.$DATABASE_SECONDARIES_PROPERTY as $REQUESTED_SECONDARIES_COUNT_COL,
           |props.$LAST_COMMITTED_TX_COL as $LAST_COMMITTED_TX_COL,
           |props.$REPLICATION_LAG_COL as $REPLICATION_LAG_COL,
           |d.$DATABASE_CREATED_AT_PROPERTY as $CREATION_TIME_COL,
           |d.$DATABASE_STARTED_AT_PROPERTY as $LAST_START_TIME_COL,
           |d.$DATABASE_STOPPED_AT_PROPERTY as $LAST_STOP_TIME_COL,
           |props.$STORE_COL as $STORE_COL
           |""".stripMargin
      } else {
        ""
      }
    val verboseNames =
      if (verbose) {
        s", $DATABASE_ID_COL, $SERVER_ID_COL, $REQUESTED_PRIMARIES_COUNT_COL, $REQUESTED_SECONDARIES_COUNT_COL, $CURRENT_PRIMARIES_COUNT_COL, " +
          s"$CURRENT_SECONDARIES_COUNT_COL, $CREATION_TIME_COL, $LAST_START_TIME_COL, $LAST_STOP_TIME_COL, $STORE_COL, $LAST_COMMITTED_TX_COL, $REPLICATION_LAG_COL"
      } else {
        ""
      }
    val returnClause = AdministrationShowCommandUtils.generateReturnClause(symbols, yields, returns, Seq("name"))

    val query = Predef.augmentString(
      s"""// First resolve which database is the home database
           |OPTIONAL MATCH (default:$DATABASE_LABEL {$DATABASE_DEFAULT_PROPERTY: true})
           |OPTIONAL MATCH (user:User {$NAME_PROPERTY: $$`$usernameKey`})
           |WITH coalesce(user.homeDatabase, default.$DATABASE_NAME_PROPERTY) as homeDbName
           |
           |UNWIND $$`$accessibleDbsKey` AS props
           |MATCH (d:$DATABASE)<-[:$TARGETS]-(dn:$DATABASE_NAME {$NAME_PROPERTY: props.name, $NAMESPACE_PROPERTY: '$DEFAULT_NAMESPACE'})
           |WITH d, dn, props, homeDbName
           |OPTIONAL MATCH (d)<-[:$TARGETS]-(a:$DATABASE_NAME)
           |WITH a, d, dn, props, homeDbName ORDER BY a.$DISPLAY_NAME_PROPERTY
           |OPTIONAL MATCH (constituent:$DATABASE_NAME {$NAMESPACE_PROPERTY: dn.$NAME_PROPERTY})
           |WHERE d:$COMPOSITE_DATABASE AND constituent <> dn
           |WITH d.name as name,
           |collect(a) as aliases,
           |collect(constituent.$DISPLAY_NAME_PROPERTY) as constituents,
           |props.$ACCESS_COL as $ACCESS_COL,
           |props.$ADDRESS_COL as $ADDRESS_COL,
           |props.$ROLE_COL as $ROLE_COL,
           |props.$WRITER_COL as $WRITER_COL,
           | // serverID needs to be part of the grouping key here as it is guaranteed to be different on different servers
           |props.$SERVER_ID_COL as $SERVER_ID_COL,
           |d.$DATABASE_STATUS_PROPERTY as requestedStatus,
           |props.$CURRENT_STATUS_COL as $CURRENT_STATUS_COL,
           |props.$STATUS_MSG_COL as $STATUS_MSG_COL,
           |props.type as type,
           |d.$DATABASE_DEFAULT_PROPERTY as default,
           |homeDbName,
           |coalesce( homeDbName in collect(a.$DISPLAY_NAME_PROPERTY) + [d.name], false ) as home
           |$verboseColumns
           |$extraFilter
           |
           |WITH name AS $NAME_COL,
           |type,
           |[alias in aliases WHERE NOT (name = alias.$NAME_PROPERTY AND alias.$NAMESPACE_PROPERTY = '$DEFAULT_NAMESPACE') | alias.$DISPLAY_NAME_PROPERTY] as $ALIASES_COL,
           |$ACCESS_COL,
           |$ADDRESS_COL,
           |$ROLE_COL,
           |$WRITER_COL,
           |requestedStatus AS $REQUESTED_STATUS_COL,
           |$CURRENT_STATUS_COL,
           |$STATUS_MSG_COL,
           |default AS $DEFAULT_COL,
           |home AS $HOME_COL,
           |constituents as $CONSTITUENTS_COL
           |$verboseNames
           |$returnClause"""
    ).stripMargin
    SystemCommandExecutionPlan(
      scope.showCommandName,
      normalExecutionEngine,
      securityAuthorizationHandler,
      query,
      params,
      parameterGenerator = paramGenerator,
      parameterConverter = paramConverter,
      parameterValidator = paramValidator
    )
  }

  private def generateShowAccessibleDatabasesParameter(
    transaction: Transaction,
    securityContext: SecurityContext,
    maybeYield: Option[Yield],
    verbose: Boolean
  ): MapValue = {
    def accessForDatabase(database: Node, roles: java.util.Set[String]): Option[Boolean] = {
      // (:Role)-[p]->(:Privilege {action: 'access'})-[s:SCOPE]->()-[f:FOR]->(d:Database)
      var result: Seq[Boolean] = Seq.empty
      val dbRelationships = database.getRelationships(Direction.INCOMING, withName("FOR"))
      try {
        dbRelationships.forEach { f =>
          val startRelationships = f.getStartNode.getRelationships(Direction.INCOMING, withName("SCOPE"))
          try {
            startRelationships.forEach { s =>
              val privilegeNode = s.getStartNode
              if (privilegeNode.getProperty("action").equals("access")) {
                val prevRelationships = privilegeNode.getRelationships(Direction.INCOMING)
                try {
                  prevRelationships.forEach { p =>
                    val roleName = p.getStartNode.getProperty("name")
                    if (roles.contains(roleName)) {
                      p.getType.name() match {
                        case "DENIED"  => result = result :+ false
                        case "GRANTED" => result = result :+ true
                        case _         =>
                      }
                    }
                  }
                } finally {
                  prevRelationships.close()
                }
              }
            }
          } finally {
            startRelationships.close()
          }
        }
      } finally {
        dbRelationships.close()
      }
      result.reduceOption(_ && _)
    }

    val allowsStandardDatabaseManagement: Boolean =
      securityContext.allowsAdminAction(new AdminActionOnResource(
        PrivilegeAction.CREATE_DATABASE,
        AdminActionOnResource.DatabaseScope.ALL,
        Segment.ALL
      )).allowsAccess() ||
        securityContext.allowsAdminAction(new AdminActionOnResource(
          PrivilegeAction.DROP_DATABASE,
          AdminActionOnResource.DatabaseScope.ALL,
          Segment.ALL
        )).allowsAccess() ||
        securityContext.allowsAdminAction(new AdminActionOnResource(
          PrivilegeAction.ALTER_DATABASE,
          AdminActionOnResource.DatabaseScope.ALL,
          Segment.ALL
        )).allowsAccess() ||
        securityContext.allowsAdminAction(new AdminActionOnResource(
          PrivilegeAction.SET_DATABASE_ACCESS,
          AdminActionOnResource.DatabaseScope.ALL,
          Segment.ALL
        )).allowsAccess()
    val allowsCompositeDatabaseManagement: Boolean =
      securityContext.allowsAdminAction(new AdminActionOnResource(
        PrivilegeAction.CREATE_COMPOSITE_DATABASE,
        AdminActionOnResource.DatabaseScope.ALL,
        Segment.ALL
      )).allowsAccess() ||
        securityContext.allowsAdminAction(new AdminActionOnResource(
          PrivilegeAction.DROP_COMPOSITE_DATABASE,
          AdminActionOnResource.DatabaseScope.ALL,
          Segment.ALL
        )).allowsAccess()
    val allowsAllDatabaseManagement: Boolean =
      allowsStandardDatabaseManagement && allowsCompositeDatabaseManagement
    val roles = securityContext.mode().roles()

    def databaseAccess(label: String) =
      Using(transaction.findNodes(Label.label(label))) {
        defaultDatabaseNodes =>
          defaultDatabaseNodes.asScala.flatMap(defaultDatabaseNode =>
            accessForDatabase(defaultDatabaseNode, roles)
          ).reduceOption(_ && _)
      }.get
    val allDatabaseAccess = databaseAccess("DatabaseAll")
    val defaultDatabaseAccess = databaseAccess("DatabaseDefault")
    val defaultDatabaseName = defaultDatabaseResolver.defaultDatabase(securityContext.subject().executingUser())

    val accessibleDatabases =
      transaction.findNodes(DATABASE_NAME_LABEL, PRIMARY_PROPERTY, true).asScala
        .foldLeft[Map[DatabaseId, DatabaseType]](
          Map.empty
        ) { (acc, dbNameNode) =>
          val dbName = dbNameNode.getProperty(DATABASE_NAME_PROPERTY).toString
          val dbNode = Iterables.first(dbNameNode.getRelationships(TARGETS_RELATIONSHIP)).getEndNode
          val dbUUID = DatabaseIdFactory.from(UUID.fromString(dbNode.getProperty(DATABASE_UUID_PROPERTY).toString))
          val dbType =
            if (dbNode.hasLabel(COMPOSITE_DATABASE_LABEL) && dbNode.hasProperty(DATABASE_VIRTUAL_PROPERTY)) Composite
            else Standard
          val isDefault = dbName.equals(defaultDatabaseName)
          if (dbName.equals(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)) {
            acc + (dbUUID -> System)
          } else if (allowsAllDatabaseManagement) {
            acc + (dbUUID -> dbType)
          } else if (allowsStandardDatabaseManagement && dbType == Standard) {
            acc + (dbUUID -> dbType)
          } else if (allowsCompositeDatabaseManagement && dbType == Composite) {
            acc + (dbUUID -> dbType)
          } else {
            (accessForDatabase(dbNode, roles), allDatabaseAccess, defaultDatabaseAccess, isDefault) match {
              // denied
              case (Some(false), _, _, _)    => acc
              case (_, Some(false), _, _)    => acc
              case (_, _, Some(false), true) => acc

              // granted
              case (Some(true), _, _, _)    => acc + (dbUUID -> dbType)
              case (_, Some(true), _, _)    => acc + (dbUUID -> dbType)
              case (_, _, Some(true), true) => acc + (dbUUID -> dbType)

              // no privilege
              case _ => acc
            }
          }
        }

    val username = Option(securityContext.subject().executingUser()) match {
      case None       => Values.NO_VALUE
      case Some("")   => Values.NO_VALUE
      case Some(user) => Values.stringValue(user)
    }

    val dbMetadata =
      if (verbose && maybeYield.isDefined && requiresDetailedLookup(maybeYield.get)) {
        requestDetailedInfo(accessibleDatabases, transaction).asJava
      } else {
        lookupCachedInfo(accessibleDatabases, transaction).asJava
      }

    VirtualValues.map(
      Array(accessibleDbsKey, internalKey("username")),
      Array(VirtualValues.fromList(dbMetadata), username)
    )
  }

  private val detailedLookupCols = Set(
    STORE_COL,
    LAST_COMMITTED_TX_COL,
    REPLICATION_LAG_COL,
    CURRENT_PRIMARIES_COUNT_COL,
    CURRENT_SECONDARIES_COUNT_COL
  )

  private def requiresDetailedLookup(yields: Yield): Boolean = {
    yields.returnItems.includeExisting || yields.returnItems.items.map(_.expression).exists {
      case Variable(name) => detailedLookupCols.contains(name)
      case _              => false
    }
  }

  private def lookupCachedInfo(typeMap: Map[DatabaseId, DatabaseType], transaction: Transaction): List[AnyValue] = {
    val dbInfos = infoService.lookupCachedInfo(typeMap.keys.toSet.asJava, transaction).asScala
    dbInfos.map(info => BaseDatabaseInfoMapper.toMapValue(dbms, info, typeMap)).toList
  }

  private def requestDetailedInfo(typeMap: Map[DatabaseId, DatabaseType], transaction: Transaction)(implicit
  mapper: DatabaseInfoMapper[ExtendedDatabaseInfo]): List[AnyValue] = {
    val dbInfos = infoService.requestDetailedInfo(typeMap.keys.toSet.asJava, transaction).asScala
    dbInfos.map(info => mapper.toMapValue(dbms, info, typeMap)).toList
  }
}

trait DatabaseInfoMapper[T <: DatabaseInfo] {

  def toMapValue(
    databaseManagementService: DatabaseManagementService,
    extendedDatabaseInfo: T,
    typeMap: Map[DatabaseId, DatabaseType]
  ): MapValue
}

object BaseDatabaseInfoMapper extends DatabaseInfoMapper[DatabaseInfo] {

  override def toMapValue(
    databaseManagementService: DatabaseManagementService,
    extendedDatabaseInfo: DatabaseInfo,
    typeMap: Map[DatabaseId, DatabaseType]
  ): MapValue = {
    val databaseType = typeMap(extendedDatabaseInfo.namedDatabaseId().databaseId())
    val (access, role) =
      if (databaseType == Composite) (DatabaseAccess.READ_ONLY.getStringRepr, Values.NO_VALUE)
      else (extendedDatabaseInfo.access().getStringRepr, Values.stringValue(extendedDatabaseInfo.role()))

    VirtualValues.map(
      Array(
        NAME_COL,
        TYPE_COL,
        ACCESS_COL,
        ADDRESS_COL,
        ROLE_COL,
        WRITER_COL,
        CURRENT_STATUS_COL,
        STATUS_MSG_COL,
        DATABASE_ID_COL,
        SERVER_ID_COL
      ),
      Array(
        Values.stringValue(extendedDatabaseInfo.namedDatabaseId().name()),
        Values.stringValue(databaseType.toString),
        Values.stringValue(access),
        extendedDatabaseInfo.boltAddress().map[AnyValue](s => Values.stringValue(s.toString)).orElse(Values.NO_VALUE),
        role,
        Values.booleanValue(extendedDatabaseInfo.writer()),
        Values.stringValue(extendedDatabaseInfo.status()),
        Values.stringValue(extendedDatabaseInfo.statusMessage()),
        getDatabaseId(databaseManagementService, extendedDatabaseInfo.namedDatabaseId().name()).map(
          Values.stringValue
        ).getOrElse(Values.NO_VALUE),
        extendedDatabaseInfo.serverId().toScala.map(srvId => Values.stringValue(srvId.uuid().toString)).getOrElse(
          Values.NO_VALUE
        )
      )
    )
  }

  private def getDatabaseId(dbms: DatabaseManagementService, dbName: String): Option[String] = {
    Try(dbms.database(dbName).asInstanceOf[GraphDatabaseAPI]).toOption.flatMap(graphDatabaseAPI =>
      if (graphDatabaseAPI.isAvailable(0)) {
        val storeIdProvider = graphDatabaseAPI.getDependencyResolver.resolveDependency(classOf[StoreIdProvider])
        Some(StoreIdDecodeUtils.decodeId(storeIdProvider))
      } else {
        None
      }
    )
  }
}

object CommunityExtendedDatabaseInfoMapper extends DatabaseInfoMapper[ExtendedDatabaseInfo] {

  override def toMapValue(
    databaseManagementService: DatabaseManagementService,
    extendedDatabaseInfo: ExtendedDatabaseInfo,
    typeMap: Map[DatabaseId, DatabaseType]
  ): MapValue =
    BaseDatabaseInfoMapper.toMapValue(databaseManagementService, extendedDatabaseInfo, typeMap).updatedWith(
      VirtualValues.map(
        Array(
          CURRENT_PRIMARIES_COUNT_COL,
          CURRENT_SECONDARIES_COUNT_COL,
          STORE_COL,
          LAST_COMMITTED_TX_COL,
          REPLICATION_LAG_COL
        ),
        Array(
          Values.longValue(extendedDatabaseInfo.primariesCount()),
          Values.longValue(extendedDatabaseInfo.secondariesCount()),
          extendedDatabaseInfo.storeId().toScala.map(Values.stringValue).getOrElse(Values.NO_VALUE),
          Values.NO_VALUE,
          Values.longValue(0)
        )
      )
    )
}

sealed trait DatabaseType

case object System extends DatabaseType {
  override def toString: String = "system"
}

case object Standard extends DatabaseType {
  override def toString: String = "standard"
}

case object Composite extends DatabaseType {
  override def toString: String = "composite"
}
