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
import org.neo4j.cypher.internal.AdministrationCommandRuntime.getNameFields
import org.neo4j.cypher.internal.AdministrationCommandRuntime.internalKey
import org.neo4j.cypher.internal.AdministrationCommandRuntime.runtimeStringValue
import org.neo4j.cypher.internal.AdministrationShowCommandUtils
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.ast.DatabaseScope
import org.neo4j.cypher.internal.ast.DefaultDatabaseScope
import org.neo4j.cypher.internal.ast.HomeDatabaseScope
import org.neo4j.cypher.internal.ast.NamedDatabaseScope
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.procs.SystemCommandExecutionPlan
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.dbms.database.DatabaseInfo
import org.neo4j.dbms.database.DatabaseInfoService
import org.neo4j.dbms.database.ExtendedDatabaseInfo
import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType.withName
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.kernel.api.security.AdminActionOnResource
import org.neo4j.internal.kernel.api.security.PrivilegeAction
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.internal.kernel.api.security.Segment
import org.neo4j.kernel.database.DefaultDatabaseResolver
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.storageengine.api.StoreIdProvider
import org.neo4j.storageengine.util.StoreIdDecodeUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

import java.util.Optional
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.JavaConverters.setAsJavaSetConverter
import scala.util.Try

case class ShowDatabasesExecutionPlanner(resolver: DependencyResolver, defaultDatabaseResolver: DefaultDatabaseResolver,
                                         normalExecutionEngine: ExecutionEngine, securityAuthorizationHandler: SecurityAuthorizationHandler)
                                        (implicit extendedDatabaseInfoMapper: DatabaseInfoMapper[ExtendedDatabaseInfo]) {

  private val accessibleDbsKey = internalKey("accessibleDbs")
  private val dbms = resolver.resolveDependency(classOf[DatabaseManagementService])
  private val infoService = resolver.resolveDependency(classOf[DatabaseInfoService])

  def planShowDatabases(scope: DatabaseScope, verbose: Boolean, symbols: List[String], yields: Option[Yield], returns: Option[Return]): ExecutionPlan = {
      val usernameKey = internalKey("username")
      val paramGenerator: (Transaction, SecurityContext) => MapValue = (tx, securityContext) => generateShowAccessibleDatabasesParameter(tx, securityContext, yields, verbose)
      val (extraFilter, params, paramConverter) = scope match {
        // show default database
        case _: DefaultDatabaseScope => (s"WHERE default = true", VirtualValues.EMPTY_MAP, IdentityConverter)
        // show home database
        case _: HomeDatabaseScope => (s"WHERE home = true", VirtualValues.EMPTY_MAP, IdentityConverter)
        // show database name
        case NamedDatabaseScope(p) =>
          val nameFields = getNameFields("databaseName", p, valueMapper = s => new NormalizedDatabaseName(s).name())
          val combinedConverter: (Transaction, MapValue) => MapValue = (tx, m) => {
            val normalizedName = new NormalizedDatabaseName(runtimeStringValue(p, m)).name()
            val filteredDatabases = VirtualValues.fromList(m.get(accessibleDbsKey).asInstanceOf[ListValue].asScala
              .filter { case db: MapValue => Values.stringValue(normalizedName).equals(db.get("name")) }.toList.asJava
            )
            nameFields.nameConverter(tx, m.updatedWith(accessibleDbsKey, filteredDatabases))
          }
          (s"WHERE name = $$`${nameFields.nameKey}`", VirtualValues.map(Array(nameFields.nameKey), Array(nameFields.nameValue)), combinedConverter)
        // show all databases
        case _ => ("", VirtualValues.EMPTY_MAP, IdentityConverter)
      }
      val verboseColumns = if (verbose) ", props.databaseID as databaseID, props.serverID as serverID, props.lastCommittedTxn as lastCommittedTxn, props.replicationLag as replicationLag" else ""
      val returnClause = AdministrationShowCommandUtils.generateReturnClause(symbols, yields, returns, Seq("name"))

      val query = s"""// First resolve which database is the home database
                     |OPTIONAL MATCH (default: Database {default: true})
                     |OPTIONAL MATCH (user:User {name: $$`$usernameKey`})
                     |WITH coalesce(user.homeDatabase, default.name) as homeDbName
                     |
                     |UNWIND $$`$accessibleDbsKey` AS props
                     |MATCH (d:Database)
                     |WHERE d.name = props.name
                     |WITH d.name as name, props.address as address, props.role as role, d.status as requestedStatus, props.status as currentStatus, props.error as error,
                     |d.default as default, coalesce(d.name = homeDbName, false) as home
                     |$verboseColumns
                     |$extraFilter
                     |$returnClause""".stripMargin
      SystemCommandExecutionPlan(scope.showCommandName,
        normalExecutionEngine,
        securityAuthorizationHandler,
        query,
        params,
        parameterGenerator = paramGenerator,
        parameterConverter = paramConverter)
  }

  private def generateShowAccessibleDatabasesParameter(transaction: Transaction, securityContext: SecurityContext, maybeYield: Option[Yield], verbose: Boolean): MapValue = {
    def accessForDatabase(database: Node, roles: java.util.Set[String]): Option[Boolean] = {
      //(:Role)-[p]->(:Privilege {action: 'access'})-[s:SCOPE]->()-[f:FOR]->(d:Database)
      var result: Seq[Boolean] = Seq.empty
      database.getRelationships(Direction.INCOMING, withName("FOR")).forEach { f =>
        f.getStartNode.getRelationships(Direction.INCOMING, withName("SCOPE")).forEach { s =>
          val privilegeNode = s.getStartNode
          if (privilegeNode.getProperty("action").equals("access")) {
            privilegeNode.getRelationships(Direction.INCOMING).forEach { p =>
              val roleName = p.getStartNode.getProperty("name")
              if (roles.contains(roleName)) {
                p.getType.name() match {
                  case "DENIED" => result = result :+ false
                  case "GRANTED" => result = result :+ true
                  case _ =>
                }
              }
            }
          }
        }
      }
      result.reduceOption(_ && _)
    }

    val allowsDatabaseManagement: Boolean =
      securityContext.allowsAdminAction(new AdminActionOnResource(PrivilegeAction.CREATE_DATABASE, AdminActionOnResource.DatabaseScope.ALL, Segment.ALL)).allowsAccess() ||
        securityContext.allowsAdminAction(new AdminActionOnResource(PrivilegeAction.DROP_DATABASE, AdminActionOnResource.DatabaseScope.ALL, Segment.ALL)).allowsAccess() ||
        securityContext.allowsAdminAction(new AdminActionOnResource(PrivilegeAction.ALTER_DATABASE, AdminActionOnResource.DatabaseScope.ALL, Segment.ALL)).allowsAccess() ||
        securityContext.allowsAdminAction(new AdminActionOnResource(PrivilegeAction.SET_DATABASE_ACCESS, AdminActionOnResource.DatabaseScope.ALL, Segment.ALL)).allowsAccess()
    val roles = securityContext.mode().roles()

    val allDatabaseNode = transaction.findNode(Label.label("DatabaseAll"), "name", "*")
    val allDatabaseAccess = if (allDatabaseNode != null) accessForDatabase(allDatabaseNode, roles) else None
    val defaultDatabaseNode = transaction.findNode(Label.label("DatabaseDefault"), "name", "DEFAULT")
    val defaultDatabaseAccess = if (defaultDatabaseNode != null) accessForDatabase(defaultDatabaseNode, roles) else None
    val defaultDatabaseName = defaultDatabaseResolver.defaultDatabase(securityContext.subject().executingUser())

    val accessibleDatabases = transaction.findNodes(Label.label("Database")).asScala.foldLeft[Seq[String]](Seq.empty) { (acc, dbNode) =>
      val dbName = dbNode.getProperty("name").toString
      val isDefault = dbName.equals(defaultDatabaseName)
      if (dbName.equals(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)) {
        acc :+ dbName
      } else if (allowsDatabaseManagement) {
        acc :+ dbName
      } else {
        (accessForDatabase(dbNode, roles), allDatabaseAccess, defaultDatabaseAccess, isDefault) match {
          // denied
          case (Some(false), _, _, _) => acc
          case (_, Some(false), _, _) => acc
          case (_, _, Some(false), true) => acc

          // granted
          case (Some(true), _, _, _) => acc :+ dbName
          case (_, Some(true), _, _) => acc :+ dbName
          case (_, _, Some(true), true) => acc :+ dbName

          // no privilege
          case _ => acc
        }
      }
    }

    val username = Option(securityContext.subject().executingUser()) match {
      case None => Values.NO_VALUE
      case Some("") => Values.NO_VALUE
      case Some(user) => Values.stringValue(user)
    }

    val dbMetadata = if (verbose && maybeYield.isDefined && requiresDetailedLookup(maybeYield.get)  ) {
      requestDetailedInfo(accessibleDatabases).asJava
    } else {
      lookupCachedInfo(accessibleDatabases).asJava
    }
    VirtualValues.map(Array(accessibleDbsKey, internalKey("username")), Array(VirtualValues.fromList(dbMetadata), username))
  }

  private def requiresDetailedLookup(yields: Yield): Boolean =
    yields.returnItems.includeExisting || yields.returnItems.items.map(_.expression).exists {
      case Variable(name) => Set("lastCommittedTxn", "replicationLag").contains(name)
      case _ => false
    }

  private def lookupCachedInfo(databaseNames: Seq[String]): List[AnyValue] = {
    val dbInfos = infoService.lookupCachedInfo(databaseNames.toSet.asJava).asScala
    dbInfos.map(info => BaseDatabaseInfoMapper.toMapValue(dbms, info)).toList
  }

  private def requestDetailedInfo(databaseNames: Seq[String])(implicit mapper: DatabaseInfoMapper[ExtendedDatabaseInfo]): List[AnyValue] = {
    val dbInfos = infoService.requestDetailedInfo(databaseNames.toSet.asJava).asScala
    dbInfos.map(info => mapper.toMapValue(dbms, info)).toList
  }
}

trait DatabaseInfoMapper[T <: DatabaseInfo] {
  def toMapValue(databaseManagementService: DatabaseManagementService, extendedDatabaseInfo: T): MapValue
}

object BaseDatabaseInfoMapper extends DatabaseInfoMapper[DatabaseInfo] {
  override def toMapValue(databaseManagementService: DatabaseManagementService,
                          extendedDatabaseInfo: DatabaseInfo): MapValue = VirtualValues.map(
    Array("name", "address", "role", "status", "error", "databaseID", "serverID"),
    Array(
      Values.stringValue(extendedDatabaseInfo.namedDatabaseId().name()),
      extendedDatabaseInfo.boltAddress().map[AnyValue](s => Values.stringValue(s.toString)).orElse(Values.NO_VALUE),
      Values.stringValue(extendedDatabaseInfo.role()),
      Values.stringValue(extendedDatabaseInfo.status()),
      Values.stringValue(extendedDatabaseInfo.error()),
      getDatabaseId(databaseManagementService, extendedDatabaseInfo.namedDatabaseId().name()).map(Values.stringValue).getOrElse(Values.NO_VALUE),
      extendedDatabaseInfo.serverId().asScala.map(srvId => Values.stringValue(srvId.uuid().toString)).getOrElse(Values.NO_VALUE),
    )
  )

  private def getDatabaseId(dbms: DatabaseManagementService, dbName: String): Option[String] = {
    Try(dbms.database(dbName).asInstanceOf[GraphDatabaseAPI]).toOption.flatMap( graphDatabaseAPI =>
      if (graphDatabaseAPI.isAvailable(0)) {
        val storeIdProvider = graphDatabaseAPI.getDependencyResolver.resolveDependency(classOf[StoreIdProvider])
        Some(StoreIdDecodeUtils.decodeId(storeIdProvider))
      } else {
        None
      }
    )
  }

  implicit class RichOptional[T](v: Optional[T]) {
    def asScala: Option[T] = if (v.isPresent) Some(v.get()) else None
  }
}

object CommunityExtendedDatabaseInfoMapper extends DatabaseInfoMapper[ExtendedDatabaseInfo] {

  override def toMapValue(databaseManagementService: DatabaseManagementService, extendedDatabaseInfo: ExtendedDatabaseInfo): MapValue =
    BaseDatabaseInfoMapper.toMapValue(databaseManagementService, extendedDatabaseInfo).updatedWith(
      VirtualValues.map(
        Array("lastCommittedTxn", "replicationLag"),
        Array(
          Values.NO_VALUE,
          Values.longValue(0)
        )
      )
    )
}


