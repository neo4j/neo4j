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
package org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.doNothing
import org.mockito.Mockito.when
import org.neo4j.common.DependencyResolver
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryTransactionalContext
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CommunityCypherRowFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.dbms.database.DatabaseContext
import org.neo4j.graphdb.ExecutionPlanDescription
import org.neo4j.graphdb.GqlStatusObject
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Notification
import org.neo4j.graphdb.QueryExecutionType
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.graphdb.ResourceIterator
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.Transaction
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.internal.kernel.api.Procedures
import org.neo4j.internal.kernel.api.security.AuthSubject
import org.neo4j.internal.kernel.api.security.PermissionState
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.internal.schema.LabelSchemaDescriptor
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor
import org.neo4j.internal.schema.SchemaDescriptors
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.impl.api.TransactionRegistry
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.jdk.CollectionConverters.SetHasAsJava
import scala.language.implicitConversions

class ShowCommandTestBase extends CypherFunSuite {
  protected lazy val initialCypherRow: CypherRow = CommunityCypherRowFactory().newRow()
  protected lazy val ctx: QueryContext = mock[QueryContext]
  protected lazy val queryState: QueryState = QueryStateHelper.emptyWith(query = ctx)
  protected lazy val txContext: QueryTransactionalContext = mock[QueryTransactionalContext]
  protected lazy val securityContext: SecurityContext = mock[SecurityContext]

  protected def setupBaseSecurityContext(): Unit = {
    when(ctx.transactionalContext).thenReturn(txContext)
    when(txContext.securityContext).thenReturn(securityContext)
    when(securityContext.subject()).thenReturn(AuthSubject.AUTH_DISABLED)
    when(securityContext.allowsAdminAction(any())).thenReturn(PermissionState.EXPLICIT_GRANT)
  }

  // To avoid needing to write Some(_) on all result checks
  implicit protected def stringToOption(value: String): Option[String] = Some(value)
  implicit protected def longToOption(value: Long): Option[Long] = Some(value)
  implicit protected def floatToOption(value: Float): Option[Float] = Some(value)
  implicit protected def booleanToOption(value: Boolean): Option[Boolean] = Some(value)
  implicit protected def listToOption[T](value: List[T]): Option[List[T]] = Some(value)
  implicit protected def mapToOption(value: Map[String, AnyValue]): Option[Map[String, AnyValue]] = Some(value)
  implicit protected def valueToOption(value: AnyValue): Option[AnyValue] = Some(value)

  // Index and constraint variables

  protected lazy val label: String = "Label"
  protected lazy val relType: String = "REL_TYPE"
  protected lazy val prop: String = "prop"
  protected lazy val labelDescriptor: LabelSchemaDescriptor = SchemaDescriptors.forLabel(0, 0)
  protected lazy val relTypeDescriptor: RelationTypeSchemaDescriptor = SchemaDescriptors.forRelType(0, 0)

  // Transaction variables

  protected lazy val userDbName: String = "database"
  protected lazy val userTxRegistry: TransactionRegistry = mock[TransactionRegistry]

  protected lazy val systemDbName: String = NamedDatabaseId.SYSTEM_DATABASE_NAME
  protected lazy val systemTxRegistry: TransactionRegistry = mock[TransactionRegistry]

  protected lazy val username: String = "executingUser"
  protected lazy val tx1: String = s"$userDbName-transaction-0"
  protected lazy val tx2: String = s"$userDbName-transaction-1"
  protected lazy val tx3: String = s"$systemDbName-transaction-0"

  protected def setupDbDependencies(dbContext: DatabaseContext, txRegistry: TransactionRegistry): Unit = {
    val dependencies = mock[DependencyResolver]
    when(dbContext.dependencies).thenReturn(dependencies)
    when(dependencies.resolveDependency(classOf[TransactionRegistry])).thenReturn(txRegistry)
  }

  // Procedure and function variables and functions

  // Cannot reach the default role variables (and are in either case mocking the privileges...)
  protected lazy val publicRole: String = "PUBLIC"
  protected lazy val adminRole: String = "admin"

  protected lazy val systemGraph: GraphDatabaseService = mock[GraphDatabaseService]
  protected lazy val securityHandler: SecurityAuthorizationHandler = mock[SecurityAuthorizationHandler]
  protected lazy val systemTx: Transaction = mock[Transaction]
  protected lazy val procedures: Procedures = mock[Procedures]

  protected def mockSetupProcFunc(): Unit = {
    when(ctx.systemGraph).thenReturn(systemGraph)
    when(systemGraph.beginTx()).thenReturn(systemTx)
    doNothing().when(systemTx).commit()

    setupBaseSecurityContext()
    when(securityContext.roles()).thenReturn(Set.empty[String].asJava)

    when(txContext.procedures).thenReturn(procedures)
    when(txContext.securityAuthorizationHandler).thenReturn(securityHandler)

    when(securityHandler.logAndGetAuthorizationException(any(), any()))
      .thenAnswer(invocation => new AuthorizationViolationException(invocation.getArgument(1)))
  }

  protected def handleSystemQueries(query: String, segment: String): Result = query match {
    case "SHOW ALL PRIVILEGES YIELD * WHERE action='execute' AND segment STARTS WITH $seg RETURN access, segment, collect(role) as roles" =>
      MockResult(List(Map("access" -> "GRANTED", "segment" -> s"$segment(*)", "roles" -> List(publicRole).asJava)))
    case "SHOW ALL PRIVILEGES YIELD * WHERE action STARTS WITH 'execute_boosted' AND segment STARTS WITH $seg RETURN access, segment, collect(role) as roles" =>
      MockResult()
    case "SHOW ALL PRIVILEGES YIELD * WHERE action='execute_admin' RETURN access, collect(role) as roles" =>
      MockResult()
    case "SHOW ALL PRIVILEGES YIELD * WHERE action IN ['admin', 'dbms_actions'] RETURN access, collect(role) as roles" =>
      MockResult(List(Map("access" -> "GRANTED", "roles" -> List(adminRole).asJava)))
    case "SHOW USERS YIELD user, roles WHERE user = $name RETURN roles" =>
      MockResult(List(Map("roles" -> List(publicRole).asJava)))
    // For some reason we get null query sometimes (probably related to mock set-up),
    // return empty result as to not get exception and continue the tests
    case null => MockResult()
    case _    => throw new RuntimeException(s"Unexpected system query: $query")
  }

  protected def argumentAndReturnDescriptionMaps(
    name: String,
    description: String,
    _type: String,
    deprecated: Boolean = false,
    default: Option[String] = None
  ): AnyValue =
    default.map(d =>
      VirtualValues.map(
        Array("name", "description", "type", "isDeprecated", "default"),
        Array(
          Values.stringValue(name),
          Values.stringValue(description),
          Values.stringValue(_type),
          Values.booleanValue(deprecated),
          Values.stringValue(d)
        )
      )
    ).getOrElse(VirtualValues.map(
      Array("name", "description", "type", "isDeprecated"),
      Array(
        Values.stringValue(name),
        Values.stringValue(description),
        Values.stringValue(_type),
        Values.booleanValue(deprecated)
      )
    ))

  // noinspection NotImplementedCode
  // Need to return a Result from the system queries
  protected case class MockResult(result: List[Map[String, AnyRef]] = List.empty) extends Result {
    private var index: Int = 0

    override def hasNext: Boolean = result.size > index

    override def next(): java.util.Map[String, AnyRef] = {
      val res = result(index).asJava
      index = index + 1
      res
    }

    override def columnAs[T](name: String): ResourceIterator[T] = new ResourceIterator[T] {
      override def close(): Unit = {}

      override def hasNext: Boolean = MockResult.this.hasNext

      override def next(): T = MockResult.this.next().get(name).asInstanceOf[T]
    }

    override def getQueryExecutionType: QueryExecutionType = ???

    override def columns(): java.util.List[String] = ???

    override def close(): Unit = ???

    override def getQueryStatistics: QueryStatistics = ???

    override def getExecutionPlanDescription: ExecutionPlanDescription = ???

    override def resultAsString(): String = ???

    override def writeAsStringTo(writer: java.io.PrintWriter): Unit = ???

    override def getNotifications: java.lang.Iterable[Notification] = ???

    override def getGqlStatusObjects: java.lang.Iterable[GqlStatusObject] = ???

    override def accept[VisitationException <: Exception](visitor: Result.ResultVisitor[VisitationException]): Unit =
      ???
  }
}
