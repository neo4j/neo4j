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
package org.neo4j.cypher.internal.runtime.interpreted

import org.apache.commons.lang3.SystemUtils
import org.assertj.core.api.Assertions.assertThat
import org.mockito.Mockito.RETURNS_DEEP_STUBS
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions
import org.mockito.Mockito.when
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.cypher.HttpServerTestSupportBuilder
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.javacompat
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.CreateTempFileTestSupport
import org.neo4j.cypher.internal.runtime.DummyResource
import org.neo4j.cypher.internal.runtime.DummyResource.verifyClose
import org.neo4j.cypher.internal.runtime.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.security.URLAccessValidationError
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.TokenReadSession
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.internal.kernel.api.security.SecurityContext.AUTH_DISABLED
import org.neo4j.internal.schema.SchemaDescriptors
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour
import org.neo4j.io.pagecache.context.CursorContext
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.api.query.ExecutingQuery
import org.neo4j.kernel.api.security.AnonymousContext
import org.neo4j.kernel.database.TestDatabaseIdRepository
import org.neo4j.kernel.impl.api.KernelStatement
import org.neo4j.kernel.impl.api.KernelTransactionImplementation
import org.neo4j.kernel.impl.api.TransactionClockContext
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.factory.KernelTransactionFactory
import org.neo4j.kernel.impl.newapi.DefaultPooledCursors
import org.neo4j.kernel.impl.query.Neo4jTransactionalContext
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory
import org.neo4j.kernel.impl.query.QueryExecutionConfiguration
import org.neo4j.lock.LockTracer
import org.neo4j.resources.CpuClock
import org.neo4j.storageengine.api.cursor.StoreCursors
import org.neo4j.test.TestDatabaseManagementServiceBuilder
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP

import java.lang.Boolean.FALSE
import java.lang.Boolean.TRUE
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicReference

import scala.jdk.CollectionConverters.MapHasAsJava

class TransactionBoundQueryContextTest extends CypherFunSuite with CreateTempFileTestSupport {

  var managementService: DatabaseManagementService = _
  var graphOps: GraphDatabaseService = null
  var graph: GraphDatabaseQueryService = null
  var outerTx: InternalTransaction = null
  var transactionFactory: KernelTransactionFactory = _
  var statement: KernelStatement = null
  val indexSearchMonitor = mock[IndexSearchMonitor]

  override def beforeEach(): Unit = {
    super.beforeEach()
    managementService = new TestDatabaseManagementServiceBuilder().impermanent().build()
    graphOps = managementService.database(DEFAULT_DATABASE_NAME)
    graph = new javacompat.GraphDatabaseCypherService(graphOps)
    transactionFactory = mock[KernelTransactionFactory]
    outerTx = mock[InternalTransaction](RETURNS_DEEP_STUBS)
    val kernelTransaction = mock[KernelTransactionImplementation](RETURNS_DEEP_STUBS)
    when(kernelTransaction.securityContext()).thenReturn(AUTH_DISABLED)
    when(kernelTransaction.acquireStatement()).thenReturn(statement)
    statement = new KernelStatement(
      kernelTransaction,
      LockTracer.NONE,
      new TransactionClockContext(),
      new AtomicReference[CpuClock](CpuClock.NOT_AVAILABLE),
      new TestDatabaseIdRepository().defaultDatabase,
      Config.defaults()
    )
    statement.initialize(null, CursorContext.NULL_CONTEXT, 7)
    statement.acquire()
  }

  override def afterEach(): Unit = {
    managementService.shutdown()
  }

  test("should mark transaction successful if successful") {
    // GIVEN
    when(outerTx.rollback()).thenThrow(new AssertionError("Shouldn't be called"))
    when(outerTx.transactionType()).thenReturn(Type.IMPLICIT)
    when(outerTx.securityContext()).thenReturn(AUTH_DISABLED)
    when(outerTx.clientInfo()).thenReturn(ClientConnectionInfo.EMBEDDED_CONNECTION)

    val transaction = mock[KernelTransaction]
    val indexingBehaviour = mock[StorageEngineIndexingBehaviour];
    when(transaction.cursors()).thenReturn(new DefaultPooledCursors(
      null,
      StoreCursors.NULL,
      Config.defaults(),
      indexingBehaviour,
      false
    ))
    val tc = new Neo4jTransactionalContext(
      graph,
      outerTx,
      statement,
      mock[ExecutingQuery],
      transactionFactory,
      QueryExecutionConfiguration.DEFAULT_CONFIG
    )
    val transactionalContext = TransactionalContextWrapper(tc)
    val context = new TransactionBoundQueryContext(transactionalContext, new ResourceManager)(indexSearchMonitor)
    // WHEN
    context.transactionalContext.close()

    // THEN
    verify(outerTx).transactionType()
    verify(outerTx).clientInfo()
    verify(outerTx).securityContext()
    verify(outerTx).kernelTransaction()
    verify(outerTx).elementIdMapper()
    verifyNoMoreInteractions(outerTx)
  }

  test("should mark transaction failed if not successful") {
    // GIVEN
    when(outerTx.commit()).thenThrow(new AssertionError("Shouldn't be called"))
    when(outerTx.transactionType()).thenReturn(Type.IMPLICIT)
    when(outerTx.securityContext()).thenReturn(AUTH_DISABLED)
    when(outerTx.clientInfo()).thenReturn(ClientConnectionInfo.EMBEDDED_CONNECTION)
    val transaction = mock[KernelTransaction]
    when(transaction.acquireStatement()).thenReturn(statement)
    val indexingBehaviour = mock[StorageEngineIndexingBehaviour];
    when(transaction.cursors()).thenReturn(new DefaultPooledCursors(
      null,
      StoreCursors.NULL,
      Config.defaults(),
      indexingBehaviour,
      false
    ))
    val tc = new Neo4jTransactionalContext(
      graph,
      outerTx,
      statement,
      mock[ExecutingQuery],
      transactionFactory,
      QueryExecutionConfiguration.DEFAULT_CONFIG
    )
    val transactionalContext = TransactionalContextWrapper(tc)
    val context = new TransactionBoundQueryContext(transactionalContext, new ResourceManager)(indexSearchMonitor)
    // WHEN
    context.transactionalContext.close()

    // THEN
    verify(outerTx).transactionType()
    verify(outerTx).clientInfo()
    verify(outerTx).securityContext()
    verify(outerTx).kernelTransaction()
    verify(outerTx).elementIdMapper()
    verifyNoMoreInteractions(outerTx)
  }

  test("should return fresh but equal iterators") {
    // GIVEN
    val relTypeName = "LINK"
    val node = createMiniGraph(relTypeName)

    val tx = graph.beginTransaction(Type.EXPLICIT, AnonymousContext.read())
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))
    val context = new TransactionBoundQueryContext(transactionalContext, new ResourceManager)(indexSearchMonitor)

    // WHEN
    val iteratorA = context.getRelationshipsForIds(node.getId, SemanticDirection.BOTH, null)
    val iteratorB = context.getRelationshipsForIds(node.getId, SemanticDirection.BOTH, null)

    // THEN
    iteratorA should not equal iteratorB
    val listA = PrimitiveLongHelper.map(iteratorA, i => i).toList
    val listB = PrimitiveLongHelper.map(iteratorB, i => i).toList
    listA should equal(listB)
    listA.size should equal(2)

    transactionalContext.close()
    tx.close()
  }

  test("getRelationshipsForIds closes underlying cursor") {
    // GIVEN
    val relTypeName = "LINK"
    val node = createMiniGraph(relTypeName)

    val tx = graph.beginTransaction(Type.EXPLICIT, AnonymousContext.read())
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))
    val monitor = QueryStateHelper.trackClosedMonitor
    val context =
      new TransactionBoundQueryContext(transactionalContext, new ResourceManager(monitor))(indexSearchMonitor)
    val iteratorA = context.getRelationshipsForIds(node.getId, SemanticDirection.BOTH, null)

    // WHEN
    iteratorA.next()
    iteratorA.close()

    // THEN
    monitor.closedResources.collect { case r: RelationshipTraversalCursor => r } should have size (1)
  }

  test("getRelationshipsForIdsPrimitive closes underlying cursor") {
    // GIVEN
    val relTypeName = "LINK"
    val node = createMiniGraph(relTypeName)

    val tx = graph.beginTransaction(Type.EXPLICIT, AnonymousContext.read())
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))
    val monitor = QueryStateHelper.trackClosedMonitor
    val context =
      new TransactionBoundQueryContext(transactionalContext, new ResourceManager(monitor))(indexSearchMonitor)
    val iteratorA = context.getRelationshipsForIds(node.getId, SemanticDirection.BOTH, null)

    // WHEN
    iteratorA.next()
    iteratorA.close()

    // THEN
    monitor.closedResources.collect { case r: RelationshipTraversalCursor => r } should have size (1)
  }

  test("getNodesByLabel closes underlying cursor") {
    // GIVEN
    createLabeledNodesAndRels()

    val tx = graph.beginTransaction(Type.EXPLICIT, AnonymousContext.read())
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))
    val monitor = QueryStateHelper.trackClosedMonitor
    val context =
      new TransactionBoundQueryContext(transactionalContext, new ResourceManager(monitor))(indexSearchMonitor)

    val iteratorA = context.getNodesByLabel(tokenReadSession(tx), 0, IndexOrderNone)

    // WHEN
    iteratorA.next()
    iteratorA.close()

    // THEN
    monitor.closedResources.collect { case r: NodeLabelIndexCursor => r } should have size (1)
  }

  test("getNodesByLabelPrimitive closes underlying cursor") {
    // GIVEN
    createLabeledNodesAndRels()

    val tx = graph.beginTransaction(Type.EXPLICIT, AnonymousContext.read())
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))
    val monitor = QueryStateHelper.trackClosedMonitor
    val context =
      new TransactionBoundQueryContext(transactionalContext, new ResourceManager(monitor))(indexSearchMonitor)
    val iteratorA = context.getNodesByLabel(tokenReadSession(tx), 0, IndexOrderNone)

    // WHEN
    iteratorA.next()
    iteratorA.close()

    // THEN
    monitor.closedResources.collect { case r: NodeLabelIndexCursor => r } should have size (1)
  }

  test("nodeOps.all closes underlying cursor") {
    // GIVEN
    createLabeledNodesAndRels()

    val tx = graph.beginTransaction(Type.EXPLICIT, AnonymousContext.read())
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))
    val monitor = QueryStateHelper.trackClosedMonitor
    val context =
      new TransactionBoundQueryContext(transactionalContext, new ResourceManager(monitor))(indexSearchMonitor)
    val iteratorA = context.nodeReadOps.all

    // WHEN
    iteratorA.next()
    iteratorA.close()

    // THEN
    monitor.closedResources.collect { case r: NodeCursor => r } should have size (1)
  }

  test("relationshipOps.all closes underlying cursor") {
    // GIVEN
    createLabeledNodesAndRels()

    val tx = graph.beginTransaction(Type.EXPLICIT, AnonymousContext.read())
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))
    val monitor = QueryStateHelper.trackClosedMonitor
    val context =
      new TransactionBoundQueryContext(transactionalContext, new ResourceManager(monitor))(indexSearchMonitor)
    val iteratorA = context.relationshipReadOps.all

    // WHEN
    iteratorA.next()
    iteratorA.close()

    // THEN
    monitor.closedResources.collect { case r: RelationshipScanCursor => r } should have size (1)
  }

  test("should deny non-whitelisted URL protocols for loading") {
    // GIVEN
    val tx = graph.beginTransaction(Type.EXPLICIT, AnonymousContext.read())
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))
    val context = new TransactionBoundQueryContext(transactionalContext, new ResourceManager)(indexSearchMonitor)
    val fileName = "data.csv"
    val fileUrl = createCSVTempFileURL(fileName)({
      _ => ()
    })

    // THEN
    withHttpServer(
      s"/$fileName",
      httpPort => {
        context.getImportDataConnection(
          new URI(s"http://localhost:$httpPort/$fileName")
        ).sourceDescription() should equal(
          s"http://localhost:$httpPort/$fileName"
        )
      }
    )
    windowsSafeFileURL(context.getImportDataConnection(new URI(fileUrl)).sourceDescription()) should equal(
      windowsSafeFileURL(new URI(fileUrl).getSchemeSpecificPart)
    )
    the[URLAccessValidationError] thrownBy (context.getImportDataConnection(
      new URI("jar:file:/tmp/blah.jar!/tmp/foo/data.csv")
    )) should have message "Invalid URL 'jar:file:/tmp/blah.jar!/tmp/foo/data.csv': unknown protocol: jar"

    transactionalContext.close()
    tx.close()
  }

  private def windowsSafeFileURL(url: String) =
    if (SystemUtils.IS_OS_WINDOWS) {
      // getFile on Windows uses / and has a leading one before `C:`
      // while sourceDescription uses \ and doesn't have that initial \
      // Let's normalize to one format: use / but not an initial one
      val switchedSlashes = url.replaceAll("\\\\", "/")
      if (switchedSlashes.startsWith("/")) switchedSlashes.tail else switchedSlashes
    } else url

  test("should deny file URLs when not allowed by config") {
    // GIVEN
    managementService.shutdown()
    startGraph(GraphDatabaseSettings.allow_file_urls -> FALSE, GraphDatabaseSettings.auth_enabled -> TRUE)
    val tx = graph.beginTransaction(Type.EXPLICIT, AnonymousContext.read())
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))
    val context = new TransactionBoundQueryContext(transactionalContext, new ResourceManager)(indexSearchMonitor)

    // THEN
    withHttpServer(
      "/data.csv",
      httpPort => {
        context.getImportDataConnection(
          new URI(s"http://localhost:$httpPort/data.csv")
        ).sourceDescription() should equal(
          s"http://localhost:$httpPort/data.csv"
        )
        the[URLAccessValidationError] thrownBy (context.getImportDataConnection(
          new URI("file:///tmp/foo/data.csv")
        )) should have message (
          "configuration property 'dbms.security.allow_csv_import_from_file_urls' is false"
        )

        transactionalContext.close()
        tx.close()
      }
    )
  }

  test("provide access to kernel statement page cache tracer") {
    val creator = graphOps.beginTx()
    creator.createNode()
    creator.createNode()
    creator.createNode()
    creator.commit()

    val tx = graph.beginTransaction(Type.EXPLICIT, LoginContext.AUTH_DISABLED)
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))

    val tracer = transactionalContext.kernelStatisticProvider
    tracer.getPageCacheHits should equal(0)

    tx.getNodeById(2)
    tx.getNodeById(1)
    val accesses = tracer.getPageCacheHits + tracer.getPageCacheMisses
    assertThat(accesses).isGreaterThanOrEqualTo(1L)

    transactionalContext.close()
    tx.close()
  }

  test("should close all resources when closing resources") {
    // GIVEN
    val transaction = mock[KernelTransaction]
    when(transaction.acquireStatement()).thenReturn(statement)
    val indexingBehaviour = mock[StorageEngineIndexingBehaviour];
    when(transaction.cursors()).thenReturn(new DefaultPooledCursors(
      null,
      StoreCursors.NULL,
      Config.defaults(),
      indexingBehaviour,
      false
    ))
    val tc = new Neo4jTransactionalContext(
      graph,
      outerTx,
      statement,
      mock[ExecutingQuery],
      transactionFactory,
      QueryExecutionConfiguration.DEFAULT_CONFIG
    )
    val transactionalContext = TransactionalContextWrapper(tc)
    val context = new TransactionBoundQueryContext(transactionalContext, new ResourceManager)(indexSearchMonitor)
    val resource1 = new DummyResource
    val resource2 = new DummyResource
    val resource3 = new DummyResource
    context.resources.trace(resource1)
    context.resources.trace(resource2)
    context.resources.trace(resource3)

    // WHEN
    context.resources.close()

    // THEN
    verifyClose(resource1)
    verifyClose(resource2)
    verifyClose(resource3)
  }

  test("should add cursor as resource when calling all") {
    // GIVEN
    val tx = graph.beginTransaction(Type.EXPLICIT, AnonymousContext.read())
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))
    val context = new TransactionBoundQueryContext(transactionalContext, new ResourceManager)(indexSearchMonitor)
    val initSize = context.resources.allResources.size

    // WHEN
    context.nodeReadOps.all

    // THEN
    context.resources.allResources should have size initSize + 1
    context.resources.close()
    tx.close()
  }

  test("should remove cursor after closing resource") {
    // GIVEN
    val tx = graph.beginTransaction(Type.EXPLICIT, AnonymousContext.read())
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))
    val context = new TransactionBoundQueryContext(transactionalContext, new ResourceManager)(indexSearchMonitor)
    val initSize = context.resources.allResources.size

    // WHEN
    context.nodeReadOps.all
    context.resources.allResources should have size initSize + 1
    context.resources.close()

    // THEN
    context.resources.allResources shouldBe empty
    tx.close()
  }

  private def startGraph(config: (Setting[_], Object)*): Unit = {
    val configs = config.toMap
    managementService = new TestDatabaseManagementServiceBuilder().impermanent().setConfig(configs.asJava).build()
    graphOps = managementService.database(DEFAULT_DATABASE_NAME)
    graph = new GraphDatabaseCypherService(graphOps)
  }

  private def createTransactionContext(
    graphDatabaseQueryService: GraphDatabaseQueryService,
    transaction: InternalTransaction
  ) = {
    val contextFactory = Neo4jTransactionalContextFactory.create(graphDatabaseQueryService)
    contextFactory.newContext(transaction, "no query", EMPTY_MAP, QueryExecutionConfiguration.DEFAULT_CONFIG)
  }

  private def createMiniGraph(relTypeName: String): Node = {
    val relType = RelationshipType.withName(relTypeName)
    val tx = graph.beginTransaction(Type.EXPLICIT, AnonymousContext.writeToken())
    try {
      val node = tx.createNode()
      val other1 = tx.createNode()
      val other2 = tx.createNode()

      node.createRelationshipTo(other1, relType)
      other2.createRelationshipTo(node, relType)
      tx.commit()
      node
    } finally {
      tx.close()
    }
  }

  private def tokenReadSession(tx: InternalTransaction): TokenReadSession = {
    val index = tx.kernelTransaction().schemaRead.indexForSchemaNonTransactional(
      SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR
    ).next()
    tx.kernelTransaction().dataRead().tokenReadSession(index)
  }

  private def createLabeledNodesAndRels(): Unit = {
    val label = Label.label("Foo")
    val relType = RelationshipType.withName("Foo")
    val tx = graph.beginTransaction(Type.EXPLICIT, AnonymousContext.writeToken())
    try {
      val n1 = tx.createNode(label)
      val n2 = tx.createNode(label)
      val n3 = tx.createNode(label)

      n1.createRelationshipTo(n2, relType)
      n2.createRelationshipTo(n3, relType)
      n3.createRelationshipTo(n1, relType)

      tx.commit()
    } finally {
      tx.close()
    }
  }

  private def withHttpServer(path: String, test: Int => Unit): Unit = {
    val builder = new HttpServerTestSupportBuilder()
    builder.onPathReplyWithData(path, "".getBytes(StandardCharsets.UTF_8))
    val httpServer = builder.build()
    try {
      httpServer.start()
      test(httpServer.boundInfo.getPort)
    } finally {
      httpServer.stop()
    }
  }

}
