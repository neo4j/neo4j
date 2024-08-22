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
package org.neo4j.cypher

import org.assertj.core.api.Condition
import org.neo4j.collection.Dependencies
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.Transaction
import org.neo4j.graphdb.TransactionFailureException
import org.neo4j.graphdb.config.Setting
import org.neo4j.internal.helpers.collection.Iterables
import org.neo4j.internal.kernel.api.TokenRead
import org.neo4j.internal.kernel.api.procs.ProcedureSignature
import org.neo4j.internal.kernel.api.procs.QualifiedName
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.internal.schema.IndexProviderDescriptor
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.CypherScope
import org.neo4j.kernel.api.Kernel
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.index.IndexProvider
import org.neo4j.kernel.api.procedure.CallableProcedure
import org.neo4j.kernel.api.procedure.CallableUserAggregationFunction
import org.neo4j.kernel.api.procedure.CallableUserFunction
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.index.schema.AbstractIndexProviderFactory
import org.neo4j.kernel.impl.index.schema.BuiltInDelegatingIndexProviderFactory
import org.neo4j.logging.InternalLogProvider
import org.neo4j.logging.NullLogProvider
import org.neo4j.monitoring.Monitors
import org.neo4j.test.TestDatabaseManagementServiceBuilder
import org.neo4j.test.assertion.Assert.assertEventually
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

import java.nio.file.Path
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.util.Try

trait GraphDatabaseTestSupport
    extends GraphIcing with BeforeAndAfterEach {
  self: CypherFunSuite =>

  var graphOps: GraphDatabaseService = _
  var graph: GraphDatabaseCypherService = _
  var managementService: DatabaseManagementService = _
  var nodes: List[Node] = _
  protected var tx: InternalTransaction = _
  protected val registeredCallables: ArrayBuffer[QualifiedName] = ArrayBuffer.empty

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    startGraphDatabase()
  }

  def expectedShardCount: Int = 0

  def databaseConfig(): Map[Setting[_], Object] = Map(
    GraphDatabaseSettings.transaction_timeout -> Duration.ofMinutes(15),
    GraphDatabaseInternalSettings.enable_experimental_cypher_versions -> java.lang.Boolean.TRUE
  )

  def dependencies(): Option[Dependencies] = None

  def logProvider: InternalLogProvider = NullLogProvider.getInstance()

  /**
   * @return Some(url, databaseName) to load an existing database instead of creating an impermanent test database
   */
  def externalDatabase: Option[(String, String)] = None

  protected def startGraphDatabase(
    config: Map[Setting[_], Object] = databaseConfig(),
    maybeExternalDatabase: Option[(String, String)] = externalDatabase,
    maybeProvider: Option[(AbstractIndexProviderFactory[_ <: IndexProvider], IndexProviderDescriptor)] = None,
    maybeExternalPath: Option[Path] = None
  ): Unit = {
    val (databaseFactory, dbName) = (maybeExternalDatabase, maybeExternalPath) match {
      case (Some((url, databaseName)), _) =>
        (graphDatabaseFactory(Path.of(url)), databaseName)
      case (_, Some(path)) =>
        (graphDatabaseFactory(path), DEFAULT_DATABASE_NAME)
      case _ =>
        (graphDatabaseFactory(Path.of("root")).impermanent(), DEFAULT_DATABASE_NAME)
    }

    val updatedDatabaseFactory = maybeProvider match {
      case Some((providerFactory, descriptor)) =>
        databaseFactory.addExtension(new BuiltInDelegatingIndexProviderFactory(providerFactory, descriptor))
      case None => databaseFactory
    }

    dependencies().foreach(databaseFactory.setExternalDependencies(_))

    managementService =
      updatedDatabaseFactory.setConfig(config.asJava).setInternalLogProvider(logProvider).build()
    if (expectedShardCount > 0) {
      assertEventually(
        () => managementService.listDatabases().contains(dbName),
        new Condition[Boolean]((value: Boolean) => value, "Should be true."),
        60L,
        TimeUnit.SECONDS
      )
    }
    graphOps = managementService.database(dbName)
    if (expectedShardCount > 0) {
      assertEventually(
        () => {
          val onlineDatabaseCount: Long = managementService.database(SYSTEM_DATABASE_NAME).executeTransactionally(
            "SHOW DATABASES YIELD name, currentStatus WHERE currentStatus = 'online'",
            Map.empty.asJava,
            (result: Result) => result.stream.count()
          )
          onlineDatabaseCount >= expectedShardCount
        },
        new Condition[Boolean]((value: Boolean) => value, "Should be true."),
        60L,
        TimeUnit.SECONDS
      )
    }

    graph = new GraphDatabaseCypherService(graphOps)
    onNewGraphDatabase()
  }

  protected def onNewGraphDatabase(): Unit = ()
  protected def onDeletedGraphDatabase(): Unit = ()
  protected def onSelectDatabase(): Unit = ()

  protected def beginTransaction(`type`: KernelTransaction.Type, loginContext: LoginContext): InternalTransaction = {
    if (tx != null) {
      throw new TransactionFailureException(
        "Failed to start a new transaction. Already have an open transaction in `tx` in this test.",
        Status.Transaction.TransactionStartFailed
      )
    }
    tx = graph.beginTransaction(`type`, loginContext)
    tx
  }

  // Runs code inside of a transaction. Will mark the transaction as successful if no exception is thrown
  protected def inTestTx[T](f: InternalTransaction => T, txType: Type = Type.IMPLICIT): T = withTx(f, txType)

  protected def inTestTx[T](f: => T): T = inTestTx(_ => f)

  protected def withTx[T](f: InternalTransaction => T, txType: Type = Type.IMPLICIT): T = {
    if (tx == null) {
      graph.withTx(f, txType)
    } else {
      // Reuse the open transaction
      // (NOTE: Transaction Type is meaningless after the removal of placebo transaction anyway, so we do not care to check if it differs)
      f(tx)
    }
  }

  /**
   * Runs a block of code in a new transaction bound to [[tx]].
   * Calls to [[createNode]], [[relate]], etc will reuse that transaction.
   */
  protected def givenTx[T](f: => T): T = {
    beginTransaction(Type.IMPLICIT, LoginContext.AUTH_DISABLED)
    try {
      try f
      finally tx.commit()
    } finally {
      tx.close()
      tx = null
    }
  }

  protected def startGraphDatabase(storeDir: Path): Unit = {
    managementService = graphDatabaseFactory(storeDir).impermanent().build()
    graphOps = managementService.database(DEFAULT_DATABASE_NAME)
    graph = new GraphDatabaseCypherService(graphOps)
  }

  def selectDatabase(name: String): Unit = {
    graphOps = managementService.database(name)
    graph = new GraphDatabaseCypherService(graphOps)
    onSelectDatabase()
  }

  final protected def graphDatabaseFactory(databaseRootDir: Path): TestDatabaseManagementServiceBuilder = {
    val factory = createDatabaseFactory(databaseRootDir)
    this match {
      case custom: FakeClock =>
        factory.setClock(custom.clock)
      case _ =>
    }
    factory
  }

  /**
   * Override this method when you need an enterprise ManagementServiceBuilder
   */
  protected def createDatabaseFactory(databaseRootDir: Path): TestDatabaseManagementServiceBuilder =
    new TestDatabaseManagementServiceBuilder(databaseRootDir)

  protected def restartWithConfig(
    config: Map[Setting[_], Object] = databaseConfig(),
    maybeExternalPath: Option[Path] = None
  ): Unit = {
    managementService.shutdown()
    startGraphDatabase(config, maybeExternalPath = maybeExternalPath)
  }

  protected def restartWithIndexProvider(
    factory: AbstractIndexProviderFactory[_ <: IndexProvider],
    provider: IndexProviderDescriptor
  ): Unit = {
    managementService.shutdown()
    startGraphDatabase(maybeProvider = Some((factory, provider)))
  }

  override protected def afterEach(): Unit = {
    try {
      if (tx != null) {
        tx.close()
        tx = null
      }
      super.afterEach()
    } finally {
      if (managementService != null) {
        managementService.shutdown()
      }
      graphOps = null
      graph = null
      managementService = null
      nodes = null
      registeredCallables.clear()
      onDeletedGraphDatabase()
    }
  }

  def assertInTx(f: => Option[String]): Unit = {
    inTestTx {
      f match {
        case Some(error) => fail(error)
        case _           =>
      }
    }
  }

  def resampleIndexes(): Unit = {
    graph.withTx(_.execute("CALL db.prepareForReplanning"))
  }

  def tokenReader[T](tx: Transaction, f: TokenRead => T): T = {
    f(kernelTransaction(tx).tokenRead())
  }

  def kernelTransaction(tx: Transaction): KernelTransaction = {
    tx.asInstanceOf[InternalTransaction].kernelTransaction()
  }

  def nodeId(n: Node) = inTestTx {
    n.getId
  }

  def relationshipId(r: Relationship) = inTestTx {
    r.getId
  }

  def labels(n: Node) = inTestTx {
    n.getLabels.iterator().asScala.map(_.toString).toSet
  }

  def countNodes() = graph.withTx(tx => {
    Iterables.count(tx.getAllNodes).toInt
  })

  def countRelationships() = graph.withTx(tx => {
    Iterables.count(tx.getAllRelationships).toInt
  })

  def createNode(): Node = createNode(Map[String, Any]())

  def createNode(name: String): Node = createNode(Map[String, Any]("name" -> name))

  def createNode(props: Map[String, Any]): Node = {
    inTestTx { tx =>
      val node = tx.createNode()

      props.foreach((kv) => node.setProperty(kv._1, kv._2))
      node
    }
  }

  def createLabeledNode(props: Map[String, Any], labels: String*): Node = {
    inTestTx(tx => {
      val n = tx.createNode()
      labels.foreach {
        name => n.addLabel(Label.label(name))
      }

      props.foreach {
        case (k, v) => n.setProperty(k, v)
      }
      n
    })
  }

  def createLabeledNode(labels: String*): Node = createLabeledNode(Map[String, Any](), labels: _*)

  def createNode(values: (String, Any)*): Node = createNode(values.toMap)

  def deleteAllEntities(): Unit = {
    withTx(tx => {
      val relationships = tx.getAllRelationships
      try {
        val relIterator = relationships.iterator()

        while (relIterator.hasNext) {
          relIterator.next().delete()
        }
      } finally {
        relationships.close()
      }
      val allNodes = tx.getAllNodes
      try {
        val nodeIterator = allNodes.iterator()
        try {
          while (nodeIterator.hasNext) {
            nodeIterator.next().delete()
          }
        } finally {
          nodeIterator.close()
        }
      } finally {
        allNodes.close()
      }
    })
  }

  def nodeIds = nodes.map(_.getId).toArray

  val REL = RelationshipType.withName("REL")

  def relate(a: Node, b: Node): Relationship = relate(a, b, "REL")

  def relate(a: Node, b: Node, pk: (String, Any)*): Relationship = relate(a, b, "REL", pk.toMap)

  def relate(n1: Node, n2: Node, relType: String, name: String): Relationship =
    relate(n1, n2, relType, Map("name" -> name))

  def relate(n1: Node, n2: Node, relType: String, props: Map[String, Any] = Map()): Relationship = inTestTx(tx => {
    val r = tx.getNodeById(n1.getId).createRelationshipTo(tx.getNodeById(n2.getId), RelationshipType.withName(relType))
    props.foreach((kv) => r.setProperty(kv._1, kv._2))
    r
  })

  def relate(x: ((String, String), String)): Relationship = graph.withTx(tx => {
    x match {
      case ((from, relType), to) => {
        val f = tx.getNodeById(node(tx, from).getId)
        val t = tx.getNodeById(node(tx, to).getId)
        f.createRelationshipTo(t, RelationshipType.withName(relType))
      }
    }
  })

  def node(transaction: Transaction, name: String): Node = {
    nodes.find(n => transaction.getNodeById(n.getId).getProperty("name") == name).get
  }

  def relType(name: String): RelationshipType =
    graph.withTx(tx => tx.getAllRelationshipTypes.asScala.find(_.name() == name).get)

  def createNodes(names: String*): List[Node] = {
    nodes = names.map(x => createNode(Map("name" -> x))).toList
    nodes
  }

  def createDiamond(): (Node, Node, Node, Node) = {
    //    Graph:
    //             (a)
    //             / \
    //            v   v
    //          (b)  (c)
    //           \   /
    //            v v
    //            (d)

    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    val d = createNode("d")

    relate(a, b)
    relate(b, d)
    relate(a, c)
    relate(c, d)
    (a, b, c, d)
  }

  def asQualifiedName(fqn: String): QualifiedName = {
    val parts = fqn.split('.')
    if (fqn.isBlank) {
      throw new IllegalArgumentException("QualifiedName must not be a blank string")
    } else if (parts.length == 0) {
      throw new IllegalArgumentException("QualifiedName must contain symbols other than '.'")
    } else {
      new QualifiedName(parts.slice(0, parts.length - 1), parts(parts.length - 1))
    }
  }

  def registerProcedure[T <: CallableProcedure](fqn: String)(f: ProcedureSignature.Builder => T): T = {
    val builder = ProcedureSignature.procedureSignature(asQualifiedName(fqn))
    val proc = f(builder)
    kernelAPI.registerProcedure(proc)
    registeredCallables.addOne(proc.signature().name())
    proc
  }

  def registerUserDefinedFunction[T <: CallableUserFunction](fqn: String)(
    f: UserFunctionSignature.Builder => T
  ): T = {
    val builder = UserFunctionSignature.functionSignature(asQualifiedName(fqn))
    val func = f(builder)
    kernelAPI.registerUserFunction(func)
    registeredCallables.addOne(func.signature().name())
    func
  }

  def registerUserDefinedAggregationFunction[T <: CallableUserAggregationFunction](fqn: String)(
    f: UserFunctionSignature.Builder => T
  ): T = {
    val builder = UserFunctionSignature.functionSignature(asQualifiedName(fqn))
    val func = f(builder)
    kernelAPI.registerUserAggregationFunction(func)
    registeredCallables.addOne(func.signature().name())
    func
  }

  def getUserFunctionHandle(qualifiedName: String): UserFunctionHandle = {
    globalProcedures.getCurrentView.function(asQualifiedName(qualifiedName), CypherScope.CYPHER_5)
  }

  def kernelMonitors: Monitors = graph.getDependencyResolver.resolveDependency(classOf[Monitors])
  def globalProcedures: GlobalProcedures = graph.getDependencyResolver.resolveDependency(classOf[GlobalProcedures])

  private def kernelAPI: Kernel = graph.getDependencyResolver.resolveDependency(classOf[Kernel])

  case class haveConstraints(expectedConstraints: String*) extends Matcher[GraphDatabaseQueryService] {

    def apply(graph: GraphDatabaseQueryService): MatchResult = {
      graph.withTx(tx => {
        val constraintNames = tx.schema().getConstraints.asScala.toList.map(i =>
          s"${i.getConstraintType}:${i.getLabel}(${i.getPropertyKeys.asScala.toList.mkString(",")})"
        )
        val result = expectedConstraints.forall(i => constraintNames.contains(i.toString))
        MatchResult(
          result,
          s"Expected graph to have constraints ${expectedConstraints.mkString(", ")}, but it was ${constraintNames.mkString(", ")}",
          s"Expected graph to not have constraints ${expectedConstraints.mkString(", ")}, but it did."
        )
      })
    }
  }

  case class beAValidUUID() extends Matcher[AnyRef] {

    def apply(value: AnyRef): MatchResult = {
      MatchResult(
        Try(UUID.fromString(value.asInstanceOf[String])).isSuccess,
        s"""$value was not a valid UUID""",
        s"""$value was a valid UUID"""
      )
    }
  }
}
