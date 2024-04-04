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

import org.neo4j.cypher.ExecutionEngineHelper.asJavaMapDeep
import org.neo4j.cypher.ExecutionEngineHelper.createEngine
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.FullyParsedQuery
import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.RuntimeJavaValueConverter
import org.neo4j.cypher.internal.runtime.RuntimeScalaValueConverter
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.TransactionalContextWrapper
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Result
import org.neo4j.kernel.DeadlockDetectedException
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.query.ExecutingQuery
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.query.QueryExecutionConfiguration
import org.neo4j.kernel.impl.query.QueryExecutionEngine
import org.neo4j.kernel.impl.query.QueryExecutionMonitor
import org.neo4j.kernel.impl.query.RecordingQuerySubscriber
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.logging.InternalLogProvider
import org.neo4j.logging.NullLogProvider

import java.util
import java.util.concurrent.TimeUnit

import scala.annotation.tailrec
import scala.collection.Map
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters.IterableHasAsJava
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava

trait ExecutionEngineTestSupport extends ExecutionEngineHelper {
  self: CypherFunSuite with GraphDatabaseTestSupport =>

  var eengine: ExecutionEngine = _

  override protected def onNewGraphDatabase(): Unit = {
    eengine = createEngine(graph)
  }

  override protected def onDeletedGraphDatabase(): Unit = {
    eengine = null
  }

  override protected def onSelectDatabase(): Unit = {
    eengine = createEngine(graph)
  }

  override def executeScalar[T](q: String, params: (String, Any)*): T =
    try {
      super.executeScalar[T](q, params: _*)
    } catch {
      case e: ScalarFailureException => fail(e.getMessage)
    }

  protected def timeOutIn(length: Int, timeUnit: TimeUnit)(f: => Unit): Unit = {
    val future = Future {
      f
    }

    Await.result(future, Duration.apply(length, timeUnit))
  }
}

object ExecutionEngineHelper {

  def createEngine(db: GraphDatabaseService, logProvider: InternalLogProvider): ExecutionEngine = {
    val service = new GraphDatabaseCypherService(db)
    createEngine(service, logProvider)
  }

  def createEngine(db: GraphDatabaseService): ExecutionEngine = {
    val service = new GraphDatabaseCypherService(db)
    createEngine(service, NullLogProvider.getInstance())
  }

  def createEngine(
    graphDatabaseCypherService: GraphDatabaseQueryService,
    logProvider: InternalLogProvider = NullLogProvider.getInstance()
  ): ExecutionEngine = {
    val resolver = graphDatabaseCypherService.getDependencyResolver
    resolver.resolveDependency(classOf[QueryExecutionEngine]).asInstanceOf[
      org.neo4j.cypher.internal.javacompat.ExecutionEngine
    ].getCypherExecutionEngine
  }

  def asJavaMapDeep(map: Map[String, Any]): java.util.Map[String, AnyRef] = {
    map.view.mapValues(asJavaValueDeep).toMap.asJava
  }

  def asJavaValueDeep(any: Any): AnyRef =
    any match {
      case map: Map[_, _]                => asJavaMapDeep(map.asInstanceOf[Map[String, Any]])
      case array: Array[Any]             => array.map(asJavaValueDeep)
      case iterable: Iterable[_]         => iterable.map(asJavaValueDeep).asJava
      case iterableOnce: IterableOnce[_] => iterableOnce.map(asJavaValueDeep).toList.asJava
      case x                             => x.asInstanceOf[AnyRef]
    }

  private def scalar[T](input: List[Map[String, Any]]): T = input match {
    case m :: Nil =>
      if (m.size != 1)
        throw new ScalarFailureException(s"expected scalar value: $m")
      else {
        val value: Any = m.head._2
        value.asInstanceOf[T]
      }
    case x => throw new ScalarFailureException(s"expected to get a single row back, got: $x")
  }
}

protected class ScalarFailureException(msg: String) extends RuntimeException(msg)

trait ExecutionEngineHelper {
  self: GraphIcing =>
  implicit val searchMonitor: IndexSearchMonitor = IndexSearchMonitor.NOOP

  private val converter = new RuntimeScalaValueConverter(_ => false)
  private val javaConverter = new RuntimeJavaValueConverter(_ => false)

  def graph: GraphDatabaseCypherService

  def eengine: ExecutionEngine

  def execute(q: String, params: (String, Any)*): RewindableExecutionResult = {
    execute(q, params.toMap)
  }

  def execute(q: String, params: Map[String, Any]): RewindableExecutionResult = {
    executeWithQueryExecutionConfiguration(q, params, QueryExecutionConfiguration.DEFAULT_CONFIG)
  }

  def executeWithQueryExecutionConfiguration(
    q: String,
    params: Map[String, Any],
    queryExecutionConfiguration: QueryExecutionConfiguration
  ): RewindableExecutionResult = {
    graph.withTx { tx =>
      execute(q, params, tx, queryExecutionConfiguration)
    }
  }

  def executeWithRetry(q: String, params: (String, Any)*): RewindableExecutionResult = {
    executeWithRetry(q, params.toMap)
  }

  @tailrec
  final def executeWithRetry(q: String, params: Map[String, Any]): RewindableExecutionResult = {
    try {
      execute(q, params)
    } catch {
      case _: DeadlockDetectedException =>
        executeWithRetry(q, params)
    }
  }

  def execute(q: String, params: Map[String, Any], tx: InternalTransaction): RewindableExecutionResult = {
    execute(q, params, tx, QueryExecutionConfiguration.DEFAULT_CONFIG)
  }

  def execute(
    q: String,
    params: Map[String, Any],
    tx: InternalTransaction,
    queryExecutionConfiguration: QueryExecutionConfiguration
  ): RewindableExecutionResult = {
    val subscriber = new RecordingQuerySubscriber
    val context = graph.transactionalContext(tx, query = q -> params.toMap, queryExecutionConfiguration)
    val tbqc = new TransactionBoundQueryContext(TransactionalContextWrapper(context), new ResourceManager)
    RewindableExecutionResult(
      eengine.execute(
        q,
        ValueUtils.asParameterMapValue(asJavaMapDeep(params)),
        context,
        profile = false,
        prePopulate = false,
        subscriber
      ),
      context,
      tbqc,
      subscriber
    )
  }

  def execute(fpq: FullyParsedQuery, params: Map[String, Any], input: InputDataStream): RewindableExecutionResult = {
    val subscriber = new RecordingQuerySubscriber
    graph.withTx { tx =>
      val context = graph.transactionalContext(tx, query = fpq.description -> params.toMap)
      val tbqc = new TransactionBoundQueryContext(TransactionalContextWrapper(context), new ResourceManager)
      RewindableExecutionResult(
        eengine.execute(
          query = fpq,
          params = ValueUtils.asParameterMapValue(asJavaMapDeep(params)),
          context = context,
          prePopulate = false,
          input = input,
          queryMonitor = DummyQueryExecutionMonitor,
          subscriber = subscriber
        ),
        context,
        tbqc,
        subscriber
      )
    }
  }

  def executeOfficial(tx: InternalTransaction, q: String, params: (String, Any)*): Result =
    tx.execute(q, javaConverter.asDeepJavaMap(params.toMap).asInstanceOf[util.Map[String, AnyRef]])

  def executeScalar[T](q: String, params: (String, Any)*): T = {
    ExecutionEngineHelper.scalar[T](execute(q, params: _*).toList)
  }

  def asScalaResult(result: Result): Iterator[Map[String, Any]] = result.asScala.map(converter.asDeepScalaMap)
}

case object DummyQueryExecutionMonitor extends QueryExecutionMonitor {
  override def startProcessing(query: ExecutingQuery): Unit = {}
  override def startExecution(query: ExecutingQuery): Unit = {}
  override def endFailure(query: ExecutingQuery, failure: Throwable): Unit = {}
  override def endFailure(query: ExecutingQuery, reason: String, status: Status): Unit = {}
  override def endSuccess(query: ExecutingQuery): Unit = {}
}
