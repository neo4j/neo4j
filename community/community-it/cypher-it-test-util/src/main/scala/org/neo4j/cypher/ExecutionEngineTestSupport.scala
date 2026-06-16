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

import org.neo4j.cypher.ExecutionEngineHelper.ParsedQuery
import org.neo4j.cypher.ExecutionEngineHelper.QueryType
import org.neo4j.cypher.ExecutionEngineHelper.TextQuery
import org.neo4j.cypher.ExecutionEngineHelper.asJavaMapDeep
import org.neo4j.cypher.ExecutionEngineHelper.createEngine
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.preparser.FullyParsedQuery
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.NoInput
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.RuntimeScalaValueConverter
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.TransactionalContextWrapper
import org.neo4j.cypher.util.GraphDatabaseCypherTestService
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Result
import org.neo4j.kernel.DeadlockDetectedException
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.query.QueryExecutionConfiguration
import org.neo4j.kernel.impl.query.QueryExecutionEngine
import org.neo4j.kernel.impl.query.QueryExecutionMonitor
import org.neo4j.kernel.impl.query.QuerySubscriberProbe
import org.neo4j.kernel.impl.query.RecordingQuerySubscriber
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.logging.InternalLogProvider
import org.neo4j.logging.NullLogProvider

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
  self: CypherITTestSuite & GraphDatabaseTestSupport =>

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
      super.executeScalar[T](q, params*)
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

  def createEngine(db: GraphDatabaseService, awaitSystemDatabase: Boolean): ExecutionEngine = {
    val service = new GraphDatabaseCypherTestService(db, awaitSystemDatabase)
    createEngine(service, NullLogProvider.getInstance())
  }

  def createEngine(
    graphDatabaseCypherService: GraphDatabaseQueryService,
    logProvider: InternalLogProvider = NullLogProvider.getInstance()
  ): ExecutionEngine = {
    val resolver = graphDatabaseCypherService.getDependencyResolver
    resolver.resolveDependency(classOf[QueryExecutionEngine]).asInstanceOf[
      org.neo4j.cypher.internal.javacompat.InternalQueryExecutionEngine
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

  sealed trait QueryType
  case class TextQuery(text: String) extends QueryType
  case class ParsedQuery(fpq: FullyParsedQuery) extends QueryType
}

protected class ScalarFailureException(msg: String) extends RuntimeException(msg)

trait ExecutionEngineHelper {
  self: GraphIcing =>
  implicit val searchMonitor: IndexSearchMonitor = IndexSearchMonitor.NOOP

  private val converter = new RuntimeScalaValueConverter(_ => false)

  protected def graph: GraphDatabaseCypherService

  protected def eengine: ExecutionEngine

  protected def executableQuery(query: QueryType): ExecutableQuery =
    ExecutableQuery(
      graph,
      query,
      Map.empty,
      QueryExecutionConfiguration.DEFAULT_CONFIG,
      tx = None,
      input = NoInput,
      deadlockRetry = false,
      monitor = None,
      maximumResultRows = None,
      databaseMode = TransactionalContext.DatabaseMode.SINGLE
    )

  protected def executableQuery(query: String): ExecutableQuery = executableQuery(TextQuery(query))
  protected def executableQuery(fpq: FullyParsedQuery): ExecutableQuery = executableQuery(ParsedQuery(fpq))

  protected def execute(q: String, params: (String, Any)*): RewindableExecutionResult =
    executableQuery(q).withParams(params.toMap).execute()

  protected def execute(q: String, params: Map[String, Any]): RewindableExecutionResult =
    executableQuery(q).withParams(params).execute()

  protected def executeWithQueryExecutionConfiguration(
    q: String,
    params: Map[String, Any],
    queryExecutionConfiguration: QueryExecutionConfiguration
  ): RewindableExecutionResult =
    executableQuery(q).withParams(params).withConfig(queryExecutionConfiguration).execute()

  protected def executeWithRetry(q: String, params: (String, Any)*): RewindableExecutionResult =
    executableQuery(q).withParams(params.toMap).withDeadlockRetry.execute()

  protected def execute(q: String, params: Map[String, Any], tx: InternalTransaction): RewindableExecutionResult =
    executableQuery(q).withParams(params).withTransaction(tx).execute()

  protected def execute(
    q: String,
    params: Map[String, Any],
    tx: InternalTransaction,
    config: QueryExecutionConfiguration
  ): RewindableExecutionResult =
    executableQuery(q).withParams(params).withTransaction(tx).withConfig(config).execute()

  protected def execute(
    fpq: FullyParsedQuery,
    params: Map[String, Any],
    input: InputDataStream
  ): RewindableExecutionResult =
    executableQuery(fpq).withParams(params).withInput(input).execute()

  protected def executeOfficial(tx: InternalTransaction, q: String, params: (String, Any)*): Result =
    executableQuery(q).withTransaction(tx).withParams(params.toMap).executeOfficial()

  protected def executeScalar[T](q: String, params: (String, Any)*): T =
    ExecutionEngineHelper.scalar[T](executableQuery(q).withParams(params.toMap).execute().toList)

  protected def asScalaResult(result: Result): Iterator[Map[String, Any]] = result.asScala.map(converter.asDeepScalaMap)

}

/** Parameter object representing a query to be executed */
case class ExecutableQuery(
  graph: GraphDatabaseQueryService,
  query: QueryType,
  params: Map[String, Any],
  queryExecutionConfiguration: QueryExecutionConfiguration,
  tx: Option[InternalTransaction],
  deadlockRetry: Boolean,
  input: InputDataStream,
  monitor: Option[QueryExecutionMonitor],
  maximumResultRows: Option[Long],
  databaseMode: TransactionalContext.DatabaseMode
)(implicit indexSearchMonitor: IndexSearchMonitor) extends GraphIcing {

  def withParams(params: Map[String, Any]): ExecutableQuery =
    copy(params = params)

  def withDeadlockRetry: ExecutableQuery = copy(deadlockRetry = true)

  def withTransaction(tx: InternalTransaction): ExecutableQuery = copy(tx = Some(tx))

  def withConfig(queryExecutionConfiguration: QueryExecutionConfiguration): ExecutableQuery =
    copy(queryExecutionConfiguration = queryExecutionConfiguration)

  def withGraph(graph: GraphDatabaseQueryService): ExecutableQuery =
    copy(graph = graph)

  def withInput(input: InputDataStream): ExecutableQuery =
    copy(input = input)

  def withMonitor(monitor: QueryExecutionMonitor): ExecutableQuery =
    copy(monitor = Some(monitor))

  def withMaximumResultRows(rows: Long): ExecutableQuery =
    copy(maximumResultRows = Some(rows))

  def withMaximumResultRows(rows: Option[Long]): ExecutableQuery =
    copy(maximumResultRows = rows)

  def withSpd(runOnSpd: Boolean): ExecutableQuery =
    copy(databaseMode =
      if (runOnSpd) TransactionalContext.DatabaseMode.SHARDED else TransactionalContext.DatabaseMode.SINGLE
    )

  private def execute(tx: InternalTransaction) = {
    val subscriber = maximumResultRows match {
      case Some(limit) => new RecordingQuerySubscriber(new QuerySubscriberProbe {
          var count = 0
          override def onRecordCompleted(): Unit = {
            count += 1
            if (count > limit) {
              throw new ResultRecordLimitExceededException(s"Exceeded result record limit of $limit")
            }
          }
        })
      case None => new RecordingQuerySubscriber
    }

    val eengine = ExecutionEngineHelper.createEngine(graph)
    query match {
      case ExecutionEngineHelper.TextQuery(text) =>
        val context =
          graph.transactionalContext(tx, query = text -> params.toMap, queryExecutionConfiguration, databaseMode)
        val tbqc = new TransactionBoundQueryContext(TransactionalContextWrapper(context), new ResourceManager)
        RewindableExecutionResult(
          eengine.execute(
            text,
            ValueUtils.asParameterMapValue(asJavaMapDeep(params)),
            context,
            profile = false,
            prePopulate = false,
            subscriber,
            monitor.getOrElse(eengine.defaultQueryExecutionMonitor)
          ),
          tbqc,
          subscriber
        )

      case ExecutionEngineHelper.ParsedQuery(fpq) =>
        val context = graph.transactionalContext(tx, query = fpq.description -> params.toMap, dbMode = databaseMode)
        val tbqc = new TransactionBoundQueryContext(TransactionalContextWrapper(context), new ResourceManager)
        RewindableExecutionResult(
          eengine.execute(
            query = fpq,
            params = ValueUtils.asParameterMapValue(asJavaMapDeep(params)),
            context = context,
            prePopulate = false,
            input = input,
            queryMonitor = monitor.getOrElse(eengine.defaultQueryExecutionMonitor),
            subscriber = subscriber
          ),
          tbqc,
          subscriber
        )
    }
  }

  private def withTx[T](f: InternalTransaction => T): T =
    tx match {
      case Some(tx) => f(tx)
      case None     => graph.withTx(f)
    }

  @tailrec
  private def retry(block: => RewindableExecutionResult): RewindableExecutionResult = {
    try {
      block
    } catch {
      case _: DeadlockDetectedException =>
        retry(block)
    }
  }

  def execute(): RewindableExecutionResult = {
    if (deadlockRetry) {
      retry(withTx(execute))
    } else {
      withTx(execute)
    }
  }

  def executeOfficial(): Result = {
    query match {
      case TextQuery(text) =>
        withTx(_.execute(text, asJavaMapDeep(params)))
      case ParsedQuery(_) => throw new IllegalArgumentException("Cannot execute fully parsed query via tx.execute")
    }
  }
}

class ResultRecordLimitExceededException(msg: String) extends Exception(msg)
