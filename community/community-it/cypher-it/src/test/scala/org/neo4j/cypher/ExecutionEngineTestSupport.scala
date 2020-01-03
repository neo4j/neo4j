/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher

import java.util
import java.util.concurrent.TimeUnit

import org.neo4j.cypher.ExecutionEngineHelper.createEngine
import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.{TransactionBoundQueryContext, TransactionalContextWrapper}
import org.neo4j.cypher.internal.runtime.{InputDataStream, QueryContext, RuntimeJavaValueConverter, RuntimeScalaValueConverter}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.{CypherFunSuite, CypherTestSupport}
import org.neo4j.graphdb.{GraphDatabaseService, Result}
import org.neo4j.internal.schema.IndexDescriptor
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.query.{QueryExecutionEngine, RecordingQuerySubscriber}
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.logging.{LogProvider, NullLogProvider}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{MapValue, VirtualValues}

import scala.collection.JavaConverters._
import scala.collection.Map
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

trait ExecutionEngineTestSupport extends CypherTestSupport with ExecutionEngineHelper {
  self: CypherFunSuite with GraphDatabaseTestSupport =>

  var eengine: ExecutionEngine = _

  override protected def onNewGraphDatabase(): Unit = {
    eengine = createEngine(graph)
  }

  override protected def onDeletedGraphDatabase(): Unit = {
    eengine = null
  }

  override def executeScalar[T](q: String, params: (String, Any)*): T = try {
    super.executeScalar[T](q, params: _*)
  } catch {
    case e: ScalarFailureException => fail(e.getMessage)
  }

  protected def timeOutIn(length: Int, timeUnit: TimeUnit)(f: => Unit) {
    val future = Future {
      f
    }

    Await.result(future, Duration.apply(length, timeUnit))
  }
}

object ExecutionEngineHelper {
  def createEngine(db: GraphDatabaseService, logProvider: LogProvider): ExecutionEngine = {
    val service = new GraphDatabaseCypherService(db)
    createEngine(service, logProvider)
  }

  def createEngine(db: GraphDatabaseService): ExecutionEngine = {
    val service = new GraphDatabaseCypherService(db)
    createEngine(service, NullLogProvider.getInstance())
  }

  def createEngine(graphDatabaseCypherService: GraphDatabaseQueryService, logProvider: LogProvider = NullLogProvider.getInstance()): ExecutionEngine = {
    val resolver = graphDatabaseCypherService.getDependencyResolver
    resolver.resolveDependency(classOf[QueryExecutionEngine]).asInstanceOf[org.neo4j.cypher.internal.javacompat.ExecutionEngine].getCypherExecutionEngine
  }

  def asMapValue(map: Map[String, Any]): MapValue = {
    val keys = map.keys.toArray
    val values = map.values.map(asValue).toArray
    VirtualValues.map(keys, values)
  }

  def asMap(map: MapValue, context: QueryContext): java.util.Map[String, AnyRef] = {
    val out = new util.HashMap[String, AnyRef]()
    map.foreach((key: String, value: AnyValue) => {
      out.put(key, context.asObject(value))
    })
    out
  }

  def asValue(any: Any): AnyValue =
    any match {
      case map: Map[String, Any] => asMapValue(map)
      case array: Array[AnyRef] =>
        val value = Values.unsafeOf(array, false)
        if (value == null) VirtualValues.list(array.map(asValue):_*)
        else value
      case iterable: Iterable[_] => VirtualValues.list(iterable.map(asValue).toArray:_*)
      case traversable: TraversableOnce[_] => VirtualValues.list(traversable.map(asValue).toArray:_*)
      case x => ValueUtils.of(x)
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
  implicit val searchMonitor: IndexSearchMonitor = DummyIndexSearchMonitor

  private val converter = new RuntimeScalaValueConverter(_ => false)
  private val javaConverter = new RuntimeJavaValueConverter(_ => false)

  def graph: GraphDatabaseCypherService

  def eengine: ExecutionEngine

  def execute(q: String, params: (String, Any)*): RewindableExecutionResult = {
    execute(q, params.toMap)
  }

  def execute(q: String, params: Map[String, Any]): RewindableExecutionResult = {
    graph.withTx { tx =>
      execute(q, params, tx)
    }
  }

  def execute(q: String, params: Map[String, Any], tx: InternalTransaction): RewindableExecutionResult = {
    val subscriber = new RecordingQuerySubscriber
    val context = graph.transactionalContext(tx, query = q -> params.toMap)
    val tbqc = new TransactionBoundQueryContext(TransactionalContextWrapper(context))
    RewindableExecutionResult(eengine.execute(q,
      ExecutionEngineHelper.asMapValue(params),
      context,
      profile = false,
      prePopulate = false,
      subscriber),
      tbqc,
      subscriber)
  }

  def execute(fpq: FullyParsedQuery, params: Map[String, Any], input: InputDataStream): RewindableExecutionResult = {
    val subscriber = new RecordingQuerySubscriber
    graph.withTx { tx =>
      val context = graph.transactionalContext(tx, query = fpq.description -> params.toMap)
      val tbqc = new TransactionBoundQueryContext(TransactionalContextWrapper(context))
      RewindableExecutionResult(
        eengine.execute(
          query = fpq,
          params = ExecutionEngineHelper.asMapValue(params),
          context = context,
          prePopulate = false,
          input = input,
          subscriber = subscriber
        ),
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

case object DummyIndexSearchMonitor extends IndexSearchMonitor {

  override def indexSeek(index: IndexDescriptor, values: Seq[Any]): Unit = {}

  override def lockingUniqueIndexSeek(index: IndexDescriptor, values: Seq[Any]): Unit = {}
}
