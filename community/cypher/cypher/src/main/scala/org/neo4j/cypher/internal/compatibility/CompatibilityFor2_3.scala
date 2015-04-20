/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compatibility

import java.io.PrintWriter
import java.util

import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compiler.v2_3
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{ExecutionPlan => ExecutionPlan_v2_3, InternalExecutionResult}
import org.neo4j.cypher.internal.compiler.v2_3.notification.{CartesianProductNotification, InternalNotification, LegacyPlannerNotification}
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.{Argument, InternalPlanDescription, PlanDescriptionArgumentSerializer}
import org.neo4j.cypher.internal.compiler.v2_3.spi.MapToPublicExceptions
import org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.compiler.v2_3.{CypherCompilerFactory, CypherException => CypherException_v2_3, InfoLogger, InputPosition => CompilerInputPosition, Monitors, PlannerName, _}
import org.neo4j.cypher.internal.spi.v2_3.{TransactionBoundGraphStatistics, TransactionBoundPlanContext, TransactionBoundQueryContext}
import org.neo4j.cypher.javacompat.ProfilerStatistics
import org.neo4j.cypher.{ArithmeticException, CypherTypeException, EntityNotFoundException, FailedIndexException, IncomparableValuesException, IndexHintException, InternalException, InvalidArgumentException, InvalidSemanticsException, LabelScanHintException, LoadCsvStatusWrapCypherException, LoadExternalResourceException, MergeConstraintConflictException, NodeStillHasRelationshipsException, ParameterNotFoundException, ParameterWrongTypeException, PatternException, PeriodicCommitInOpenTransactionException, ProfilerStatisticsNotReadyException, SyntaxException, UniquePathNotUniqueException, UnknownLabelException, _}
import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.graphdb.impl.notification.NotificationCode
import org.neo4j.graphdb.{GraphDatabaseService, InputPosition, QueryExecutionType, ResourceIterator}
import org.neo4j.helpers.{Assertion, Clock}
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.api.{KernelAPI, Statement}
import org.neo4j.kernel.impl.query.{QueryExecutionMonitor, QuerySession}
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.Log

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.Try

object helpersv2_3 {
  implicit def monitorFailure(t: Throwable)(implicit monitor: QueryExecutionMonitor, session: QuerySession): Unit = {
    monitor.endFailure(session, t)
  }
}

object exceptionHandlerFor2_3 extends MapToPublicExceptions[CypherException] {
  def syntaxException(message: String, query: String, offset: Option[Int]) = new SyntaxException(message, query, offset)

  def arithmeticException(message: String, cause: Throwable) = new ArithmeticException(message, cause)

  def profilerStatisticsNotReadyException() = {
    throw new ProfilerStatisticsNotReadyException()
  }

  def incomparableValuesException(lhs: String, rhs: String) = new IncomparableValuesException(lhs, rhs)

  def unknownLabelException(s: String) = new UnknownLabelException(s)

  def patternException(message: String) = new PatternException(message)

  def invalidArgumentException(message: String, cause: Throwable) = new InvalidArgumentException(message, cause)

  def mergeConstraintConflictException(message: String) = new MergeConstraintConflictException(message)

  def internalException(message: String) = new InternalException(message)

  def missingConstraintException() = new MissingConstraintException

  def loadCsvStatusWrapCypherException(extraInfo: String, cause: CypherException_v2_3) =
    new LoadCsvStatusWrapCypherException(extraInfo, cause.mapToPublic(exceptionHandlerFor2_3))

  def loadExternalResourceException(message: String, cause: Throwable) = throw new LoadExternalResourceException(message, cause)

  def parameterNotFoundException(message: String, cause: Throwable) = throw new ParameterNotFoundException(message, cause)

  def uniquePathNotUniqueException(message: String) = throw new UniquePathNotUniqueException(message)

  def entityNotFoundException(message: String, cause: Throwable) = throw new EntityNotFoundException(message, cause)

  def cypherTypeException(message: String, cause: Throwable) = throw new CypherTypeException(message, cause)

  def labelScanHintException(identifier: String, label: String, message: String) = throw new LabelScanHintException(identifier, label, message)

  def invalidSemanticException(message: String) = throw new InvalidSemanticsException(message)

  def parameterWrongTypeException(message: String, cause: Throwable) = throw new ParameterWrongTypeException(message, cause)

  def outOfBoundsException(message: String) = throw new OutOfBoundsException(message)

  def nodeStillHasRelationshipsException(nodeId: Long, cause: Throwable) = throw new NodeStillHasRelationshipsException(nodeId, cause)

  def indexHintException(identifier: String, label: String, property: String, message: String) = throw new IndexHintException(identifier, label, property, message)

  def periodicCommitInOpenTransactionException() = throw new PeriodicCommitInOpenTransactionException

  def runSafely[T](body: => T)(implicit f: Throwable => Unit = (_) => ()) = {
    try {
      body
    }
    catch {
      case e: CypherException_v2_3 =>
        f(e)
        throw e.mapToPublic(exceptionHandlerFor2_3)
      case e: Throwable =>
        f(e)
        throw e
    }
  }

  def failedIndexException(indexName: String): CypherException = throw new FailedIndexException(indexName)
}

case class WrappedMonitors2_3(kernelMonitors: KernelMonitors) extends Monitors {
  def addMonitorListener[T](monitor: T, tags: String*) {
    kernelMonitors.addMonitorListener(monitor, tags: _*)
  }

  def newMonitor[T <: AnyRef : ClassTag](tags: String*): T = {
    val clazz = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    kernelMonitors.newMonitor(clazz, tags: _*)
  }
}

trait CompatibilityFor2_3 {
  import org.neo4j.cypher.internal.compatibility.helpers._

  val graph: GraphDatabaseService
  val queryCacheSize: Int
  val kernelMonitors: KernelMonitors
  val kernelAPI: KernelAPI

  protected val rewriterSequencer: (String) => RewriterStepSequencer = {
    import org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters.RewriterStepSequencer._
    import org.neo4j.helpers.Assertion._

    if (assertionsEnabled()) newValidating else newPlain
  }

  protected val compiler: v2_3.CypherCompiler

  implicit val executionMonitor = kernelMonitors.newMonitor(classOf[QueryExecutionMonitor])

  def produceParsedQuery(preParsedQuery: PreParsedQuery, offset: CompilerInputPosition) = new ParsedQuery {
    val preparedQueryForV_2_3 = Try(compiler.prepareQuery(preParsedQuery.statement, preParsedQuery.notificationLogger, Some(offset)))

    def isPeriodicCommit = preparedQueryForV_2_3.map(_.isPeriodicCommit).getOrElse(false)

    def plan(statement: Statement): (ExecutionPlan, Map[String, Any]) = exceptionHandlerFor2_3.runSafely {
      val planContext = new TransactionBoundPlanContext(statement, graph)
      val (planImpl, extractedParameters) = compiler.planPreparedQuery(preparedQueryForV_2_3.get, planContext)
      (new ExecutionPlanWrapper(planImpl), extractedParameters)
    }
  }

  class ExecutionPlanWrapper(inner: ExecutionPlan_v2_3) extends ExecutionPlan {

    private def queryContext(graph: GraphDatabaseAPI, txInfo: TransactionInfo) = {
      val ctx = new TransactionBoundQueryContext(graph, txInfo.tx, txInfo.isTopLevelTx, txInfo.statement)
      new ExceptionTranslatingQueryContextFor2_3(ctx)
    }

    def run(graph: GraphDatabaseAPI, txInfo: TransactionInfo, executionMode: ExecutionMode, params: Map[String, Any], session: QuerySession): ExtendedExecutionResult = {
      implicit val s = session
      exceptionHandlerFor2_3.runSafely {
        ExecutionResultWrapperFor2_3(inner.run(queryContext(graph, txInfo), txInfo.statement, executionMode, params), inner.plannerUsed, inner.runtimeUsed)
      }
    }

    def isPeriodicCommit = inner.isPeriodicCommit

    def isStale(lastTxId: () => Long, statement: Statement) =
      inner.isStale(lastTxId, TransactionBoundGraphStatistics(statement))
  }
}

case class ExecutionResultWrapperFor2_3(inner: InternalExecutionResult, planner: PlannerName, runtime: RuntimeName)(implicit monitor: QueryExecutionMonitor, session: QuerySession) extends ExtendedExecutionResult {

  import org.neo4j.cypher.internal.compatibility.helpers._
  def planDescriptionRequested = exceptionHandlerFor2_3.runSafely {inner.planDescriptionRequested}

  private def endQueryExecution() = {
    monitor.endSuccess(session) // this method is expected to be idempotent
  }

  def javaIterator: ResourceIterator[util.Map[String, Any]] = {
    val innerJavaIterator = inner.javaIterator
    exceptionHandlerFor2_3.runSafely {
      if ( !innerJavaIterator.hasNext ) {
        endQueryExecution()
      }
    }
    new ResourceIterator[util.Map[String, Any]] {
      def close() = exceptionHandlerFor2_3.runSafely {
        endQueryExecution()
        innerJavaIterator.close()
      }
      def next() = exceptionHandlerFor2_3.runSafely {innerJavaIterator.next}
      def hasNext = exceptionHandlerFor2_3.runSafely{
        val next = innerJavaIterator.hasNext
        if (!next) {
          endQueryExecution()
        }
        next
      }
      def remove() =  exceptionHandlerFor2_3.runSafely{innerJavaIterator.remove()}
    }
  }

  def columnAs[T](column: String) = exceptionHandlerFor2_3.runSafely{inner.columnAs[T](column)}

  def columns = exceptionHandlerFor2_3.runSafely{inner.columns}

  def javaColumns = exceptionHandlerFor2_3.runSafely{inner.javaColumns}

  def queryStatistics() = exceptionHandlerFor2_3.runSafely {
    val i = inner.queryStatistics()
    QueryStatistics(nodesCreated = i.nodesCreated,
      relationshipsCreated = i.relationshipsCreated,
      propertiesSet = i.propertiesSet,
      nodesDeleted = i.nodesDeleted,
      relationshipsDeleted = i.relationshipsDeleted,
      labelsAdded = i.labelsAdded,
      labelsRemoved = i.labelsRemoved,
      indexesAdded = i.indexesAdded,
      indexesRemoved = i.indexesRemoved,
      constraintsAdded = i.constraintsAdded,
      constraintsRemoved = i.constraintsRemoved)
  }

  def dumpToString(writer: PrintWriter) = exceptionHandlerFor2_3.runSafely{inner.dumpToString(writer)}

  def dumpToString() = exceptionHandlerFor2_3.runSafely{inner.dumpToString()}

  def javaColumnAs[T](column: String) = exceptionHandlerFor2_3.runSafely{inner.javaColumnAs[T](column)}

  def executionPlanDescription(): ExtendedPlanDescription =
    exceptionHandlerFor2_3.runSafely {
      convert(
        inner.executionPlanDescription().
          addArgument(Version("CYPHER 2.3")).
          addArgument(Planner(planner.name)).
          addArgument(Runtime(runtime.name))
    )
  }

  def close() = exceptionHandlerFor2_3.runSafely{ inner.close() }

  def next() = exceptionHandlerFor2_3.runSafely{ inner.next() }

  def hasNext = exceptionHandlerFor2_3.runSafely{ inner.hasNext }

  def convert(i: InternalPlanDescription): ExtendedPlanDescription = exceptionHandlerFor2_3.runSafely {
    CompatibilityPlanDescriptionFor2_3(i, CypherVersion.v2_3, planner, runtime)
  }

  def executionType: QueryExecutionType = exceptionHandlerFor2_3.runSafely {inner.executionType}

  def notifications = inner.notifications.map(asKernelNotification)

  private def asKernelNotification(notification: InternalNotification) = notification match {
    case CartesianProductNotification(pos) =>
       NotificationCode.CARTESIAN_PRODUCT.notification(new InputPosition(pos.offset, pos.line, pos.column))
    case LegacyPlannerNotification =>
      NotificationCode.LEGACY_PLANNER.notification(InputPosition.empty)
  }

  override def accept[EX <: Exception](visitor: ResultVisitor[EX]) = exceptionHandlerFor2_3.runSafely {inner.accept(visitor)}
}

case class CompatibilityPlanDescriptionFor2_3(inner: InternalPlanDescription, version: CypherVersion, planner: PlannerName, runtime: RuntimeName)
  extends ExtendedPlanDescription {

  self =>
  def children = extendedChildren

  def extendedChildren = exceptionHandlerFor2_3.runSafely {
    inner.children.toSeq.map(CompatibilityPlanDescriptionFor2_3.apply(_, version, planner, runtime))
  }

  def arguments: Map[String, AnyRef] = exceptionHandlerFor2_3.runSafely {
    inner.arguments.map { arg => arg.name -> PlanDescriptionArgumentSerializer.serialize(arg) }.toMap
  }

  def identifiers = exceptionHandlerFor2_3.runSafely { inner.orderedIdentifiers.toSet }

  def hasProfilerStatistics = exceptionHandlerFor2_3.runSafely { inner.arguments.exists(_.isInstanceOf[DbHits]) }

  def name = exceptionHandlerFor2_3.runSafely { inner.name }

  def asJava: javacompat.PlanDescription = exceptionHandlerFor2_3.runSafely { asJava(self) }

  override def toString: String = {
    val NL = System.lineSeparator()
    exceptionHandlerFor2_3.runSafely { s"Compiler CYPHER ${version.name}$NL${NL}Planner ${planner.name.toUpperCase}$NL${NL}Runtime ${runtime.name.toUpperCase}$NL$NL$inner" }
  }

  def asJava(in: ExtendedPlanDescription): javacompat.PlanDescription = new javacompat.PlanDescription {
    def getProfilerStatistics: ProfilerStatistics = new ProfilerStatistics {
      def getDbHits: Long = extract { case DbHits(count) => count}

      def getRows: Long = extract { case Rows(count) => count}

      private def extract(f: PartialFunction[Argument, Long]): Long =
        inner.arguments.collectFirst(f).getOrElse(throw new InternalException("Don't have profiler stats"))
    }

    def getName: String = name

    def hasProfilerStatistics: Boolean = self.hasProfilerStatistics

    def getArguments: util.Map[String, AnyRef] = arguments.asJava

    def getIdentifiers: util.Set[String] = identifiers.asJava

    def getChildren: util.List[javacompat.PlanDescription] = in.extendedChildren.toList.map(_.asJava).asJava

    override def toString: String = self.toString
  }
}

class StringInfoLogger2_3(log: Log) extends InfoLogger {
  def info(message: String) {
    log.info(message)
  }
}
case class CompatibilityFor2_3Cost(graph: GraphDatabaseService,
                                           queryCacheSize: Int,
                                           statsDivergenceThreshold: Double,
                                           queryPlanTTL: Long,
                                           clock: Clock,
                                           kernelMonitors: KernelMonitors,
                                           kernelAPI: KernelAPI,
                                           log: Log,
                                           plannerName: CostBasedPlannerName,
                                           runtimeName: RuntimeName) extends CompatibilityFor2_3 {
  protected val compiler = CypherCompilerFactory.costBasedCompiler(
    graph, queryCacheSize, statsDivergenceThreshold, queryPlanTTL, clock, new WrappedMonitors2_3( kernelMonitors ),
    new StringInfoLogger2_3( log ), rewriterSequencer, plannerName, runtimeName
  )
}

case class CompatibilityFor2_3Rule(graph: GraphDatabaseService,
                                   queryCacheSize: Int,
                                   statsDivergenceThreshold: Double,
                                   queryPlanTTL: Long,
                                   clock: Clock,
                                   kernelMonitors: KernelMonitors,
                                   kernelAPI: KernelAPI) extends CompatibilityFor2_3 {
  protected val compiler = CypherCompilerFactory.ruleBasedCompiler(
    graph, queryCacheSize, statsDivergenceThreshold, queryPlanTTL, clock, new WrappedMonitors2_3( kernelMonitors ), rewriterSequencer)
}
