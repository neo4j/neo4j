/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{EntityAccessor, ExecutionPlan => ExecutionPlan_v2_3, InternalExecutionResult}
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.{Argument, InternalPlanDescription, PlanDescriptionArgumentSerializer}
import org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.compiler.v2_3.{CypherCompilerFactory, DPPlannerName, ExplainMode => ExplainModev2_3, GreedyPlannerName, IDPPlannerName, InfoLogger, Monitors, NormalMode => NormalModev2_3, PlannerName, ProfileMode => ProfileModev2_3, _}
import org.neo4j.cypher.internal.frontend.v2_3.notification.{InternalNotification, LegacyPlannerNotification, PlannerUnsupportedNotification, RuntimeUnsupportedNotification, _}
import org.neo4j.cypher.internal.frontend.v2_3.spi.MapToPublicExceptions
import org.neo4j.cypher.internal.frontend.v2_3.{CypherException => InternalCypherException}
import org.neo4j.cypher.internal.spi.v2_3.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.spi.v2_3.{TransactionBoundGraphStatistics, TransactionBoundPlanContext, TransactionBoundQueryContext}
import org.neo4j.cypher.javacompat.ProfilerStatistics
import org.neo4j.cypher.{QueryStatistics, _}
import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.graphdb._
import org.neo4j.graphdb.impl.notification.{NotificationCode, NotificationDetail}
import org.neo4j.helpers.Clock
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.api.{KernelAPI, Statement}
import org.neo4j.kernel.impl.core.NodeManager
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
  def syntaxException(message: String, query: String, offset: Option[Int], cause: Throwable) = new SyntaxException(message, query, offset, cause)

  def arithmeticException(message: String, cause: Throwable) = new ArithmeticException(message, cause)

  def profilerStatisticsNotReadyException(cause: Throwable) = {
    throw new ProfilerStatisticsNotReadyException(cause)
  }

  def incomparableValuesException(details: Option[String], lhs: String, rhs: String, cause: Throwable): CypherException = new IncomparableValuesException(details, lhs, rhs, cause)

  def unknownLabelException(s: String, cause: Throwable) = new UnknownLabelException(s, cause)

  def patternException(message: String, cause: Throwable) = new PatternException(message, cause)

  def invalidArgumentException(message: String, cause: Throwable) = new InvalidArgumentException(message, cause)

  def mergeConstraintConflictException(message: String, cause: Throwable) = new MergeConstraintConflictException(message, cause)

  def internalException(message: String, cause: Exception) = new InternalException(message, cause)

  def loadCsvStatusWrapCypherException(extraInfo: String, cause: InternalCypherException) =
    new LoadCsvStatusWrapCypherException(extraInfo, cause.mapToPublic(exceptionHandlerFor2_3))

  def loadExternalResourceException(message: String, cause: Throwable) = throw new LoadExternalResourceException(message, cause)

  def parameterNotFoundException(message: String, cause: Throwable) = throw new ParameterNotFoundException(message, cause)

  def uniquePathNotUniqueException(message: String, cause: Throwable) = throw new UniquePathNotUniqueException(message, cause)

  def entityNotFoundException(message: String, cause: Throwable) = throw new EntityNotFoundException(message, cause)

  def hintException(message: String, cause: Throwable) = throw new HintException(message, cause)

  def cypherTypeException(message: String, cause: Throwable) = throw new CypherTypeException(message, cause)

  def cypherExecutionException(message: String, cause: Throwable) = throw new CypherExecutionException(message, cause)

  def labelScanHintException(identifier: String, label: String, message: String, cause: Throwable) = throw new LabelScanHintException(identifier, label, message, cause)

  def invalidSemanticException(message: String, cause: Throwable) = throw new InvalidSemanticsException(message, cause)

  def parameterWrongTypeException(message: String, cause: Throwable) = throw new ParameterWrongTypeException(message, cause)

  def nodeStillHasRelationshipsException(nodeId: Long, cause: Throwable) = throw new NodeStillHasRelationshipsException(nodeId, cause)

  def indexHintException(identifier: String, label: String, property: String, message: String, cause: Throwable) = throw new IndexHintException(identifier, label, property, message, cause)

  def joinHintException(identifier: String, message: String, cause: Throwable) = throw new JoinHintException(identifier, message, cause)

  def periodicCommitInOpenTransactionException(cause: Throwable) = throw new PeriodicCommitInOpenTransactionException(cause)

  def failedIndexException(indexName: String, cause: Throwable): CypherException = throw new FailedIndexException(indexName, cause)

  def runSafely[T](body: => T)(implicit f: Throwable => Unit = (_) => ()) = {
    try {
      body
    }
    catch {
      case e: InternalCypherException =>
        f(e)
        throw e.mapToPublic(exceptionHandlerFor2_3)
      case e: Throwable =>
        f(e)
        throw e
    }
  }
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

  def produceParsedQuery(preParsedQuery: PreParsedQuery, tracer: CompilationPhaseTracer) = {
    val preparedQueryForV_2_3 =
      Try(compiler.prepareQuery(preParsedQuery.statement,
                                preParsedQuery.rawStatement,
                                preParsedQuery.notificationLogger,
                                preParsedQuery.planner.name,
                                Some(preParsedQuery.offset), tracer))
    new ParsedQuery {
      def isPeriodicCommit = preparedQueryForV_2_3.map(_.isPeriodicCommit).getOrElse(false)

      def plan(statement: Statement, tracer: CompilationPhaseTracer): (ExecutionPlan, Map[String, Any]) = exceptionHandlerFor2_3.runSafely {
        val planContext = new TransactionBoundPlanContext(statement, graph)
        val (planImpl, extractedParameters) = compiler.planPreparedQuery(preparedQueryForV_2_3.get, planContext, tracer)

        // Log notifications/warnings from planning
        planImpl.notifications(planContext).foreach(preParsedQuery.notificationLogger += _)

        (new ExecutionPlanWrapper(planImpl), extractedParameters)
      }

      override def hasErrors = preparedQueryForV_2_3.isFailure
    }
  }

  class ExecutionPlanWrapper(inner: ExecutionPlan_v2_3) extends ExecutionPlan {

    private def queryContext(graph: GraphDatabaseAPI, txInfo: TransactionInfo) = {
      val searchMonitor: IndexSearchMonitor = kernelMonitors.newMonitor(classOf[IndexSearchMonitor])
      val ctx = new TransactionBoundQueryContext(graph, txInfo.tx, txInfo.isTopLevelTx, txInfo.statement)(searchMonitor)
      new ExceptionTranslatingQueryContextFor2_3(ctx)
    }

    def run(graph: GraphDatabaseAPI, txInfo: TransactionInfo, executionMode: CypherExecutionMode, params: Map[String, Any], session: QuerySession): ExtendedExecutionResult = {
      implicit val s = session
      val innerExecutionMode = executionMode match {
        case CypherExecutionMode.explain => ExplainModev2_3
        case CypherExecutionMode.profile => ProfileModev2_3
        case CypherExecutionMode.normal => NormalModev2_3
      }
      exceptionHandlerFor2_3.runSafely {
        ExecutionResultWrapperFor2_3(inner.run(queryContext(graph, txInfo), txInfo.statement, innerExecutionMode, params), inner.plannerUsed, inner.runtimeUsed)
      }
    }

    def isPeriodicCommit = inner.isPeriodicCommit

    def isStale(lastCommittedTxId: LastCommittedTxIdProvider, statement: Statement): Boolean =
      inner.isStale(lastCommittedTxId, TransactionBoundGraphStatistics(statement))
  }
}

case class ExecutionResultWrapperFor2_3(inner: InternalExecutionResult, planner: PlannerName, runtime: RuntimeName)
                                       (implicit monitor: QueryExecutionMonitor, session: QuerySession)
  extends ExtendedExecutionResult {

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
      constraintsAdded = i.uniqueConstraintsAdded + i.existenceConstraintsAdded,
      constraintsRemoved = i.uniqueConstraintsRemoved + i.existenceConstraintsRemoved
    )
  }

  def dumpToString(writer: PrintWriter) = exceptionHandlerFor2_3.runSafely{inner.dumpToString(writer)}

  def dumpToString() = exceptionHandlerFor2_3.runSafely{inner.dumpToString()}

  def javaColumnAs[T](column: String) = exceptionHandlerFor2_3.runSafely{inner.javaColumnAs[T](column)}

  def executionPlanDescription(): ExtendedPlanDescription =
    exceptionHandlerFor2_3.runSafely {
      convert(
        inner.executionPlanDescription().
          addArgument(Version("CYPHER 2.3")).
          addArgument(Planner(planner.toTextOutput)).
          addArgument(PlannerImpl(planner.name)).
          addArgument(Runtime(runtime.toTextOutput)).
          addArgument(RuntimeImpl(runtime.name))
    )
  }

  def close() = exceptionHandlerFor2_3.runSafely {
    endQueryExecution()
    inner.close()
  }

  def next() = exceptionHandlerFor2_3.runSafely{ inner.next() }

  def hasNext = exceptionHandlerFor2_3.runSafely {
    val next = inner.hasNext
    if (!next) {
      endQueryExecution()
    }
    next
  }

  def convert(i: InternalPlanDescription): ExtendedPlanDescription = exceptionHandlerFor2_3.runSafely {
    CompatibilityPlanDescriptionFor2_3(i, CypherVersion.v2_3, planner, runtime)
  }

  def executionType: QueryExecutionType = exceptionHandlerFor2_3.runSafely {inner.executionType}

  def notifications = inner.notifications.map(asKernelNotification)

  private def asKernelNotification(notification: InternalNotification) = notification match {
    case CartesianProductNotification(pos, identifiers) =>
      NotificationCode.CARTESIAN_PRODUCT.notification(pos.asInputPosition, NotificationDetail.Factory.cartesianProduct(identifiers.asJava))
    case LegacyPlannerNotification =>
      NotificationCode.LEGACY_PLANNER.notification(InputPosition.empty)
    case LengthOnNonPathNotification(pos) =>
      NotificationCode.LENGTH_ON_NON_PATH.notification(pos.asInputPosition)
    case PlannerUnsupportedNotification =>
      NotificationCode.PLANNER_UNSUPPORTED.notification(InputPosition.empty)
    case RuntimeUnsupportedNotification =>
      NotificationCode.RUNTIME_UNSUPPORTED.notification(InputPosition.empty)
    case IndexHintUnfulfillableNotification(label, propertyKey) =>
      NotificationCode.INDEX_HINT_UNFULFILLABLE.notification(InputPosition.empty, NotificationDetail.Factory.index(label, propertyKey))
    case JoinHintUnfulfillableNotification(identifiers) =>
      NotificationCode.JOIN_HINT_UNFULFILLABLE.notification(InputPosition.empty, NotificationDetail.Factory.joinKey(identifiers.asJava))
    case JoinHintUnsupportedNotification(identifiers) =>
      NotificationCode.JOIN_HINT_UNSUPPORTED.notification(InputPosition.empty, NotificationDetail.Factory.joinKey(identifiers.asJava))
    case IndexLookupUnfulfillableNotification(labels) =>
      NotificationCode.INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(InputPosition.empty, NotificationDetail.Factory.indexSeekOrScan(labels.asJava))
    case BareNodeSyntaxDeprecatedNotification(pos) =>
      NotificationCode.BARE_NODE_SYNTAX_DEPRECATED.notification(pos.asInputPosition)
    case EagerLoadCsvNotification =>
      NotificationCode.EAGER_LOAD_CSV.notification(InputPosition.empty)
    case LargeLabelWithLoadCsvNotification =>
      NotificationCode.LARGE_LABEL_LOAD_CSV.notification(InputPosition.empty)
    case MissingLabelNotification(pos, label) =>
      NotificationCode.MISSING_LABEL.notification(pos.asInputPosition, NotificationDetail.Factory.label(label))
    case MissingRelTypeNotification(pos, relType) =>
      NotificationCode.MISSING_REL_TYPE.notification(pos.asInputPosition, NotificationDetail.Factory.relationshipType(relType))
    case MissingPropertyNameNotification(pos, name) =>
      NotificationCode.MISSING_PROPERTY_NAME.notification(pos.asInputPosition, NotificationDetail.Factory.propertyName(name))
    case UnboundedShortestPathNotification(pos) =>
      NotificationCode.UNBOUNDED_SHORTEST_PATH.notification(pos.asInputPosition)
  }

  override def accept[EX <: Exception](visitor: ResultVisitor[EX]) = exceptionHandlerFor2_3.runSafely {
    inner.accept(visitor)
    endQueryExecution()
  }

  override def toString() = {
    getClass.getName + "@" + Integer.toHexString(hashCode())
  }

  private implicit class ConvertibleCompilerInputPosition(pos: frontend.v2_3.InputPosition) {
    def asInputPosition = new InputPosition(pos.offset, pos.line, pos.column)
  }
}

case class CompatibilityPlanDescriptionFor2_3(inner: InternalPlanDescription, version: CypherVersion,
                                              planner: PlannerName, runtime: RuntimeName)
  extends ExtendedPlanDescription {

  self =>

  override def children = extendedChildren

  def extendedChildren = exceptionHandlerFor2_3.runSafely {
    inner.children.toSeq.map(CompatibilityPlanDescriptionFor2_3.apply(_, version, planner, runtime))
  }

  def arguments: Map[String, AnyRef] = exceptionHandlerFor2_3.runSafely {
    inner.arguments.map { arg => arg.name -> PlanDescriptionArgumentSerializer.serialize(arg) }.toMap
  }

  def identifiers = exceptionHandlerFor2_3.runSafely { inner.orderedIdentifiers.toSet }

  override def hasProfilerStatistics = exceptionHandlerFor2_3.runSafely { inner.arguments.exists(_.isInstanceOf[DbHits]) }

  def name = exceptionHandlerFor2_3.runSafely { inner.name }

  def asJava: javacompat.PlanDescription = exceptionHandlerFor2_3.runSafely { asJava(self) }

  override def toString: String = {
    val NL = System.lineSeparator()
    exceptionHandlerFor2_3.runSafely {
      s"Compiler CYPHER ${version.name}$NL${NL}Planner ${planner.toTextOutput.toUpperCase}$NL${NL}Runtime ${runtime.toTextOutput.toUpperCase}$NL$NL$inner"
    }
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

class EntityAccessorWrapper2_3(nodeManager: NodeManager) extends EntityAccessor {
  override def newNodeProxyById(id: Long): Node = nodeManager.newNodeProxyById(id)
  override def newRelationshipProxyById(id: Long): Relationship = nodeManager.newRelationshipProxyById(id)
}

case class CompatibilityFor2_3Cost(graph: GraphDatabaseService,
                                   config: CypherCompilerConfiguration,
                                   clock: Clock,
                                   kernelMonitors: KernelMonitors,
                                   kernelAPI: KernelAPI,
                                   log: Log,
                                   planner: CypherPlanner,
                                   runtime: CypherRuntime) extends CompatibilityFor2_3 {

  protected val compiler = {
    val plannerName = planner match {
      case CypherPlanner.default => None
      case CypherPlanner.cost | CypherPlanner.idp => Some(IDPPlannerName)
      case CypherPlanner.greedy => Some(GreedyPlannerName)
      case CypherPlanner.dp => Some(DPPlannerName)
      case _ => throw new IllegalArgumentException(s"unknown cost based planner: ${planner.name}")
    }

    val runtimeName = runtime match {
      case CypherRuntime.default => None
      case CypherRuntime.interpreted => Some(InterpretedRuntimeName)
    }

    val nodeManager = graph.asInstanceOf[GraphDatabaseAPI].getDependencyResolver.resolveDependency(classOf[NodeManager])
    CypherCompilerFactory.costBasedCompiler( graph, new EntityAccessorWrapper2_3(nodeManager), config, clock, new WrappedMonitors2_3( kernelMonitors ), new StringInfoLogger2_3( log ),
      rewriterSequencer, plannerName, runtimeName)
  }

  override val queryCacheSize: Int = config.queryCacheSize
}

case class CompatibilityFor2_3Rule(graph: GraphDatabaseService,
                                   config: CypherCompilerConfiguration,
                                   clock: Clock,
                                   kernelMonitors: KernelMonitors,
                                   kernelAPI: KernelAPI) extends CompatibilityFor2_3 {
  protected val compiler = {
    val nodeManager = graph.asInstanceOf[GraphDatabaseAPI].getDependencyResolver.resolveDependency(classOf[NodeManager])
    CypherCompilerFactory.ruleBasedCompiler( graph, new EntityAccessorWrapper2_3(nodeManager), config, clock,
      new WrappedMonitors2_3( kernelMonitors ), rewriterSequencer)
  }

  override val queryCacheSize: Int = config.queryCacheSize
}
