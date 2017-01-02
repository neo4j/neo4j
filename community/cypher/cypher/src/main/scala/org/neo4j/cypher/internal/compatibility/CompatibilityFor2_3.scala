/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.cypher._
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{EntityAccessor, InternalExecutionResult, ExecutionPlan => ExecutionPlan_v2_3}
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.{Argument, InternalPlanDescription, PlanDescriptionArgumentSerializer}
import org.neo4j.cypher.internal.compiler.v2_3.spi.{PlanContext, QueryContext}
import org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.compiler.v2_3.{CypherCompilerFactory, DPPlannerName, GreedyPlannerName, IDPPlannerName, InfoLogger, Monitors, PlannerName, ExplainMode => ExplainModev2_3, NormalMode => NormalModev2_3, ProfileMode => ProfileModev2_3, _}
import org.neo4j.cypher.internal.compiler.{v2_3, v3_1}
import org.neo4j.cypher.internal.frontend.v2_3.notification.{InternalNotification, LegacyPlannerNotification, PlannerUnsupportedNotification, RuntimeUnsupportedNotification, _}
import org.neo4j.cypher.internal.frontend.v2_3.spi.MapToPublicExceptions
import org.neo4j.cypher.internal.frontend.v2_3.{CypherException => InternalCypherException, InputPosition => InternalInputPosition}
import org.neo4j.cypher.internal.javacompat.{PlanDescription, ProfilerStatistics}
import org.neo4j.cypher.internal.spi.TransactionalContextWrapperv3_1
import org.neo4j.cypher.internal.spi.v2_3.{TransactionBoundGraphStatistics, TransactionBoundPlanContext, TransactionBoundQueryContext}
import org.neo4j.cypher.internal.{CypherExecutionMode, ExecutionResult, LastCommittedTxIdProvider, ParsedQuery, PreParsedQuery, QueryStatistics}
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.graphdb._
import org.neo4j.graphdb.impl.notification.{NotificationCode, NotificationDetail}
import org.neo4j.helpers.Clock
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.KernelAPI
import org.neo4j.kernel.impl.core.NodeManager
import org.neo4j.kernel.impl.query.{QueryExecutionMonitor, TransactionalContext}
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.Log

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.Try

object helpersv2_3 {
  implicit def monitorFailure(t: Throwable)(implicit monitor: QueryExecutionMonitor, tc: TransactionalContext): Unit = {
    monitor.endFailure(tc.executingQuery(), t)
  }
}

object exceptionHandlerFor2_3 extends MapToPublicExceptions[CypherException] {
  def syntaxException(message: String, query: String, offset: Option[Int], cause: Throwable) = new SyntaxException(message, query, offset, cause)

  def arithmeticException(message: String, cause: Throwable) = new ArithmeticException(message, cause)

  def profilerStatisticsNotReadyException(cause: Throwable) = {
    throw new ProfilerStatisticsNotReadyException(cause)
  }

  def incomparableValuesException(lhs: String, rhs: String, cause: Throwable) = new IncomparableValuesException(lhs, rhs, cause)

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

  def labelScanHintException(variable: String, label: String, message: String, cause: Throwable) = throw new LabelScanHintException(variable, label, message, cause)

  def invalidSemanticException(message: String, cause: Throwable) = throw new InvalidSemanticsException(message, cause)

  def parameterWrongTypeException(message: String, cause: Throwable) = throw new ParameterWrongTypeException(message, cause)

  def nodeStillHasRelationshipsException(nodeId: Long, cause: Throwable) = throw new NodeStillHasRelationshipsException(nodeId, cause)

  def indexHintException(variable: String, label: String, property: String, message: String, cause: Throwable) = throw new IndexHintException(variable, label, property, message, cause)

  def joinHintException(variable: String, message: String, cause: Throwable) = throw new JoinHintException(variable, message, cause)

  def periodicCommitInOpenTransactionException(cause: Throwable) = throw new PeriodicCommitInOpenTransactionException(cause)

  def failedIndexException(indexName: String, cause: Throwable): CypherException = throw new FailedIndexException(indexName, cause)

  object runSafely extends RunSafely {
    def apply[T](body: => T)(implicit f: Throwable => Unit = (_) => ()) = {
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

  val graph: GraphDatabaseQueryService
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
    import org.neo4j.cypher.internal.helpers.wrappersFor2_3._
    val notificationLogger = new RecordingNotificationLogger
    val preparedQueryForV_2_3 =
      Try(compiler.prepareQuery(preParsedQuery.statement,
                                preParsedQuery.rawStatement,
                                notificationLogger,
                                preParsedQuery.planner.name,
                                Some(as2_3(preParsedQuery.offset)), tracer))
    new ParsedQuery {
      def isPeriodicCommit = preparedQueryForV_2_3.map(_.isPeriodicCommit).getOrElse(false)

      def plan(transactionalContext: TransactionalContextWrapperv3_1, tracer: v3_1.CompilationPhaseTracer): (org.neo4j.cypher.internal.ExecutionPlan, Map[String, Any]) = exceptionHandlerFor2_3.runSafely {
        val planContext: PlanContext = new TransactionBoundPlanContext(transactionalContext)
        val (planImpl, extractedParameters) = compiler.planPreparedQuery(preparedQueryForV_2_3.get, planContext, as2_3(tracer))

        // Log notifications/warnings from planning
        planImpl.notifications(planContext).foreach(notificationLogger += _)

        (new ExecutionPlanWrapper(planImpl), extractedParameters)
      }

      override def hasErrors = preparedQueryForV_2_3.isFailure
    }
  }

  class ExecutionPlanWrapper(inner: ExecutionPlan_v2_3) extends org.neo4j.cypher.internal.ExecutionPlan {

    private def queryContext(transactionalContext: TransactionalContextWrapperv3_1): QueryContext =
      new ExceptionTranslatingQueryContextFor2_3(new TransactionBoundQueryContext(transactionalContext))

    def run(transactionalContext: TransactionalContextWrapperv3_1, executionMode: CypherExecutionMode, params: Map[String, Any]): ExecutionResult = {
      val innerExecutionMode = executionMode match {
        case CypherExecutionMode.explain => ExplainModev2_3
        case CypherExecutionMode.profile => ProfileModev2_3
        case CypherExecutionMode.normal => NormalModev2_3
      }

      val query = transactionalContext.tc.executingQuery()

      exceptionHandlerFor2_3.runSafely {
        val innerResult = inner.run(queryContext(transactionalContext), transactionalContext.statement, innerExecutionMode, params)
        new ClosingExecutionResult(
          query,
          new ExecutionResultWrapperFor2_3(innerResult, inner.plannerUsed, inner.runtimeUsed),
          exceptionHandlerFor2_3.runSafely
        )
      }
    }

    def isPeriodicCommit = inner.isPeriodicCommit


    def isStale(lastCommittedTxId: LastCommittedTxIdProvider, ctx: TransactionalContextWrapperv3_1): Boolean =
      inner.isStale(lastCommittedTxId, TransactionBoundGraphStatistics(ctx.readOperations))
  }
}

object ExecutionResultWrapperFor2_3 {
  def unapply(v: Any): Option[(InternalExecutionResult, PlannerName, RuntimeName)] = v match {
    case closing: ClosingExecutionResult => unapply(closing.inner)
    case wrapper: ExecutionResultWrapperFor2_3 => Some((wrapper.inner, wrapper.planner, wrapper.runtime))
    case _ => None
  }
}

class ExecutionResultWrapperFor2_3(val inner: InternalExecutionResult, val planner: PlannerName, val runtime: RuntimeName)
  extends ExecutionResult {

  override def planDescriptionRequested = inner.planDescriptionRequested
  override def javaIterator = inner.javaIterator
  override def columnAs[T](column: String) = inner.columnAs(column)
  override def columns = inner.columns
  override def javaColumns = inner.javaColumns

  def queryStatistics() = {
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

  override def dumpToString(writer: PrintWriter) = inner.dumpToString(writer)
  override def dumpToString() = inner.dumpToString()

  override def javaColumnAs[T](column: String) = inner.javaColumnAs(column)

  def executionPlanDescription(): org.neo4j.cypher.internal.PlanDescription =
    convert(
      inner.executionPlanDescription().
        addArgument(Version("CYPHER 2.3")).
        addArgument(Planner(planner.toTextOutput)).
        addArgument(PlannerImpl(planner.name)).
        addArgument(Runtime(runtime.toTextOutput)).
        addArgument(RuntimeImpl(runtime.name))
    )

  private def convert(i: InternalPlanDescription): org.neo4j.cypher.internal.PlanDescription = exceptionHandlerFor2_3.runSafely {
    CompatibilityPlanDescriptionFor2_3(i, CypherVersion.v2_3, planner, runtime)
  }

  override def hasNext = inner.hasNext
  override def next() = inner.next()
  override def close() = inner.close()

  def executionType: QueryExecutionType = inner.executionType

  def notifications = inner.notifications.map(asKernelNotification)

  private def asKernelNotification(notification: InternalNotification) = notification match {
    case CartesianProductNotification(pos, variables) =>
      NotificationCode.CARTESIAN_PRODUCT.notification(pos.asInputPosition, NotificationDetail.Factory.cartesianProduct(variables.asJava))
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
    case JoinHintUnfulfillableNotification(variables) =>
      NotificationCode.JOIN_HINT_UNFULFILLABLE.notification(InputPosition.empty, NotificationDetail.Factory.joinKey(variables.asJava))
    case JoinHintUnsupportedNotification(variables) =>
      NotificationCode.JOIN_HINT_UNSUPPORTED.notification(InputPosition.empty, NotificationDetail.Factory.joinKey(variables.asJava))
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

  override def accept[EX <: Exception](visitor: ResultVisitor[EX]) = inner.accept(visitor)

  private implicit class ConvertibleCompilerInputPosition(pos: InternalInputPosition) {
    def asInputPosition = new InputPosition(pos.offset, pos.line, pos.column)
  }
}

case class CompatibilityPlanDescriptionFor2_3(inner: InternalPlanDescription, version: CypherVersion,
                                              planner: PlannerName, runtime: RuntimeName)
  extends org.neo4j.cypher.internal.PlanDescription {

  self =>

  def children = exceptionHandlerFor2_3.runSafely {
    inner.children.toSeq.map(CompatibilityPlanDescriptionFor2_3.apply(_, version, planner, runtime))
  }

  def arguments: Map[String, AnyRef] = exceptionHandlerFor2_3.runSafely {
    inner.arguments.map { arg => arg.name -> PlanDescriptionArgumentSerializer.serialize(arg) }.toMap
  }

  def identifiers = exceptionHandlerFor2_3.runSafely { inner.orderedIdentifiers.toSet }

  override def hasProfilerStatistics = exceptionHandlerFor2_3.runSafely { inner.arguments.exists(_.isInstanceOf[DbHits]) }

  def name = exceptionHandlerFor2_3.runSafely { inner.name }

  def asJava: PlanDescription = exceptionHandlerFor2_3.runSafely { asJava(self) }

  override def toString: String = {
    val NL = System.lineSeparator()
    exceptionHandlerFor2_3.runSafely {
      s"Compiler CYPHER ${version.name}$NL${NL}Planner ${planner.toTextOutput.toUpperCase}$NL${NL}Runtime ${runtime.toTextOutput.toUpperCase}$NL$NL$inner"
    }
  }

  def asJava(in: org.neo4j.cypher.internal.PlanDescription): PlanDescription = new PlanDescription {
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

    def getChildren: util.List[PlanDescription] = in.children.toList.map(_.asJava).asJava

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

case class CompatibilityFor2_3Cost(graph: GraphDatabaseQueryService,
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

    val runtimeName: Option[RuntimeName] = runtime match {
      case CypherRuntime.default => None
      case CypherRuntime.interpreted => Some(InterpretedRuntimeName)
      case CypherRuntime.compiled => throw new IllegalArgumentException("Compiled runtime is not supported in Cypher 2.3")
    }

    val nodeManager = graph.getDependencyResolver.resolveDependency(classOf[NodeManager])
    CypherCompilerFactory.costBasedCompiler(
      graph.asInstanceOf[GraphDatabaseCypherService].getGraphDatabaseService, new EntityAccessorWrapper2_3(nodeManager), config, clock, new WrappedMonitors2_3( kernelMonitors ),
      new StringInfoLogger2_3( log ), rewriterSequencer, plannerName, runtimeName)
  }

  override val queryCacheSize: Int = config.queryCacheSize
}

case class CompatibilityFor2_3Rule(graph: GraphDatabaseQueryService,
                                   config: CypherCompilerConfiguration,
                                   clock: Clock,
                                   kernelMonitors: KernelMonitors,
                                   kernelAPI: KernelAPI) extends CompatibilityFor2_3 {
  protected val compiler = {
    val nodeManager = graph.getDependencyResolver.resolveDependency(classOf[NodeManager])
    CypherCompilerFactory.ruleBasedCompiler(
      graph.asInstanceOf[GraphDatabaseCypherService].getGraphDatabaseService, new EntityAccessorWrapper2_3(nodeManager), config, clock, new WrappedMonitors2_3( kernelMonitors ), rewriterSequencer)
  }

  override val queryCacheSize: Int = config.queryCacheSize
}
