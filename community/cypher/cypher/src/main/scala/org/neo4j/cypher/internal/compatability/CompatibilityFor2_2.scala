/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatability

import java.io.PrintWriter
import java.util

import org.neo4j.cypher._
import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compiler.v2_2
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.{InternalExecutionResult, ExecutionPlan => ExecutionPlan_v2_2}
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.InternalPlanDescription.Arguments.{Version, DbHits, Rows}
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.{Argument, InternalPlanDescription, PlanDescriptionArgumentSerializer}
import org.neo4j.cypher.internal.compiler.v2_2.spi.{ExceptionTranslatingQueryContext => ExceptionTranslatingQueryContext_v2_2}
import org.neo4j.cypher.internal.compiler.v2_2.{CypherCompilerFactory, Legacy, PlannerName, Ronja}
import org.neo4j.cypher.internal.spi.v2_2.{TransactionBoundPlanContext, TransactionBoundQueryContext}
import org.neo4j.cypher.javacompat.ProfilerStatistics
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.api.{KernelAPI, Statement}
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}

import scala.collection.JavaConverters._
import scala.util.Try

trait CompatibilityFor2_2 {

  val graph: GraphDatabaseService
  val queryCacheSize: Int
  val kernelMonitors: KernelMonitors
  val kernelAPI: KernelAPI

  protected val compiler: v2_2.CypherCompiler

  def produceParsedQuery(statementAsText: String, planType: PlanType) = new ParsedQuery {
    val preparedQueryForV_2_2 = Try(compiler.prepareQuery(statementAsText, planType))

    def isPeriodicCommit = preparedQueryForV_2_2.map(_.isPeriodicCommit).getOrElse(false)

    def plan(statement: Statement): (ExecutionPlan, Map[String, Any]) = {
      val planContext = new TransactionBoundPlanContext(statement, graph)
      val (planImpl, extractedParameters) = compiler.planPreparedQuery(preparedQueryForV_2_2.get, planContext)
      (new ExecutionPlanWrapper(planImpl), extractedParameters)
    }
  }

  class ExecutionPlanWrapper(inner: ExecutionPlan_v2_2) extends ExecutionPlan {

    private def queryContext(graph: GraphDatabaseAPI, txInfo: TransactionInfo) = {
      val ctx = new TransactionBoundQueryContext(graph, txInfo.tx, txInfo.isTopLevelTx, txInfo.statement)
      new ExceptionTranslatingQueryContext_v2_2(ctx)
    }

    def profile(graph: GraphDatabaseAPI, txInfo: TransactionInfo, params: Map[String, Any]) =
      ExecutionResultWrapperFor2_2(inner.profile(queryContext(graph, txInfo), params), translate(inner.plannerUsed))

    def execute(graph: GraphDatabaseAPI, txInfo: TransactionInfo, params: Map[String, Any]) =
      ExecutionResultWrapperFor2_2(inner.execute(queryContext(graph, txInfo), params), translate(inner.plannerUsed))

    def isPeriodicCommit = inner.isPeriodicCommit

    private def translate(in: PlannerName): CypherVersion = in match {
      case Legacy => CypherVersion.v2_2_rule
      case Ronja  => CypherVersion.v2_2_cost
    }
  }
}

case class ExecutionResultWrapperFor2_2(inner: InternalExecutionResult, version: CypherVersion) extends ExtendedExecutionResult {
  def planDescriptionRequested = inner.planDescriptionRequested

  def javaIterator = inner.javaIterator

  def columnAs[T](column: String) = inner.columnAs[T](column)

  def columns = inner.columns

  def javaColumns = inner.javaColumns

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
      constraintsAdded = i.constraintsAdded,
      constraintsRemoved = i.constraintsRemoved)
  }

  def dumpToString(writer: PrintWriter) = inner.dumpToString(writer)

  def dumpToString() = inner.dumpToString()

  def javaColumnAs[T](column: String) = inner.javaColumnAs[T](column)

  def executionPlanDescription(): PlanDescription =
    convert(
      inner.executionPlanDescription().
      addArgument(Version(version.toString))
    )

  def close() = inner.close()

  def next() = inner.next()

  def hasNext = inner.hasNext

  def convert(i: InternalPlanDescription): PlanDescription = CompatibilityPlanDescription(i)
}

case class CompatibilityPlanDescription(inner: InternalPlanDescription) extends PlanDescription {
  self =>
  def children = inner.children.toSeq.map(CompatibilityPlanDescription.apply)

  def arguments: Map[String, AnyRef] = inner.arguments.map {
    arg => arg.name -> PlanDescriptionArgumentSerializer.serialize(arg)
  }.toMap

  def hasProfilerStatistics = inner.arguments.exists(_.isInstanceOf[DbHits])

  def name = inner.name

  def asJava: javacompat.PlanDescription = asJava(self)

  def asJava(in: PlanDescription): javacompat.PlanDescription = new javacompat.PlanDescription {
    def getProfilerStatistics: ProfilerStatistics = new ProfilerStatistics {
      def getDbHits: Long = extract { case DbHits(count) => count}
      def getRows: Long = extract { case Rows(count) => count}

      private def extract(f: PartialFunction[Argument, Long]): Long =
        inner.arguments.collectFirst(f).getOrElse(throw new InternalException("Don't have profiler stats"))
    }

    def getName: String = name

    def hasProfilerStatistics: Boolean = self.hasProfilerStatistics

    def getArguments: util.Map[String, AnyRef] = arguments.asJava

    def getChildren: util.List[javacompat.PlanDescription] = in.children.toList.map(_.asJava).asJava
  }
}


case class CompatibilityFor2_2Cost(graph: GraphDatabaseService,
                                           queryCacheSize: Int,
                                           kernelMonitors: KernelMonitors,
                                           kernelAPI: KernelAPI) extends CompatibilityFor2_2 {
  protected val compiler = CypherCompilerFactory.ronjaCompiler(graph, queryCacheSize, kernelMonitors)
}

case class CompatibilityFor2_2Rule(graph: GraphDatabaseService,
                                           queryCacheSize: Int,
                                           kernelMonitors: KernelMonitors,
                                           kernelAPI: KernelAPI) extends CompatibilityFor2_2 {
  protected val compiler = CypherCompilerFactory.legacyCompiler(graph, queryCacheSize, kernelMonitors)
}
