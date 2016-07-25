/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.executionplan.procs

import java.util

import org.neo4j.cypher.internal.compiler.v3_1.codegen.ResultRowImpl
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.{InternalQueryType, ProcedureCallMode, StandardInternalExecutionResult}
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v3_1.spi.{QualifiedProcedureName, InternalResultVisitor, QueryContext}
import org.neo4j.cypher.internal.compiler.v3_1.{ExecutionMode, InternalQueryStatistics, ProfileMode, TaskCloser}
import org.neo4j.cypher.internal.frontend.v3_1.ProfilerStatisticsNotReadyException

/**
  * Execution result of a Procedure
  *
  * @param context The QueryContext used to communicate with the kernel.
  * @param taskCloser called when done with the result, cleans up resources.
  * @param name The name of the procedure.
  * @param callMode The call mode of the procedure.
  * @param args The argument to the procedure.
  * @param indexResultNameMappings Describes how values at output row indices are mapped onto result columns.
  * @param executionPlanDescriptionGenerator Generator for the plan description of the result.
  * @param executionMode The execution mode.
  */
class ProcedureExecutionResult[E <: Exception](context: QueryContext,
                                               taskCloser: TaskCloser,
                                               name: QualifiedProcedureName,
                                               callMode: ProcedureCallMode,
                                               args: Seq[Any],
                                               indexResultNameMappings: Seq[(Int, String)],
                                               executionPlanDescriptionGenerator: () => InternalPlanDescription,
                                               val executionMode: ExecutionMode)
  extends StandardInternalExecutionResult(context, Some(taskCloser)) {

  override def columns = indexResultNameMappings.map(_._2).toList

  private final val executionResults = executeCall

  // The signature mode is taking care of eagerization
  protected def executeCall = callMode.call(context, name, args)

  override protected def createInner = new util.Iterator[util.Map[String, Any]]() {
    override def next(): util.Map[String, Any] =
      try { resultAsMap( executionResults.next( ) ) }
      catch { case e: NoSuchElementException => success(); throw e }

    override def hasNext: Boolean = if (executionResults.hasNext) true else { success(); close(); false }
  }

  override def accept[EX <: Exception](visitor: InternalResultVisitor[EX]) = {
    executionResults.foreach { res => visitor.visit(new ResultRowImpl(resultAsRefMap(res))) }
    success()
    close()
  }

  // TODO Look into having the kernel track updates, rather than cypher middle-layers, only sensible way I can think
  //      of to get accurate stats for procedure code
  override def queryStatistics() = context.getOptStatistics.getOrElse(InternalQueryStatistics())
  override def executionType: InternalQueryType = callMode.queryType

  private def resultAsMap(rowData: Array[AnyRef]): util.Map[String, Any] = {
    val mapData = new util.HashMap[String, Any](rowData.length)
    indexResultNameMappings.foreach { entry => mapData.put(entry._2, rowData(entry._1)) }
    mapData
  }

  private def resultAsRefMap(rowData: Array[AnyRef]): util.Map[String, AnyRef] = {
    val mapData = new util.HashMap[String, AnyRef](rowData.length)
    indexResultNameMappings.foreach { entry => mapData.put(entry._2, rowData(entry._1)) }
    mapData
  }

  override def executionPlanDescription(): InternalPlanDescription = executionMode match {
    case ProfileMode if executionResults.hasNext => throw new ProfilerStatisticsNotReadyException()
    case _ => executionPlanDescriptionGenerator()
  }
}
