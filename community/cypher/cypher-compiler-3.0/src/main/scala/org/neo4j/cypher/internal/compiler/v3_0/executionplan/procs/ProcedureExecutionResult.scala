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
package org.neo4j.cypher.internal.compiler.v3_0.executionplan.procs

import java.util

import org.neo4j.cypher.internal.compiler.v3_0.codegen.ResultRowImpl
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.{InternalQueryType, StandardInternalExecutionResult}
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v3_0.spi.{InternalResultVisitor, ProcedureSignature, QueryContext}
import org.neo4j.cypher.internal.compiler.v3_0.{ExecutionMode, InternalQueryStatistics, TaskCloser}
import org.neo4j.cypher.internal.frontend.v3_0.helpers.JavaValueCompatibility.asDeepJavaValue

import scala.collection.JavaConverters._

/**
  * Execution result of a Procedure
  *
  * @param taskCloser called when done with the result, cleans up resources.
  * @param context The QueryContext used to communicate with the kernel.
  * @param signature The signature of the procedure.
  * @param args The argument to the procedure.
  * @param executionPlanDescription The plan description of the result.
  */
case class ProcedureExecutionResult[E <: Exception](taskCloser: TaskCloser,
                                                    context: QueryContext,
                                                    signature: ProcedureSignature,
                                                    args: Seq[Any],
                                                    executionPlanDescription: InternalPlanDescription,
                                                    executionMode: ExecutionMode)
  extends StandardInternalExecutionResult(context, Some(taskCloser)) {

  // The signature mode is taking care of eagerization
  private val underlying = signature.mode.call(context, signature, args.map(asDeepJavaValue))
  private val outputs = signature.outputSignature

  override protected val inner = new util.Iterator[util.Map[String, Any]]() {
    override def next(): util.Map[String, Any] = resultAsMap(underlying.next())
    override def hasNext: Boolean = underlying.hasNext
  }

  override def accept[EX <: Exception](visitor: InternalResultVisitor[EX]) = {
    underlying.foreach { res => visitor.visit(new ResultRowImpl(resultAsRefMap(res))) }
    close()
  }

  override def javaColumns: java.util.List[String] = signature.outputSignature.seq.map(_.name).asJava

  // TODO Look into having the kernel track updates, rather than cypher middle-layers, only sensible way I can think
  //      of to get accurate stats for procedure code
  override def queryStatistics() = context.getOptStatistics.getOrElse(InternalQueryStatistics())
  override def executionType: InternalQueryType = signature.mode.queryType

  private def resultAsMap(rowData: Array[AnyRef]): util.Map[String, Any] = {
    val mapData = new util.HashMap[String, Any](rowData.length)
    var i = 0
    outputs.foreach { field =>
      mapData.put(field.name, rowData(i))
      i = i + 1
    }
    mapData
  }

  private def resultAsRefMap(rowData: Array[AnyRef]): util.Map[String, AnyRef] = {
    val mapData = new util.HashMap[String, AnyRef](rowData.length)
    var i = 0
    outputs.foreach { field =>
      mapData.put(field.name, rowData(i))
      i = i + 1
    }
    mapData
  }
}
