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

import org.neo4j.cypher.internal.compiler.v3_0.codegen.ResultRowImpl
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.{AcceptingExecutionResult, InternalQueryType, READ_ONLY}
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v3_0.spi.{InternalResultVisitor, ProcedureSignature, QueryContext}
import org.neo4j.cypher.internal.compiler.v3_0.{ExecutionMode, InternalQueryStatistics, TaskCloser}
import org.neo4j.cypher.internal.frontend.v3_0.helpers.JavaCompatibility.asJavaCompatible

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
  extends AcceptingExecutionResult(context, Some(taskCloser)) {

  override def javaColumns: java.util.List[String] = signature.outputSignature.seq.map(_.name).asJava

  override def accept[EX <: Exception](visitor: InternalResultVisitor[EX]) = {
    context.callReadOnlyProcedure(signature, args.map(asJavaCompatible)).foreach { res =>
      var i = 0
      val row = new ResultRowImpl
      signature.outputSignature.foreach { f =>
        row.set(f.name, res(i))
        i = i + 1
      }
      visitor.visit(row)
    }
    taskCloser.close(success = true)
  }

  //TODO so far we have only read-only procedures, but when we have updating procedures this
  //will probably not work since the updates will be done directly on GraphDatabaseService
  override def queryStatistics() = context.getOptStatistics.getOrElse(InternalQueryStatistics())

  //TODO so far only read-only queries, change when we have updating procedures
  override def executionType: InternalQueryType = READ_ONLY
}
