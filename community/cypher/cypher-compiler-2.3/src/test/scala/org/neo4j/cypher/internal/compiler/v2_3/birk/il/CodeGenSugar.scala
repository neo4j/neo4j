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
package org.neo4j.cypher.internal.compiler.v2_3.birk.il

import org.neo4j.cypher.internal.compiler.v2_3.{TaskCloser, ExecutionMode}
import org.neo4j.cypher.internal.compiler.v2_3.birk.{QueryExecutionTracer, CodeGenerator}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription
import org.neo4j.function.Supplier
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Result.{ResultRow, ResultVisitor}
import org.neo4j.kernel.api.Statement
import org.scalatest.mock.MockitoSugar

import scala.collection.JavaConversions

trait CodeGenSugar extends MockitoSugar {
  def evaluate(instructions: Instruction*): List[Map[String, Object]] = {
    evaluate(newInstance(compile(instructions: _*)))
  }

  def evaluate(result: InternalExecutionResult): List[Map[String, Object]] = {
    var rows = List.empty[Map[String, Object]]
    val columns: List[String] = result.columns
    result.accept(new ResultVisitor[RuntimeException] {
      override def visit(row: ResultRow): Boolean = {
        rows = rows :+ columns.map(key => (key, row.get(key))).toMap
        true
      }
    })
    rows
  }

  def compile(instructions: Instruction*): Class[InternalExecutionResult] =
    CodeGenerator.generateClass(instructions.toSeq)

  def newInstance(clazz: Class[InternalExecutionResult],
                  taskCloser: TaskCloser = new TaskCloser,
                  statement: Statement = mock[Statement],
                  graphdb: GraphDatabaseService = null,
                  executionMode: ExecutionMode = null,
                  description: Supplier[InternalPlanDescription] = null,
                  queryExecutionTracer: QueryExecutionTracer = null,
                  params: Map[String, Any] = Map.empty): InternalExecutionResult =
    clazz.getConstructor(
      classOf[TaskCloser],
      classOf[Statement],
      classOf[GraphDatabaseService],
      classOf[ExecutionMode],
      classOf[Supplier[InternalPlanDescription]],
      classOf[QueryExecutionTracer],
      classOf[java.util.Map[String, Object]]
    ).newInstance(taskCloser, statement, graphdb, executionMode, description, queryExecutionTracer, JavaConversions.mapAsJavaMap(params))
}
