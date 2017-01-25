/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher

import java.util

import org.neo4j.collection.RawIterator
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.helpers.GraphIcing
import org.neo4j.cypher.internal.{ExecutionEngine, ExecutionResult}
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.graphdb.ExecutionPlanDescription
import org.neo4j.graphdb.Result.{ResultRow, ResultVisitor}
import org.neo4j.kernel.api.Statement
import org.neo4j.kernel.api.exceptions.ProcedureException
import org.neo4j.kernel.api.proc.Context.KERNEL_TRANSACTION
import org.neo4j.kernel.api.proc._
import org.neo4j.procedure.Mode
import org.neo4j.test.TestGraphDatabaseFactory

import scala.collection.immutable.Map

class ExecutionEngineIT extends CypherFunSuite with GraphIcing {

  test("should be possible to close compiled result after it is consumed") {
    // given
    val db = new TestGraphDatabaseFactory().newImpermanentDatabase()

    // when
    val result = db.execute("CYPHER runtime=compiled MATCH (n) RETURN n")
    result.accept(new ResultVisitor[RuntimeException] {
      def visit(row: ResultRow) = true
    })

    result.close()

    // then
    // call to close actually worked
  }

  private implicit class RichDb(db: GraphDatabaseCypherService) {
    def planDescriptionForQuery(query: String): ExecutionPlanDescription = {
      val res = db.execute(query)
      res.resultAsString()
      res.getExecutionPlanDescription
    }
  }

  implicit class RichExecutionEngine(engine: ExecutionEngine) {
    def profile(query: String, params: Map[String, Any]): ExecutionResult =
      engine.profile(query, params, engine.queryService.transactionalContext(query = query -> params))

    def execute(query: String, params: Map[String, Any]): ExecutionResult =
      engine.execute(query, params, engine.queryService.transactionalContext(query = query -> params))
  }

  class AllNodesProcedure extends CallableProcedure {
    import scala.collection.JavaConverters._

    private val results = Map[String, AnyRef]("node" -> Neo4jTypes.NTInteger)
    val procedureName = new QualifiedName(Array[String]("org", "neo4j", "bench"), "getAllNodes")
    val emptySignature: util.List[FieldSignature] = List.empty[FieldSignature].asJava
    val signature: ProcedureSignature = new ProcedureSignature(
      procedureName, paramSignature, resultSignature, Mode.READ, java.util.Optional.empty(), Array.empty,
      java.util.Optional.empty())

    def paramSignature: util.List[FieldSignature] = List.empty[FieldSignature].asJava

    def resultSignature: util.List[FieldSignature] = results.keys.foldLeft(List.empty[FieldSignature]) { (fields, entry) =>
      fields :+ new FieldSignature(entry, results(entry).asInstanceOf[Neo4jTypes.AnyType])
    }.asJava

    override def apply(context: Context, objects: Array[AnyRef]): RawIterator[Array[AnyRef], ProcedureException] = {
      val statement: Statement = context.get(KERNEL_TRANSACTION).acquireStatement
      val readOperations = statement.readOperations
      val nodes = readOperations.nodesGetAll()
      var count = 0
      new RawIterator[Array[AnyRef], ProcedureException] {
        override def next(): Array[AnyRef] = {
          count = count + 1
          Array(new java.lang.Long(nodes.next()))
        }

        override def hasNext: Boolean = {
          if (!nodes.hasNext) statement.close()
          nodes.hasNext
        }
      }
    }
  }
}
