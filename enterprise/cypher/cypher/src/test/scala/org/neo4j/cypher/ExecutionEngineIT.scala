/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher

import java.util

import org.neo4j.collection.RawIterator
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Result.{ResultRow, ResultVisitor}
import org.neo4j.graphdb.{ExecutionPlanDescription, Result}
import org.neo4j.internal.kernel.api.exceptions.ProcedureException
import org.neo4j.internal.kernel.api.procs.{FieldSignature, Neo4jTypes, ProcedureSignature, QualifiedName}
import org.neo4j.kernel.api.ResourceTracker
import org.neo4j.kernel.api.proc.Context.KERNEL_TRANSACTION
import org.neo4j.kernel.api.proc._
import org.neo4j.procedure.Mode
import org.neo4j.test.TestGraphDatabaseFactory

import scala.collection.immutable.Map
import scala.collection.mutable.ArrayBuffer

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
    def profile(query: String, params: Map[String, Any]): Result =
      engine.profile(query, params, engine.queryService.transactionalContext(query = query -> params))

    def execute(query: String, params: Map[String, Any]): Result =
      engine.execute(query, params, engine.queryService.transactionalContext(query = query -> params))
  }

  class AllNodesProcedure extends CallableProcedure {
    import scala.collection.JavaConverters._

    private val results = Map[String, AnyRef]("node" -> Neo4jTypes.NTInteger)
    val procedureName = new QualifiedName(Array[String]("org", "neo4j", "bench"), "getAllNodes")
    val emptySignature: util.List[FieldSignature] = List.empty[FieldSignature].asJava
    val signature: ProcedureSignature = new ProcedureSignature(
      procedureName, paramSignature, resultSignature, Mode.READ, null, Array.empty,
      null, null, false, false)

    def paramSignature: util.List[FieldSignature] = List.empty[FieldSignature].asJava

    def resultSignature: util.List[FieldSignature] = results.keys.foldLeft(List.empty[FieldSignature]) { (fields, entry) =>
      fields :+ FieldSignature.outputField(entry, results(entry).asInstanceOf[Neo4jTypes.AnyType])
    }.asJava

    override def apply(context: Context,
                       objects: Array[AnyRef],
                       resourceTracker: ResourceTracker): RawIterator[Array[AnyRef], ProcedureException] = {
      val ktx = context.get(KERNEL_TRANSACTION)
      val nodeBuffer = new ArrayBuffer[Long]()
      val cursor = ktx.cursors().allocateNodeCursor()
      ktx.dataRead().allNodesScan(cursor)
      while (cursor.next()) nodeBuffer.append(cursor.nodeReference())
      cursor.close()
      val nodes = nodeBuffer.iterator
      var count = 0
      new RawIterator[Array[AnyRef], ProcedureException] {
        override def next(): Array[AnyRef] = {
          count = count + 1
          Array(new java.lang.Long(nodes.next()))
        }

        override def hasNext: Boolean = {
          nodes.hasNext
        }
      }
    }
  }
}
