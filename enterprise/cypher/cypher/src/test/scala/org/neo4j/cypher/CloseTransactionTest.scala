/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher

import java.util

import org.neo4j.collection.RawIterator
import org.neo4j.cypher.ExecutionEngineHelper.createEngine
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Result.{ResultRow, ResultVisitor}
import org.neo4j.graphdb.{GraphDatabaseService, Result}
import org.neo4j.internal.kernel.api.exceptions.ProcedureException
import org.neo4j.internal.kernel.api.procs.{FieldSignature, Neo4jTypes, ProcedureSignature, QualifiedName}
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.ResourceTracker
import org.neo4j.kernel.api.proc.Context.KERNEL_TRANSACTION
import org.neo4j.kernel.api.proc._
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.procedure.Mode
import org.neo4j.test.TestGraphDatabaseFactory

import scala.collection.immutable.Map
import scala.collection.mutable.ArrayBuffer

class CloseTransactionTest extends CypherFunSuite with GraphIcing {

  private val runtimes = Seq("interpreted", "compiled")

  private var db : GraphDatabaseService = _

  override protected def initTest(): Unit = {
    super.initTest()
    db = new TestGraphDatabaseFactory().newImpermanentDatabase()
  }

  override protected def stopTest(): Unit = {
    db.shutdown()
    super.stopTest()
  }

  for (runtime <- runtimes) {

    test(s"should not leak transaction when closing the result for a query - runtime=$runtime") {
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      // when
      db.execute(s"CYPHER runtime=$runtime return 1").close()
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime return 1", Map.empty[String, Object]).close()
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime return 1", Map.empty[String, Object]).close()
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when closing the result for a profile query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      // when
      db.execute(s"CYPHER runtime=$runtime profile return 1").close()
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime profile return 1", Map.empty[String, Object]).close()
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime profile return 1", Map.empty[String, Object]).close()
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.profile("return 1", Map.empty[String, Object]).close()
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.profile("return 1", Map.empty[String, Object]).close()
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when closing the result for an explain query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      // when
      db.execute(s"CYPHER runtime=$runtime explain return 1").close()
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime explain return 1", Map.empty[String, Object]).close()
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime explain return 1", Map.empty[String, Object]).close()
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when failing in pre-parsing - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      // when
      intercept[SyntaxException](engine.execute(s"CYPHER runtime=$runtime ", Map.empty[String, Object]))
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when closing the result for a procedure query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      procedures(service).register(new AllNodesProcedure())
      txBridge(service).hasTransaction shouldBe false

      // when
      db.execute(s"CYPHER runtime=$runtime CALL org.neo4j.bench.getAllNodes()").close()
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).close()
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).close()
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when closing the result for a profile procedure query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      procedures(service).register(new AllNodesProcedure())
      txBridge(service).hasTransaction shouldBe false

      // when
      db.execute(s"CYPHER runtime=$runtime profile CALL org.neo4j.bench.getAllNodes()").close()
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime profile CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).close()
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime profile CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).close()
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when closing the result for an explain procedure query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      procedures(service).register(new AllNodesProcedure())
      txBridge(service).hasTransaction shouldBe false

      // when
      db.execute(s"CYPHER runtime=$runtime explain CALL org.neo4j.bench.getAllNodes()").close()
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime explain CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).close()
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime explain CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).close()
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when consuming the whole iterator for a regular query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      import scala.collection.JavaConverters._
      // when
      db.execute(s"CYPHER runtime=$runtime return 1").asScala.length
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime return 1", Map.empty[String, Object]).resultAsString()
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime return 1", Map.empty[String, Object]).asScala.length
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when consuming the whole iterator for a profile query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      import scala.collection.JavaConverters._
      // when
      db.execute(s"CYPHER runtime=$runtime profile return 1").asScala.length
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime profile return 1", Map.empty[String, Object]).resultAsString()
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime profile return 1", Map.empty[String, Object]).asScala.length
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when consuming the whole iterator for an explain query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      import scala.collection.JavaConverters._
      // when
      db.execute(s"CYPHER runtime=$runtime explain return 1").asScala.length
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime explain return 1", Map.empty[String, Object]).resultAsString()
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime explain return 1", Map.empty[String, Object]).asScala.length
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when consuming the whole iterator for a procedure query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      procedures(service).register(new AllNodesProcedure())
      txBridge(service).hasTransaction shouldBe false

      import scala.collection.JavaConverters._
      // when
      db.execute(s"CYPHER runtime=$runtime CALL org.neo4j.bench.getAllNodes()").asScala.length
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).resultAsString()
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).asScala.length
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when consuming the whole iterator for a profile procedure query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      procedures(service).register(new AllNodesProcedure())
      txBridge(service).hasTransaction shouldBe false

      import scala.collection.JavaConverters._
      // when
      db.execute(s"CYPHER runtime=$runtime profile CALL org.neo4j.bench.getAllNodes()").asScala.length
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime profile CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).resultAsString()
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime profile CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).asScala.length
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when consuming the whole iterator for an explain procedure query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      procedures(service).register(new AllNodesProcedure())
      txBridge(service).hasTransaction shouldBe false

      import scala.collection.JavaConverters._
      // when
      db.execute(s"CYPHER runtime=$runtime explain CALL org.neo4j.bench.getAllNodes()").asScala.length
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime explain CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).resultAsString()
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime explain CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).asScala.length
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when visiting the result for a regular query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      // when
      db.execute(s"CYPHER runtime=$runtime return 1").accept(consumerVisitor)
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime return 1", Map.empty[String, Object]).accept(consumerVisitor)
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when visiting the result for a profile query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      // when
      db.execute(s"CYPHER runtime=$runtime profile return 1").accept(consumerVisitor)
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime profile return 1", Map.empty[String, Object]).accept(consumerVisitor)
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when visiting the result for an explain query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      // when
      db.execute(s"CYPHER runtime=$runtime explain return 1").accept(consumerVisitor)
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime explain return 1", Map.empty[String, Object]).accept(consumerVisitor)
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when visiting the result for a procedure query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      procedures(service).register(new AllNodesProcedure())
      txBridge(service).hasTransaction shouldBe false

      db.execute(s"CYPHER runtime=$runtime CALL org.neo4j.bench.getAllNodes()").accept(consumerVisitor)

      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).accept(consumerVisitor)
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when visiting the result for a profile procedure query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      procedures(service).register(new AllNodesProcedure())
      txBridge(service).hasTransaction shouldBe false

      db.execute(s"CYPHER runtime=$runtime profile CALL org.neo4j.bench.getAllNodes()").accept(consumerVisitor)

      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime profile CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).accept(consumerVisitor)
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when visiting the result for an explain procedure query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      procedures(service).register(new AllNodesProcedure())
      txBridge(service).hasTransaction shouldBe false

      db.execute(s"CYPHER runtime=$runtime explain CALL org.neo4j.bench.getAllNodes()").accept(consumerVisitor)

      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime explain CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).accept(consumerVisitor)
      // then
      txBridge(service).hasTransaction shouldBe false
    }
  }

  private val consumerVisitor = new ResultVisitor[RuntimeException] {
    override def visit(row: ResultRow): Boolean = true
  }

  private def txBridge(db: GraphDatabaseQueryService) = {
    db.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge])
  }

  private def procedures(db: GraphDatabaseQueryService) = {
    db.getDependencyResolver.resolveDependency(classOf[Procedures])
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
      new RawIterator[Array[AnyRef], ProcedureException] {
        var index = 0

        override def next(): Array[AnyRef] = {
          val value = nodeBuffer(index)
          index += 1
          Array(new java.lang.Long(value))
        }

        override def hasNext: Boolean = {
          nodeBuffer.length < index
        }
      }
    }
  }

}
