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
import org.neo4j.cypher.ExecutionEngineHelper.createEngine
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.helpers.GraphIcing
import org.neo4j.cypher.internal.{ExecutionEngine, ExecutionResult}
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Result.{ResultRow, ResultVisitor}
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.Statement
import org.neo4j.kernel.api.exceptions.ProcedureException
import org.neo4j.kernel.api.proc.Context.KERNEL_TRANSACTION
import org.neo4j.kernel.api.proc._
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.procedure.Mode
import org.neo4j.test.TestGraphDatabaseFactory

import scala.collection.immutable.Map

class CloseTransactionTest extends CypherFunSuite with GraphIcing {

  private val runtimes = Seq("interpreted", "compiled")

  for (runtime <- runtimes) {

    test(s"should not leak transaction when closing the result for a query - runtime=$runtime") {
      //given
      val db: GraphDatabaseService = new TestGraphDatabaseFactory().newImpermanentDatabase()
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
      engine.execute(s"CYPHER runtime=$runtime return 1", Map.empty[String, Object]).javaIterator.close()
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when closing the result for a profile query - runtime=$runtime") {
      //given
      val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
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
      engine.execute(s"CYPHER runtime=$runtime profile return 1", Map.empty[String, Object]).javaIterator.close()
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.profile("return 1", Map.empty[String, Object]).close()
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.profile("return 1", Map.empty[String, Object]).javaIterator.close()
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when closing the result for an explain query - runtime=$runtime") {
      //given
      val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
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
      engine.execute(s"CYPHER runtime=$runtime explain return 1", Map.empty[String, Object]).javaIterator.close()
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when failing in pre-parsing - runtime=$runtime") {
      //given
      val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      // when
      intercept[SyntaxException](engine.execute(s"CYPHER runtime=$runtime ", Map.empty[String, Object]))
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when closing the result for a procedure query - runtime=$runtime") {
      //given
      val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
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
      engine.execute(s"CYPHER runtime=$runtime CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).javaIterator.close()
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when closing the result for a profile procedure query - runtime=$runtime") {
      //given
      val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
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
      engine.execute(s"CYPHER runtime=$runtime profile CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).javaIterator.close()
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when closing the result for an explain procedure query - runtime=$runtime") {
      //given
      val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
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
      engine.execute(s"CYPHER runtime=$runtime explain CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).javaIterator.close()
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when consuming the whole iterator for a regular query - runtime=$runtime") {
      //given
      val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      import scala.collection.JavaConverters._
      // when
      db.execute(s"CYPHER runtime=$runtime return 1").asScala.length
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime return 1", Map.empty[String, Object]).length
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime return 1", Map.empty[String, Object]).javaIterator.asScala.length
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when consuming the whole iterator for a profile query - runtime=$runtime") {
      //given
      val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      import scala.collection.JavaConverters._
      // when
      db.execute(s"CYPHER runtime=$runtime profile return 1").asScala.length
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime profile return 1", Map.empty[String, Object]).length
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime profile return 1", Map.empty[String, Object]).javaIterator.asScala.length
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when consuming the whole iterator for an explain query - runtime=$runtime") {
      //given
      val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      import scala.collection.JavaConverters._
      // when
      db.execute(s"CYPHER runtime=$runtime explain return 1").asScala.length
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime explain return 1", Map.empty[String, Object]).length
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime explain return 1", Map.empty[String, Object]).javaIterator.asScala.length
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when consuming the whole iterator for a procedure query - runtime=$runtime") {
      //given
      val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
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
      engine.execute(s"CYPHER runtime=$runtime CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).length
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).javaIterator.asScala.length
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when consuming the whole iterator for a profile procedure query - runtime=$runtime") {
      //given
      val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
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
      engine.execute(s"CYPHER runtime=$runtime profile CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).length
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime profile CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).javaIterator.asScala.length
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when consuming the whole iterator for an explain procedure query - runtime=$runtime") {
      //given
      val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
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
      engine.execute(s"CYPHER runtime=$runtime explain CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).length
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime explain CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).javaIterator.asScala.length
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when visiting the result for a regular query - runtime=$runtime") {
      //given
      val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
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
      val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
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
      val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
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
      val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
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
      val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
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
      val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
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
      java.util.Optional.empty(), java.util.Optional.empty())

    def paramSignature: util.List[FieldSignature] = List.empty[FieldSignature].asJava

    def resultSignature: util.List[FieldSignature] = results.keys.foldLeft(List.empty[FieldSignature]) { (fields, entry) =>
      fields :+ FieldSignature.outputField(entry, results(entry).asInstanceOf[Neo4jTypes.AnyType])
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
