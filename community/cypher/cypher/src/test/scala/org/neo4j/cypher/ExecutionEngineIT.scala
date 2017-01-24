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
package org.neo4j.cypher

import org.neo4j.collection.RawIterator
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.compiler.v3_2.CostBasedPlannerName
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.helpers.GraphIcing
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.graphdb.Result.{ResultRow, ResultVisitor}
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.exceptions.ProcedureException
import org.neo4j.kernel.api.proc.Context.KERNEL_TRANSACTION
import org.neo4j.kernel.api.proc._
import org.neo4j.kernel.api.Statement
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.procedure.Mode
import org.neo4j.test.TestGraphDatabaseFactory

import scala.collection.immutable.Map

class ExecutionEngineIT extends CypherFunSuite with GraphIcing {

  test("by default when using cypher 2.3 some queries should default to COST") {
    //given
    val db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.cypher_parser_version, "2.3").newGraphDatabase()
    val service = new GraphDatabaseCypherService(db)

    //when
    val plan1 = service.planDescriptionForQuery("PROFILE MATCH (a) RETURN a")
    val plan2 = service.planDescriptionForQuery("PROFILE MATCH (a)-[:T*]-(a) RETURN a")

    //then
    plan1.getArguments.get("planner") should equal("COST")
    plan1.getArguments.get("planner-impl") should equal(CostBasedPlannerName.default.name)
    plan2.getArguments.get("planner") should equal("COST")
    plan2.getArguments.get("planner-impl") should equal(CostBasedPlannerName.default.name)
  }

  test("by default when using cypher 3.1 some queries should default to COST") {
    //given
    val db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.cypher_parser_version, "3.1").newGraphDatabase()
    val service = new GraphDatabaseCypherService(db)

    //when
    val plan1 = service.planDescriptionForQuery("PROFILE MATCH (a) RETURN a")
    val plan2 = service.planDescriptionForQuery("PROFILE MATCH (a)-[:T*]-(a) RETURN a")

    //then
    plan1.getArguments.get("planner") should equal("COST")
    plan1.getArguments.get("planner-impl") should equal(CostBasedPlannerName.default.name)
    plan2.getArguments.get("planner") should equal("COST")
    plan2.getArguments.get("planner-impl") should equal(CostBasedPlannerName.default.name)
  }

  test("by default when using cypher 3.2 some queries should default to COST") {
    //given
    val db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.cypher_parser_version, "3.2").newGraphDatabase()
    val service = new GraphDatabaseCypherService(db)

    //when
    val plan1 = service.planDescriptionForQuery("PROFILE MATCH (a) RETURN a")
    val plan2 = service.planDescriptionForQuery("PROFILE MATCH (a)-[:T*]-(a) RETURN a")

    //then
    plan1.getArguments.get("planner") should equal("COST")
    plan1.getArguments.get("planner-impl") should equal(CostBasedPlannerName.default.name)
    plan2.getArguments.get("planner") should equal("COST")
    plan2.getArguments.get("planner-impl") should equal(CostBasedPlannerName.default.name)
  }

  test("should be able to set RULE as default when using cypher 2.3") {
    //given
    val db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.cypher_planner, "RULE")
      .setConfig(GraphDatabaseSettings.cypher_parser_version, "2.3").newGraphDatabase()
    val service = new GraphDatabaseCypherService(db)

    //when
    val plan = service.planDescriptionForQuery("PROFILE MATCH (a) RETURN a")

    //then
    plan.getArguments.get("planner") should equal("RULE")
    plan.getArguments.get("planner-impl") should equal("RULE")
  }

  test("should be able to set RULE as default when using cypher 3.1") {
    //given
    val db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.cypher_planner, "RULE")
      .setConfig(GraphDatabaseSettings.cypher_parser_version, "3.1").newGraphDatabase()
    val service = new GraphDatabaseCypherService(db)

    //when
    val plan = service.planDescriptionForQuery("PROFILE MATCH (a) RETURN a")

    //then
    plan.getArguments.get("planner") should equal("RULE")
    plan.getArguments.get("planner-impl") should equal("RULE")
  }

  test("should be able to force COST as default when using cypher 2.3") {
    //given
    val db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.cypher_planner, "COST")
      .setConfig(GraphDatabaseSettings.cypher_parser_version, "2.3").newGraphDatabase()
    val service = new GraphDatabaseCypherService(db)

    //when
    val plan = service.planDescriptionForQuery("PROFILE MATCH (a)-[:T*]-(a) RETURN a")

    //then
    plan.getArguments.get("planner") should equal("COST")
    plan.getArguments.get("planner-impl") should equal("IDP")
  }

  test("should be able to force COST as default when using cypher 3.1") {
    //given
    val db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.cypher_planner, "COST")
      .setConfig(GraphDatabaseSettings.cypher_parser_version, "3.1").newGraphDatabase()
    val service = new GraphDatabaseCypherService(db)

    //when
    val plan = service.planDescriptionForQuery("PROFILE MATCH (a)-[:T*]-(a) RETURN a")

    //then
    plan.getArguments.get("planner") should equal("COST")
    plan.getArguments.get("planner-impl") should equal("IDP")
  }

  test("should be able to force COST as default when using cypher 3.2") {
    //given
    val db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.cypher_planner, "COST")
      .setConfig(GraphDatabaseSettings.cypher_parser_version, "3.2").newGraphDatabase()
    val service = new GraphDatabaseCypherService(db)

    //when
    val plan = service.planDescriptionForQuery("PROFILE MATCH (a)-[:T*]-(a) RETURN a")

    //then
    plan.getArguments.get("planner") should equal("COST")
    plan.getArguments.get("planner-impl") should equal("IDP")
  }

  test("should work if query cache size is set to zero") {
    //given
    val db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.query_cache_size, "0").newGraphDatabase()

    // when
    db.execute("RETURN 42").close()

    // then no exception is thrown
  }

  test("should not leak transaction when closing the result for a query") {
    //given
    val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
    val service = new GraphDatabaseCypherService(db)
    val engine = new ExecutionEngine(service)

    // when
    db.execute("return 1").close()
    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("return 1", Map.empty[String, Object]).close()
    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("return 1", Map.empty[String, Object]).javaIterator.close()
    // then
    txBridge(service).hasTransaction shouldBe false
  }

  test("should not leak transaction when closing the result for a profile query") {
    //given
    val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
    val service = new GraphDatabaseCypherService(db)
    val engine = new ExecutionEngine(service)

    // when
    db.execute("profile return 1").close()
    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("profile return 1", Map.empty[String, Object]).close()
    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("profile return 1", Map.empty[String, Object]).javaIterator.close()
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

  test("should not leak transaction when closing the result for an explain query") {
    //given
    val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
    val service = new GraphDatabaseCypherService(db)
    val engine = new ExecutionEngine(service)

    // when
    db.execute("explain return 1").close()
    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("explain return 1", Map.empty[String, Object]).close()
    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("explain return 1", Map.empty[String, Object]).javaIterator.close()
    // then
    txBridge(service).hasTransaction shouldBe false
  }

  test("should not leak transaction when failing in pre-parsing") {
    //given
    val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
    val service = new GraphDatabaseCypherService(db)
    val engine = new ExecutionEngine(service)

    // when
    intercept[SyntaxException](engine.execute("", Map.empty[String, Object]))
    // then
    txBridge(service).hasTransaction shouldBe false

  }

  test("should not leak transaction when closing the result for a procedure query") {
    //given
    val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
    val service = new GraphDatabaseCypherService(db)
    val engine = new ExecutionEngine(service)

    procedures(service).register(new AllNodesProcedure())
    txBridge(service).hasTransaction shouldBe false

    // when
    db.execute("CALL org.neo4j.bench.getAllNodes()").close()
    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).close()
    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).javaIterator.close()
    // then
    txBridge(service).hasTransaction shouldBe false
  }

  test("should not leak transaction when closing the result for a profile procedure query") {
    //given
    val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
    val service = new GraphDatabaseCypherService(db)
    val engine = new ExecutionEngine(service)

    procedures(service).register(new AllNodesProcedure())
    txBridge(service).hasTransaction shouldBe false

    // when
    db.execute("profile CALL org.neo4j.bench.getAllNodes()").close()
    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("profile CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).close()
    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("profile CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).javaIterator.close()
    // then
    txBridge(service).hasTransaction shouldBe false
  }

  test("should not leak transaction when closing the result for an explain procedure query") {
    //given
    val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
    val service = new GraphDatabaseCypherService(db)
    val engine = new ExecutionEngine(service)

    procedures(service).register(new AllNodesProcedure())
    txBridge(service).hasTransaction shouldBe false

    // when
    db.execute("explain CALL org.neo4j.bench.getAllNodes()").close()
    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("explain CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).close()
    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("explain CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).javaIterator.close()
    // then
    txBridge(service).hasTransaction shouldBe false
  }

  test("should not leak transaction when consuming the whole iterator for a regular query") {
    //given
    val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
    val service = new GraphDatabaseCypherService(db)
    val engine = new ExecutionEngine(service)

    import scala.collection.JavaConverters._
    // when
    db.execute("return 1").asScala.length
    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("return 1", Map.empty[String, Object]).length
    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("return 1", Map.empty[String, Object]).javaIterator.asScala.length
    // then
    txBridge(service).hasTransaction shouldBe false
  }

  test("should not leak transaction when consuming the whole iterator for a profile query") {
    //given
    val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
    val service = new GraphDatabaseCypherService(db)
    val engine = new ExecutionEngine(service)

    import scala.collection.JavaConverters._
    // when
    db.execute("profile return 1").asScala.length
    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("profile return 1", Map.empty[String, Object]).length
    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("profile return 1", Map.empty[String, Object]).javaIterator.asScala.length
    // then
    txBridge(service).hasTransaction shouldBe false
  }

  test("should not leak transaction when consuming the whole iterator for an explain query") {
    //given
    val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
    val service = new GraphDatabaseCypherService(db)
    val engine = new ExecutionEngine(service)

    import scala.collection.JavaConverters._
    // when
    db.execute("explain return 1").asScala.length
    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("explain return 1", Map.empty[String, Object]).length
    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("explain return 1", Map.empty[String, Object]).javaIterator.asScala.length
    // then
    txBridge(service).hasTransaction shouldBe false
  }

  test("should not leak transaction when consuming the whole iterator for a procedure query") {
    //given
    val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
    val service = new GraphDatabaseCypherService(db)
    val engine = new ExecutionEngine(service)

    procedures(service).register(new AllNodesProcedure())
    txBridge(service).hasTransaction shouldBe false

    import scala.collection.JavaConverters._
    // when
    db.execute("CALL org.neo4j.bench.getAllNodes()").asScala.length
    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).length
    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).javaIterator.asScala.length
    // then
    txBridge(service).hasTransaction shouldBe false
  }

  test("should not leak transaction when consuming the whole iterator for a profile procedure query") {
    //given
    val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
    val service = new GraphDatabaseCypherService(db)
    val engine = new ExecutionEngine(service)

    procedures(service).register(new AllNodesProcedure())
    txBridge(service).hasTransaction shouldBe false

    import scala.collection.JavaConverters._
    // when
    db.execute("profile CALL org.neo4j.bench.getAllNodes()").asScala.length
    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("profile CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).length
    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("profile CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).javaIterator.asScala.length
    // then
    txBridge(service).hasTransaction shouldBe false
  }

  test("should not leak transaction when consuming the whole iterator for an explain procedure query") {
    //given
    val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
    val service = new GraphDatabaseCypherService(db)
    val engine = new ExecutionEngine(service)

    procedures(service).register(new AllNodesProcedure())
    txBridge(service).hasTransaction shouldBe false

    import scala.collection.JavaConverters._
    // when
    db.execute("explain CALL org.neo4j.bench.getAllNodes()").asScala.length
    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("explain CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).length
    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("explain CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).javaIterator.asScala.length
    // then
    txBridge(service).hasTransaction shouldBe false
  }

  test("should not leak transaction when visiting the result for a regular query") {
    //given
    val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
    val service = new GraphDatabaseCypherService(db)
    val engine = new ExecutionEngine(service)

    // when
    db.execute("return 1").accept(consumerVisitor)
    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("return 1", Map.empty[String, Object]).accept(consumerVisitor)
    // then
    txBridge(service).hasTransaction shouldBe false
  }

  test("should not leak transaction when visiting the result for a profile query") {
    //given
    val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
    val service = new GraphDatabaseCypherService(db)
    val engine = new ExecutionEngine(service)

    // when
    db.execute("profile return 1").accept(consumerVisitor)
    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("profile return 1", Map.empty[String, Object]).accept(consumerVisitor)
    // then
    txBridge(service).hasTransaction shouldBe false
  }

  test("should not leak transaction when visiting the result for an explain query") {
    //given
    val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
    val service = new GraphDatabaseCypherService(db)
    val engine = new ExecutionEngine(service)

    // when
    db.execute("explain return 1").accept(consumerVisitor)
    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("explain return 1", Map.empty[String, Object]).accept(consumerVisitor)
    // then
    txBridge(service).hasTransaction shouldBe false
  }

  test("should not leak transaction when visiting the result for a procedure query") {
    //given
    val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
    val service = new GraphDatabaseCypherService(db)
    val engine = new ExecutionEngine(service)

    procedures(service).register(new AllNodesProcedure())
    txBridge(service).hasTransaction shouldBe false

    db.execute("CALL org.neo4j.bench.getAllNodes()").accept(consumerVisitor)

    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).accept(consumerVisitor)
    // then
    txBridge(service).hasTransaction shouldBe false
  }

  test("should not leak transaction when visiting the result for a profile procedure query") {
    //given
    val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
    val service = new GraphDatabaseCypherService(db)
    val engine = new ExecutionEngine(service)

    procedures(service).register(new AllNodesProcedure())
    txBridge(service).hasTransaction shouldBe false

    db.execute("profile CALL org.neo4j.bench.getAllNodes()").accept(consumerVisitor)

    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("profile CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).accept(consumerVisitor)
    // then
    txBridge(service).hasTransaction shouldBe false
  }

  test("should not leak transaction when visiting the result for an explain procedure query") {
    //given
    val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
    val service = new GraphDatabaseCypherService(db)
    val engine = new ExecutionEngine(service)

    procedures(service).register(new AllNodesProcedure())
    txBridge(service).hasTransaction shouldBe false

    db.execute("explain CALL org.neo4j.bench.getAllNodes()").accept(consumerVisitor)

    // then
    txBridge(service).hasTransaction shouldBe false

    // when
    engine.execute("explain CALL org.neo4j.bench.getAllNodes()", Map.empty[String, Object]).accept(consumerVisitor)
    // then
    txBridge(service).hasTransaction shouldBe false
  }

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

  test("should not refer to stale plan context in the cached execution plans") {
    // given
    val db = new TestGraphDatabaseFactory().newImpermanentDatabase()

    // when
    db.execute("EXPLAIN MERGE (a:A) ON MATCH SET a.prop = 21  RETURN *").close()
    db.execute("EXPLAIN    MERGE (a:A) ON MATCH SET a.prop = 42 RETURN *").close()

    // then no exceptions have been thrown

    db.shutdown()
  }

  private val consumerVisitor = new ResultVisitor[RuntimeException] {
    override def visit(row: ResultRow): Boolean = true
  }

  private implicit class RichDb(db: GraphDatabaseCypherService) {
    def planDescriptionForQuery(query: String) = {
      val res = db.execute(query)
      res.resultAsString()
      res.getExecutionPlanDescription
    }
  }

  private def txBridge(db: GraphDatabaseQueryService) = {
    db.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge])
  }

  private def procedures(db: GraphDatabaseQueryService) = {
    db.getDependencyResolver.resolveDependency(classOf[Procedures])
  }

  implicit class RichExecutionEngine(engine: ExecutionEngine) {
    def profile(query: String, params: Map[String, Any]) =
      engine.profile(query, params, engine.queryService.transactionalContext(query = query -> params))

    def execute(query: String, params: Map[String, Any]) =
      engine.execute(query, params, engine.queryService.transactionalContext(query = query -> params))
  }

  class AllNodesProcedure extends CallableProcedure {
    import scala.collection.JavaConverters._

    private val results = Map[String, AnyRef]("node" -> Neo4jTypes.NTInteger)
    val procedureName = new QualifiedName(Array[String]("org", "neo4j", "bench"), "getAllNodes")
    val emptySignature = List.empty[FieldSignature].asJava
    val signature: ProcedureSignature = new ProcedureSignature(
      procedureName, paramSignature, resultSignature, Mode.READ, java.util.Optional.empty(), Array.empty,
      java.util.Optional.empty())

    def paramSignature = List.empty[FieldSignature].asJava

    def resultSignature = results.keys.foldLeft(List.empty[FieldSignature]) { (fields, entry) =>
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
