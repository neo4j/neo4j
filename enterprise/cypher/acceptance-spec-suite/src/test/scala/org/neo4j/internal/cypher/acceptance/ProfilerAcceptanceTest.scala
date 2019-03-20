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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.frontend.v3_4.helpers.StringHelper.RichString
import org.neo4j.cypher.internal.planner.v3_4.spi.GraphStatistics
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments.{DbHits, EstimatedRows, Rows, Signature}
import org.neo4j.cypher.internal.runtime.planDescription.{Argument, InternalPlanDescription}
import org.neo4j.cypher.internal.runtime.{CreateTempFileTestSupport, InternalExecutionResult}
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.v3_4.logical.plans.QualifiedName
import org.neo4j.cypher.{ExecutionEngineFunSuite, ProfilerStatisticsNotReadyException, TxCounts}
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.{Configs, TestConfiguration}

import scala.reflect.ClassTag

class ProfilerAcceptanceTest extends ExecutionEngineFunSuite with CreateTempFileTestSupport with CypherComparisonSupport {

  test("profile simple query") {
    createNode()
    createNode()
    createNode()
    val result = profileWithExecute(Configs.All, "MATCH (n) RETURN n")

    assertRows(3)(result)("AllNodesScan", "ProduceResults")
    assertDbHits(0)(result)("ProduceResults")
    assertDbHits(4)(result)("AllNodesScan")
  }

  test("track db hits in Projection") {
    createNode()
    createNode()
    createNode()

    val result = profileWithExecute(Configs.All, "MATCH (n) RETURN (n:Foo)")

    assertRows(3)(result)("AllNodesScan", "ProduceResults")
    assertDbHits(0)(result)("ProduceResults")
    assertDbHits(3)(result)("Projection")
    assertDbHits(4)(result)("AllNodesScan")
  }

   test("profile standalone call") {
        createLabeledNode("Person")
        createLabeledNode("Animal")

        val result = legacyProfile("CALL db.labels")
        assertDbHits(1)(result)("ProcedureCall")
        assertRows(2)(result)("ProcedureCall")
        getPlanDescriptions(result, Seq("ProcedureCall")).foreach { plan =>
          val Signature(QualifiedName(namespaces, procName), _, returnSignature) = plan.arguments.collectFirst {
            case x: Signature => x
          }.getOrElse(fail("expected a procedure signature"))

          namespaces should equal(Seq("db"))
          procName should equal("labels")
          returnSignature should equal(Seq("label" -> CTString))
          plan.variables should equal(Set("label"))
        }
      }

      test("profile call in query") {
        createLabeledNode("Person")
        createLabeledNode("Animal")

        val result = legacyProfile("MATCH (n:Person) CALL db.labels() YIELD label RETURN *")

        assertDbHits(1)(result)("ProcedureCall")
        assertRows(2)(result)("ProcedureCall")
        getPlanDescriptions(result, Seq("ProcedureCall")).foreach { plan =>
          val Signature(QualifiedName(namespaces, procName), _, returnSignature) = plan.arguments.collectFirst {
            case x: Signature => x
          }.getOrElse(fail("expected a procedure signature"))

          namespaces should equal(Seq("db"))
          procName should equal("labels")
          returnSignature should equal(Seq("label" -> CTString))
          plan.variables should equal(Set("n", "label"))
        }
      }

      test("match (n) where (n)-[:FOO]->() return *") {
        //GIVEN
        relate( createNode(), createNode(), "FOO")

        //WHEN
        val result = profileWithExecute(Configs.Interpreted, "match (n) where (n)-[:FOO]->() return *")

        //THEN
        assertRows(1)(result)("Filter")
        assertDbHits(4)(result)("Filter")

        assertRows(2)(result)("AllNodesScan")
        assertDbHits(3)(result)("AllNodesScan")
      }

      test("match (n:A)-->(x:B) return *") {
        //GIVEN
        relate( createLabeledNode("A"), createLabeledNode("B"))

        //WHEN
        val result = profileWithExecute(Configs.All, "match (n:A)-->(x:B) return *")

        //THEN
        assertRows(1)(result)("ProduceResults", "Filter", "Expand(All)", "NodeByLabelScan")
        assertDbHits(0)(result)("ProduceResults")
        assertDbHits(1)(result)("Filter")
        assertDbHits(2)(result)("NodeByLabelScan", "Expand(All)")
      }

      test("PROFILE for Cypher 2.3") {
        val result = graph.execute("cypher 2.3 profile match (n) where (n)-[:FOO]->() return *")

        assert(result.getQueryExecutionType.requestedExecutionPlanDescription, "result not marked with planDescriptionRequested")
        result.getExecutionPlanDescription.toString should include("DB Hits")
      }

      test("PROFILE for Cypher 3.1") {
        val result = graph.execute("cypher 3.1 profile match (n) where (n)-[:FOO]->() return *")

        assert(result.getQueryExecutionType.requestedExecutionPlanDescription, "result not marked with planDescriptionRequested")
        result.getExecutionPlanDescription.toString should include("DB Hits")
      }

      test("match (n) where not (n)-[:FOO]->() return *") {
        //GIVEN
        relate( createNode(), createNode(), "FOO")

        //WHEN
        val result = profileWithExecute(Configs.Interpreted, "match (n) where not (n)-[:FOO]->() return *")

        //THEN
        assertRows(1)(result)("Filter")
        assertDbHits(4)(result)("Filter")

        assertRows(2)(result)("AllNodesScan")
        assertDbHits(3)(result)("AllNodesScan")
      }

      test("unfinished profiler complains [using MATCH]") {
        //GIVEN
        createNode("foo" -> "bar")
        val result = graph.execute("PROFILE match (n) where id(n) = 0 RETURN n")

        //WHEN THEN
        val ex = intercept[QueryExecutionException](result.getExecutionPlanDescription)
        ex.getCause.getCause shouldBe a [ProfilerStatisticsNotReadyException]
        result.close() // ensure that the transaction is closed
      }

      test("unfinished profiler complains [using CALL]") {
        //GIVEN
        createLabeledNode("Person")
        val result = graph.execute("PROFILE CALL db.labels")

        //WHEN THEN
        val ex = intercept[QueryExecutionException](result.getExecutionPlanDescription)
        ex.getCause.getCause shouldBe a [ProfilerStatisticsNotReadyException]
        result.close() // ensure that the transaction is closed
      }

      test("unfinished profiler complains [using CALL within larger query]") {
        //GIVEN
        createLabeledNode("Person")
        val result = graph.execute("PROFILE CALL db.labels() YIELD label WITH label as r RETURN r")

        //WHEN THEN
        val ex = intercept[QueryExecutionException](result.getExecutionPlanDescription)
        ex.getCause.getCause shouldBe a [ProfilerStatisticsNotReadyException]
        result.close() // ensure that the transaction is closed
      }

      test("tracks number of rows") {
        //GIVEN
        // due to the cost model, we need a bunch of nodes for the planner to pick a plan that does lookup by id
        (1 to 100).foreach(_ => createNode())

        val result = profileWithExecute(Configs.All, "match (n) where id(n) = 0 RETURN n")

        //WHEN THEN
        assertRows(1)(result)("NodeByIdSeek")
      }

      test("tracks number of graph accesses") {
        //GIVEN
        // due to the cost model, we need a bunch of nodes for the planner to pick a plan that does lookup by id
        (1 to 100).foreach(_ => createNode("foo" -> "bar"))

        val result = profileWithExecute(Configs.All, "match (n) where id(n) = 0 RETURN n.foo")

        //WHEN THEN
        assertRows(1)(result)("ProduceResults", "Projection", "NodeByIdSeek")
        assertDbHits(0)(result)("ProduceResults")
        assertDbHits(1)(result)("Projection")
        assertDbHits(1)(result)("NodeByIdSeek")
      }

      test("no problem measuring creation") {
        //GIVEN
        val result = legacyProfile("CREATE (n)")

        //WHEN THEN
        assertDbHits(0)(result)("EmptyResult")
      }

      test("tracks graph global queries") {
        createNode()

        //GIVEN
        val result = profileWithExecute(Configs.All, "MATCH (n) RETURN n.foo")

        //WHEN THEN
        assertRows(1)(result)("ProduceResults")
        assertDbHits(0)(result)("ProduceResults")

        assertRows(1)(result)("Projection")
        assertDbHits(1)(result)("Projection")

        assertRows(1)(result)("AllNodesScan")
        assertDbHits(2)(result)("AllNodesScan")
      }

      test("tracks optional matches") {
        //GIVEN
        createNode()

        // WHEN
        val result = profileWithExecute(Configs.Interpreted, "MATCH (n) optional match (n)-->(x) return x")

        // THEN
        assertDbHits(0)(result)("ProduceResults")
        assertDbHits(1)(result)("OptionalExpand(All)")
        assertDbHits(2)(result)("AllNodesScan")
      }

      test("allows optional match to start a query") {
        // WHEN
        val result = profileWithExecute(Configs.Interpreted, "optional match (n) return n")

        // THEN
        assertRows(1)(result)("Optional")
      }

      test("should produce profile when using limit") {
        // GIVEN
        createNode()
        createNode()
        createNode()
        val result = profileWithExecute(Configs.All, """MATCH (n) RETURN n LIMIT 1""")

        // WHEN
        result.toList

        // THEN PASS
        assertRows(1)(result)("AllNodesScan", "ProduceResults")
        assertDbHits(0)(result)("ProduceResults")
        assertDbHits(2)(result)("AllNodesScan")

        result.executionPlanDescription()
      }

      test("LIMIT should influence cardinality estimation even when parameterized") {
        (0 until 100).map(i => createLabeledNode("Person"))
        val result = executeWith(Configs.All, s"PROFILE MATCH (p:Person) RETURN p LIMIT {limit}", params = Map("limit" -> 10))
        assertEstimatedRows(GraphStatistics.DEFAULT_LIMIT_CARDINALITY.amount.toInt)(result)("Limit")
      }

      test("LIMIT should influence cardinality estimation with literal") {
        (0 until 100).map(i => createLabeledNode("Person"))
        val result = executeWith(Configs.All, s"PROFILE MATCH (p:Person) RETURN p LIMIT 10")
        assertEstimatedRows(10)(result)("Limit")
      }

      test("LIMIT should influence cardinality estimation with literal and parameters") {
        (0 until 100).map(i => createLabeledNode("Person"))
        val result = executeWith(Configs.All, s"PROFILE MATCH (p:Person) WHERE 50 = {fifty} RETURN p LIMIT 10", params = Map("fifty" -> 50))
        assertEstimatedRows(10)(result)("Limit")
      }

      test("LIMIT should influence cardinality estimation with independent parameterless expression") {
        (0 until 100).map(i => createLabeledNode("Person"))
        val result = executeWith(Configs.Interpreted, s"PROFILE MATCH (p:Person) with 10 as x, p RETURN p LIMIT toInt(ceil(cos(0))) + 4")
        assertEstimatedRows(5)(result)("Limit")
      }

      test("LIMIT should influence cardinality estimation by default value when expression contains parameter") {
        (0 until 100).map(i => createLabeledNode("Person"))
        val result = executeWith(Configs.Interpreted, s"PROFILE MATCH (p:Person) with 10 as x, p RETURN p LIMIT toInt(sin({limit}))", params = Map("limit" -> 1))
        assertEstimatedRows(GraphStatistics.DEFAULT_LIMIT_CARDINALITY.amount.toInt)(result)("Limit")
      }

      test("LIMIT should influence cardinality estimation by default value when expression contains rand()") {
        (0 until 100).map(i => createLabeledNode("Person"))
        // NOTE: We cannot executeWith because of random result
        val result = innerExecuteDeprecated(s"PROFILE MATCH (p:Person) with 10 as x, p RETURN p LIMIT toInt(rand()*10)", Map.empty)
        assertEstimatedRows(GraphStatistics.DEFAULT_LIMIT_CARDINALITY.amount.toInt)(result)("Limit")
      }

      test("LIMIT should influence cardinality estimation by default value when expression contains timestamp()") {
        (0 until 100).map(i => createLabeledNode("Person"))
        //TODO this cannot be run with executeWith since it will occasionally succeed on 2.3 and we have decided not
        //to fix this on 2.3. So if we fix the issue on 2.3 or if we no longer need to depend on 2.3 we should update test
        //to run with `executeWith`
        assertEstimatedRows(GraphStatistics.DEFAULT_LIMIT_CARDINALITY.amount.toInt)(
          innerExecuteDeprecated(s"PROFILE MATCH (p:Person) with 10 as x, p RETURN p LIMIT timestamp()", Map.empty))("Limit")
        assertEstimatedRows(GraphStatistics.DEFAULT_LIMIT_CARDINALITY.amount.toInt)(
          innerExecuteDeprecated(s"PROFILE CYPHER runtime=slotted MATCH (p:Person) with 10 as x, p RETURN p LIMIT timestamp()", Map.empty))("Limit")
        assertEstimatedRows(GraphStatistics.DEFAULT_LIMIT_CARDINALITY.amount.toInt)(
          innerExecuteDeprecated(s"PROFILE CYPHER runtime=interpreted MATCH (p:Person) with 10 as x, p RETURN p LIMIT timestamp()", Map.empty))("Limit")
      }

      test ("should support profiling union queries") {
        val result = profileWithExecute(Configs.Interpreted, "return 1 as A union return 2 as A")
        result.toSet should equal(Set(Map("A" -> 1), Map("A" -> 2)))
      }

      test("should support profiling merge_queries") {
        val result = legacyProfile("merge (a {x: 1}) return a.x as A")
        result.toList.head("A") should equal(1)
      }

      test("should support profiling optional match queries") {
        createLabeledNode(Map("x" -> 1), "Label")
        val result = profileWithExecute(Configs.Interpreted, "match (a:Label {x: 1}) optional match (a)-[:REL]->(b) return a.x as A, b.x as B").toList.head
        result("A") should equal(1)
        result("B") should equal(null.asInstanceOf[Int])
      }

      test("should support profiling optional match and with") {
        createLabeledNode(Map("x" -> 1), "Label")
        val executionResult: InternalExecutionResult = profileWithExecute(Configs.Interpreted, "match (n) optional match (n)--(m) with n, m where m is null return n.x as A")
        val result = executionResult.toList.head
        result("A") should equal(1)
      }

      test("should handle PERIODIC COMMIT when profiling") {
        val url = createTempFileURL("cypher", ".csv")(writer => {
          (1 to 100).foreach(writer.println)
        }).cypherEscape

        val query = s"USING PERIODIC COMMIT 10 LOAD CSV FROM '$url' AS line CREATE()"

        // given
        executeWith(Configs.Interpreted - Configs.Cost2_3, query).toList
        deleteAllEntities()
        val initialTxCounts = graph.txCounts

        // when
        val result = legacyProfile(query)

        // then
        val expectedTxCount = 10  // One per 10 rows of CSV file

        graph.txCounts-initialTxCounts should equal(TxCounts(commits = expectedTxCount))
        result.queryStatistics().containsUpdates should equal(true)
        result.queryStatistics().nodesCreated should equal(100)
      }

      test("should not have a problem profiling empty results") {
        val result = profileWithExecute(Configs.Interpreted, "MATCH (n) WHERE (n)-->() RETURN n")

        result shouldBe empty
        result.executionPlanDescription().toString should include("AllNodes")
      }

      test("reports COST planner when showing plan description") {
        val result = graph.execute("CYPHER planner=cost match (n) return n")
        result.resultAsString()
        result.getExecutionPlanDescription.toString should include("Planner COST" + System.lineSeparator())
      }

      test("does not use Apply for aggregation and order by") {
        val a = profileWithExecute(Configs.Interpreted, "match (n) return n, count(*) as c order by c")

        a.executionPlanDescription().toString should not include "Apply"
      }

      //this test asserts a specific optimization in pipe building and is not
      //valid for the compiled runtime
      test("should not use eager plans for distinct") {
        val a = innerExecuteDeprecated("PROFILE CYPHER runtime=interpreted MATCH (n) RETURN DISTINCT n.name", Map.empty)
        a.executionPlanDescription().toString should not include "Eager"
      }

      test("should not show  EstimatedRows in legacy profiling") {
        val result = legacyProfile("create()")
        result.executionPlanDescription().toString should not include "EstimatedRows"
      }

      test("match (p:Person {name:'Seymour'}) return (p)-[:RELATED_TO]->()") {
        //GIVEN
        val seymour = createLabeledNode(Map("name" -> "Seymour"), "Person")
        relate(seymour, createLabeledNode(Map("name" -> "Buddy"), "Person"), "RELATED_TO")
        relate(seymour, createLabeledNode(Map("name" -> "Boo Boo"), "Person"), "RELATED_TO")
        relate(seymour, createLabeledNode(Map("name" -> "Walt"), "Person"), "RELATED_TO")
        relate(seymour, createLabeledNode(Map("name" -> "Waker"), "Person"), "RELATED_TO")
        relate(seymour, createLabeledNode(Map("name" -> "Zooey"), "Person"), "RELATED_TO")
        relate(seymour, createLabeledNode(Map("name" -> "Franny"), "Person"), "RELATED_TO")
        // pad with enough nodes to make index seek considered more efficient than label scan
        createLabeledNode(Map("name" -> "Dummy1"), "Person")
        createLabeledNode(Map("name" -> "Dummy2"), "Person")
        createLabeledNode(Map("name" -> "Dummy3"), "Person")

        graph.createConstraint("Person", "name")

        //WHEN
        val result = profileWithExecute(Configs.Interpreted, "match (p:Person {name:'Seymour'}) return (p)-[:RELATED_TO]->()")

        //THEN
        assertDbHits(7)(result)("Expand(All)")
        assertDbHits(3)(result)("NodeUniqueIndexSeek")
       }

      test("should show expand without types in a simple form") {
        val a = profileWithExecute(Configs.All, "match (n)-->() return *")

        a.executionPlanDescription().toString should include("()<--(n)")
      }

      test("should show expand with types in a simple form") {
        val result = profileWithExecute(Configs.All, "match (n)-[r:T]->() return *")

        result.executionPlanDescription().toString should include("()<-[r:T]-(n)")
      }

      test("should report correct dbhits and rows for label scan") {
        // given
        createLabeledNode("Label1")

        // when
        val result = profileWithExecute(Configs.All, "match (n:Label1) return n")

        // then
        assertDbHits(2)(result)("NodeByLabelScan")
        assertRows(1)(result)("NodeByLabelScan")
      }

      test("should report correct dbhits and rows for expand") {
        // given
        relate(createNode(), createNode())

        // when
        val result = profileWithExecute(Configs.All, "match (n)-->(x) return x")

        // then
        assertDbHits(3)(result)("Expand(All)")
        assertRows(1)(result)("Expand(All)")
      }

      test("should report correct dbhits and rows for literal addition") {
        // when
        val result = profileWithExecute(Configs.All, "return 5 + 3")

        // then
        assertDbHits(0)(result)("Projection", "ProduceResults")
        assertRows(1)(result)("ProduceResults")
      }

      test("should report correct dbhits and rows for property addition") {
        // given
        createNode("name" -> "foo")

        // when
        val result = profileWithExecute(Configs.All, "match (n) return n.name + 3")

        // then
        assertDbHits(1)(result)("Projection")
        assertRows(1)(result)("Projection")
      }

      test("should report correct dbhits and rows for property subtraction") {
        // given
        createNode("name" -> 10)

        // when
        val result = profileWithExecute(Configs.All, "match (n) return n.name - 3")

        // then
        assertDbHits(1)(result)("Projection")
        assertRows(1)(result)("Projection")
      }

      test("should throw if accessing profiled results before they have been materialized") {
        createNode()
        val result = graph.execute("profile match (n) return n")

        val ex = intercept[QueryExecutionException](result.getExecutionPlanDescription)
        ex.getCause.getCause shouldBe a [ProfilerStatisticsNotReadyException]
        result.close() // ensure that the transaction is closed
      }

      test("should handle cartesian products") {
        createNode()
        createNode()
        createNode()
        createNode()

        val result = profileWithExecute(Configs.All, "match (n), (m) return n, m")
        assertRows(16)(result)("CartesianProduct")
      }

      test("should properly handle filters") {
        // given
        val n = createLabeledNode(Map("name" -> "Seymour"), "Glass")
        val o = createNode()
        relate(n, o, "R1")
        relate(o, createLabeledNode(Map("name" -> "Zoey"), "Glass"), "R2")
        relate(o, createLabeledNode(Map("name" -> "Franny"), "Glass"), "R2")
        relate(o, createNode(), "R2")
        relate(o, createNode(), "R2")
        graph.createIndex("Glass", "name")

        // when
        val result = profileWithExecute(Configs.All,
          "match (n:Glass {name: 'Seymour'})-[:R1]->(o)-[:R2]->(p:Glass) USING INDEX n:Glass(name) return p.name")

        // then
        assertRows(2)(result)("Filter")
      }

      test("interpreted runtime projections") {
        // given
        val n = createLabeledNode(Map("name" -> "Seymour"), "Glass")
        val o = createNode()
        relate(n, o, "R1")
        relate(o, createLabeledNode(Map("name" -> "Zoey"), "Glass"), "R2")
        relate(o, createLabeledNode(Map("name" -> "Franny"), "Glass"), "R2")
        relate(o, createNode(), "R2")
        relate(o, createNode(), "R2")
        graph.createIndex("Glass", "name")

        // when
        val result = innerExecuteDeprecated(
          "profile cypher runtime=interpreted match (n:Glass {name: 'Seymour'})-[:R1]->(o)-[:R2]->(p:Glass) USING INDEX n:Glass(name) return p.name", Map.empty)

        // then
        assertDbHits(2)(result)("Projection")
      }

      test("profile projections") {
        // given
        val n = createLabeledNode(Map("name" -> "Seymour"), "Glass")
        val o = createNode()
        relate(n, o, "R1")
        relate(o, createLabeledNode(Map("name" -> "Zoey"), "Glass"), "R2")
        relate(o, createLabeledNode(Map("name" -> "Franny"), "Glass"), "R2")
        relate(o, createNode(), "R2")
        relate(o, createNode(), "R2")
        graph.createIndex("Glass", "name")

        // when
        val result = innerExecuteDeprecated(
          "profile match (n:Glass {name: 'Seymour'})-[:R1]->(o)-[:R2]->(p:Glass) USING INDEX n:Glass(name) return p.name", Map.empty)

        // then
        assertDbHits(2)(result)("Projection")
      }

      test("profile filter") {
        // given
        val n = createLabeledNode(Map("name" -> "Seymour"), "Glass")
        val o = createNode()
        relate(n, o, "R1")
        relate(o, createLabeledNode(Map("name" -> "Zoey"), "Glass"), "R2")
        relate(o, createLabeledNode(Map("name" -> "Franny"), "Glass"), "R2")
        relate(o, createNode(), "R2")
        relate(o, createNode(), "R2")
        graph.createIndex("Glass", "name")

        // when
        val result = innerExecuteDeprecated(
          "profile match (n:Glass {name: 'Seymour'})-[:R1]->(o)-[:R2]->(p) USING INDEX n:Glass(name) WHERE p.name = 'Franny' return p.name", Map.empty)

        // then
        assertDbHits(1)(result)("Projection")
        assertDbHits(4)(result)("Filter")
      }

      test("joins with identical scans") {
        //given
        val corp = createLabeledNode("Company")

        //force a plan to have a scan on corp in both the lhs and the rhs of join
        val query =
          """PROFILE MATCH (a:Company) RETURN a
            |UNION
            |MATCH (a:Company) RETURN a""".stripMargin

        //when
        val result = innerExecuteDeprecated(query, Map.empty)

        result.toSet should be(Set(Map("a" -> corp), Map("a" -> corp)))

        //then
        assertDbHits(2)(result)("NodeByLabelScan")
        assertRows(1)(result)("NodeByLabelScan")
      }

      //this test asserts a specific optimization in pipe building and is not
      //valid for the compiled runtime
      test("distinct should not look up properties every time") {
        // GIVEN
        createNode("prop"-> 42)
        createNode("prop"-> 42)

        // WHEN
        val result = innerExecuteDeprecated("PROFILE CYPHER runtime=interpreted MATCH (n) RETURN DISTINCT n.prop", Map.empty)

        // THEN
        assertDbHits(2)(result)("Distinct")
      }

      test("profile with filter using nested expressions pipe should report dbhits correctly") {
        // GIVEN
        createLabeledNode(Map("category_type"-> "cat"), "Category")
        createLabeledNode(Map("category_type"-> "cat"), "Category")
        val e1 = createLabeledNode(Map("domain_id"-> "1"), "Entity")
        val e2 = createLabeledNode(Map("domain_id"-> "2"), "Entity")
        val aNode = createNode()
        relate(aNode, e1)
        val anotherNode = createNode()
        relate(anotherNode, e2)

        relate(aNode, createNode(), "HAS_CATEGORY")
        relate(anotherNode, createNode(), "HAS_CATEGORY")

        // WHEN
        val result = profileWithExecute(Configs.Interpreted,
          """MATCH (cat:Category)
            |WITH collect(cat) as categories
            |MATCH (m:Entity)
            |WITH m, categories
            |MATCH (m)<-[r]-(n)
            |WHERE ANY(x IN categories WHERE (n)-[:HAS_CATEGORY]->(x))
            |RETURN count(n)""".stripMargin)

        // THEN
        assertDbHits(14)(result)("Filter")
      }

  test("profile pruning var length expand"){
    //some graph
    val a = createLabeledNode("Start")
    val b1 = createLabeledNode("Node")
    val b2 = createLabeledNode("Node")
    val b3 = createLabeledNode("Node")
    val b4 = createLabeledNode("Node")
    relate(a, b1, "T1")
    relate(b1, b2, "T1")
    relate(b2, b3, "T1")
    relate(b2, b4, "T1")

    val query = "profile match (b:Start)-[*3]->(d) return count(distinct d)"
    val result = executeWith(Configs.Interpreted, query)

    assertRows(2)(result)("VarLengthExpand(Pruning)")
    assertDbHits(7)(result)("VarLengthExpand(Pruning)")

  }

  test("profiling with compiled runtime") {
    //given
    createLabeledNode("L")
    createLabeledNode("L")
    createLabeledNode("L")

    //when
    val result = innerExecuteDeprecated("PROFILE CYPHER runtime=compiled MATCH (n:L) RETURN count(n.prop)", Map.empty)

    //then
    assertRows(1)(result)("EagerAggregation")
  }

  private def assertRows(expectedRows: Int)(result: InternalExecutionResult)(names: String*) {
    getPlanDescriptions(result, names).foreach {
      plan => assert(getArgument[Rows](plan).value === expectedRows, s" wrong row count for plan: ${plan.name}")
    }
  }

  private def assertEstimatedRows(expectedRows: Int)(result: InternalExecutionResult)(names: String*) {
    getPlanDescriptions(result, names).foreach {
      plan => assert(getArgument[EstimatedRows](plan).value === expectedRows , s" wrong estiamted row count for plan: ${plan.name}")
    }
  }

  private def assertDbHits(expectedRows: Int)(result: InternalExecutionResult)(names: String*) {
    getPlanDescriptions(result, names).foreach {
      plan => assert(getArgument[DbHits](plan).value === expectedRows , s" wrong db hits for plan: ${plan.name}")
    }
  }

  type Planner = (String, Map[String, Any]) => InternalExecutionResult

  def profileWithPlanner(planner: Planner, q: String, params: Map[String, Any]): InternalExecutionResult = {
    val result = planner("profile " + q, params)
    assert(result.planDescriptionRequested, "result not marked with planDescriptionRequested")

    val planDescription: InternalPlanDescription = result.executionPlanDescription()
    planDescription.flatten.foreach {
      p =>
        if (!p.arguments.exists(_.isInstanceOf[DbHits])) {
          fail("Found plan that was not profiled with DbHits: " + p.name)
        }
        if (!p.arguments.exists(_.isInstanceOf[Rows])) fail("Found plan that was not profiled with Rows: " + p.name)
    }
    result
  }

  def profileWithExecute(configuration: TestConfiguration, q: String): InternalExecutionResult = {
    val result = executeWith(configuration, "profile " + q)
    assert(result.planDescriptionRequested, "result not marked with planDescriptionRequested")

    val planDescription: InternalPlanDescription = result.executionPlanDescription()
    planDescription.flatten.foreach {
      p =>
        if (!p.arguments.exists(_.isInstanceOf[DbHits])) {
          fail("Found plan that was not profiled with DbHits: " + p.name)
        }
        if (!p.arguments.exists(_.isInstanceOf[Rows])) fail("Found plan that was not profiled with Rows: " + p.name)
    }
    result
  }

  override def profile(q: String, params: (String, Any)*): InternalExecutionResult = fail("Don't use profile all together in ProfilerAcceptanceTest")

  def legacyProfile(q: String, params: (String, Any)*): InternalExecutionResult = profileWithPlanner(innerExecuteDeprecated, q, params.toMap)

  private def getArgument[A <: Argument](plan: InternalPlanDescription)(implicit manifest: ClassTag[A]): A = plan.arguments.collectFirst {
    case x: A => x
  }.getOrElse(fail(s"Failed to find plan description argument where expected. Wanted ${manifest.toString()} but only found ${plan.arguments}"))

  private def getPlanDescriptions(result: InternalExecutionResult, names: Seq[String]): Seq[InternalPlanDescription] = {
    result.toList
    val description = result.executionPlanDescription()
    if (names.isEmpty)
      description.flatten
    else {
      names.flatMap {
        name =>
          val descriptions: Seq[InternalPlanDescription] = description.find(name)
          withClue(s"$description\n$name is missing; ") {
            assert(descriptions.nonEmpty)
          }
          descriptions
      }
    }
  }
}
