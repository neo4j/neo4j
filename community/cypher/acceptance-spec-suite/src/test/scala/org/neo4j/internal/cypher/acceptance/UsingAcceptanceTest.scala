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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.compiler.v3_1.IDPPlannerName
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.InternalPlanDescription.Arguments.KeyNames
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.plans.{NodeHashJoin, NodeIndexSeek}
import org.neo4j.cypher.{ExecutionEngineFunSuite, IndexHintException, NewPlannerTestSupport, SyntaxException, _}
import org.neo4j.graphdb.{QueryExecutionException,Node}
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.kernel.api.exceptions.Status

class UsingAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport with RunWithConfigTestSupport {

  test("should use index on literal value") {
    val node = createLabeledNode(Map("id" -> 123), "Foo")
    graph.createIndex("Foo", "id")
    val query =
      """
        |PROFILE
        | MATCH (f:Foo)
        | USING INDEX f:Foo(id)
        | WHERE f.id=123
        | RETURN f
      """.stripMargin
    val result = executeWithCostPlannerOnly(query)
    result.columnAs[Node]("f").toList should equal(List(node))
    result.executionPlanDescription() should includeAtLeastOne(classOf[NodeIndexSeek], withVariable = "f")
  }


  test("should use index on literal map expression") {
    val nodes = Range(0,125).map(i => createLabeledNode(Map("id" -> i), "Foo"))
    graph.createIndex("Foo", "id")
    val query =
      """
        |PROFILE
        | MATCH (f:Foo)
        | USING INDEX f:Foo(id)
        | WHERE f.id={id: 123}.id
        | RETURN f
      """.stripMargin
    val result = executeWithCostPlannerOnly(query)
    result.columnAs[Node]("f").toSet should equal(Set(nodes(123)))
    result.executionPlanDescription() should includeAtLeastOne(classOf[NodeIndexSeek], withVariable = "f")
  }

  test("should use index on variable defined from literal value") {
    val node = createLabeledNode(Map("id" -> 123), "Foo")
    graph.createIndex("Foo", "id")
    val query =
      """
        |PROFILE
        | WITH 123 AS row
        | MATCH (f:Foo)
        | USING INDEX f:Foo(id)
        | WHERE f.id=row
        | RETURN f
      """.stripMargin
    val result = executeWithCostPlannerOnly(query)
    result.columnAs[Node]("f").toList should equal(List(node))
    result.executionPlanDescription() should includeAtLeastOne(classOf[NodeIndexSeek], withVariable = "f")
  }

  test("fail if using index with start clause") {
    // GIVEN
    graph.createIndex("Person", "name")

    // WHEN & THEN
    intercept[SyntaxException](
      executeWithAllPlannersAndCompatibilityMode("start n=node(*) using index n:Person(name) where n:Person and n.name = 'kabam' return n"))
  }

  test("fail if using a variable with label not used in match") {
    // GIVEN
    graph.createIndex("Person", "name")

    // WHEN
    intercept[SyntaxException](
      executeWithAllPlannersAndCompatibilityMode("match n-->() using index n:Person(name) where n.name = 'kabam' return n"))
  }

  test("fail if using an hint for a non existing index") {
    // GIVEN: NO INDEX

    // WHEN
    intercept[IndexHintException](
      executeWithAllPlannersAndCompatibilityMode("match (n:Person)-->() using index n:Person(name) where n.name = 'kabam' return n"))
  }

  test("fail if using hints with unusable equality predicate") {
    // GIVEN
    graph.createIndex("Person", "name")

    // WHEN
    intercept[SyntaxException](
      executeWithAllPlannersAndCompatibilityMode("match (n:Person)-->() using index n:Person(name) where n.name <> 'kabam' return n"))
  }

  test("fail if joining index hints in equality predicates") {
    // GIVEN
    graph.createIndex("Person", "name")
    graph.createIndex("Food", "name")

    // WHEN
    intercept[SyntaxException](
      executeWithAllPlannersAndCompatibilityMode("match (n:Person)-->(m:Food) using index n:Person(name) using index m:Food(name) where n.name = m.name return n"))
  }

  test("scan hints are handled by ronja") {
    executeWithAllPlannersAndRuntimesAndCompatibilityMode("match (n:Person) using scan n:Person return n").toList
  }

  test("fail when equality checks are done with OR") {
    // GIVEN
    graph.createIndex("Person", "name")

    // WHEN
    intercept[SyntaxException](
      executeWithAllPlannersAndCompatibilityMode("match n-->() using index n:Person(name) where n.name = 'kabam' OR n.name = 'kaboom' return n"))
  }

  test("correct status code when no index") {

    // GIVEN
    val query =
      """MATCH (n:Test)
        |USING INDEX n:Test(foo)
        |WHERE n.foo = {foo}
        |RETURN n""".stripMargin

    // WHEN
    val error = intercept[IndexHintException](executeWithAllPlannersAndCompatibilityMode(query))

    // THEN
    error.status should equal(Status.Schema.IndexNotFound)
  }

  test("should succeed (i.e. no warnings or errors) if executing a query using a 'USING INDEX' which can be fulfilled") {
    runWithConfig() {
      db =>
        db.execute("CREATE INDEX ON :Person(name)")
        shouldHaveNoWarnings(
          db.execute(s"EXPLAIN MATCH (n:Person) USING INDEX n:Person(name) WHERE n.name = 'John' RETURN n")
        )
    }
  }

  test("should generate a warning if executing a query using a 'USING INDEX' which cannot be fulfilled") {
    runWithConfig() {
      db =>
        shouldHaveWarning(db.execute(s"EXPLAIN MATCH (n:Person) USING INDEX n:Person(name) WHERE n.name = 'John' RETURN n"), Status.Schema.IndexNotFound)
    }
  }

  test("should generate a warning if executing a query using a 'USING INDEX' which cannot be fulfilled, and hint errors are turned off") {
    runWithConfig(GraphDatabaseSettings.cypher_hints_error -> "false") {
      db =>
        shouldHaveWarning(db.execute(s"EXPLAIN MATCH (n:Person) USING INDEX n:Person(name) WHERE n.name = 'John' RETURN n"), Status.Schema.IndexNotFound)
    }
  }

  test("should generate an error if executing a query using EXPLAIN and a 'USING INDEX' which cannot be fulfilled, and hint errors are turned on") {
    runWithConfig(GraphDatabaseSettings.cypher_hints_error -> "true") {
      db =>
        intercept[QueryExecutionException](
          db.execute(s"EXPLAIN MATCH (n:Person) USING INDEX n:Person(name) WHERE n.name = 'John' RETURN n")
        ).getStatusCode should equal("Neo.ClientError.Schema.IndexNotFound")
    }
  }

  test("should generate an error if executing a query using a 'USING INDEX' which cannot be fulfilled, and hint errors are turned on") {
    runWithConfig(GraphDatabaseSettings.cypher_hints_error -> "true") {
      db =>
        intercept[QueryExecutionException](
          db.execute(s"MATCH (n:Person) USING INDEX n:Person(name) WHERE n.name = 'John' RETURN n")
        ).getStatusCode should equal("Neo.ClientError.Schema.IndexNotFound")
    }
  }

  test("should generate an error if executing a query using a 'USING INDEX' for an existing index but which cannot be fulfilled for the query, and hint errors are turned on") {
    runWithConfig(GraphDatabaseSettings.cypher_hints_error -> "true") {
      db =>
        db.execute("CREATE INDEX ON :Person(email)")
        intercept[QueryExecutionException](
          db.execute(s"MATCH (n:Person) USING INDEX n:Person(email) WHERE n.name = 'John' RETURN n")
        ).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
    }
  }

  test("should generate an error if executing a query using a 'USING INDEX' for an existing index but which cannot be fulfilled for the query, even when hint errors are not turned on") {
    runWithConfig() {
      db =>
        db.execute("CREATE INDEX ON :Person(email)")
        intercept[QueryExecutionException](
          db.execute(s"MATCH (n:Person) USING INDEX n:Person(email) WHERE n.name = 'John' RETURN n")
        ).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
    }
  }

  test("should be able to use index hints on IN expressions") {
    //GIVEN
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    graph.createIndex("Person", "name")

    //WHEN
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN ['Jacob'] RETURN n")

    //THEN
    result.toList should equal(List(Map("n" -> jake)))
  }

  test("should be able to use index hints on IN collections with duplicates") {
    //GIVEN
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    graph.createIndex("Person", "name")

    //WHEN
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN ['Jacob','Jacob'] RETURN n")

    //THEN
    result.toList should equal(List(Map("n" -> jake)))
  }

  test("should be able to use index hints on IN a null value") {
    //GIVEN
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    graph.createIndex("Person", "name")

    //WHEN
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN null RETURN n")

    //THEN
    result.toList should equal(List())
  }

  test("should be able to use index hints on IN a collection parameter") {
    //GIVEN
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    graph.createIndex("Person", "name")

    //WHEN
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN {coll} RETURN n", "coll" -> List("Jacob"))

    //THEN
    result.toList should equal(List(Map("n" -> jake)))
  }

  test("does not accept multiple index hints for the same variable") {
    // GIVEN
    graph.createIndex("Entity", "source")
    graph.createIndex("Person", "first_name")
    createNode("source" -> "form1")
    createNode("first_name" -> "John")

    // WHEN THEN
    val e = intercept[SyntaxException] {
      executeWithAllPlanners(
        "MATCH (n:Entity:Person) " +
          "USING INDEX n:Person(first_name) " +
          "USING INDEX n:Entity(source) " +
          "WHERE n.first_name = \"John\" AND n.source = \"form1\" " +
          "RETURN n;"
      )
    }

    e.getMessage should startWith("Multiple hints for same variable are not supported")
  }

  test("does not accept multiple scan hints for the same variable") {
    val e = intercept[SyntaxException] {
      executeWithAllPlanners(
        "MATCH (n:Entity:Person) " +
          "USING SCAN n:Person " +
          "USING SCAN n:Entity " +
          "WHERE n.first_name = \"John\" AND n.source = \"form1\" " +
          "RETURN n;"
      )
    }

    e.getMessage should startWith("Multiple hints for same variable are not supported")
  }

  test("does not accept multiple mixed hints for the same variable") {
    val e = intercept[SyntaxException] {
      executeWithAllPlanners(
        "MATCH (n:Entity:Person) " +
          "USING SCAN n:Person " +
          "USING INDEX n:Entity(first_name) " +
          "WHERE n.first_name = \"John\" AND n.source = \"form1\" " +
          "RETURN n;"
      )
    }

    e.getMessage should startWith("Multiple hints for same variable are not supported")
  }

  test("scan hint must fail if using a variable not used in the query") {
    // GIVEN

    // WHEN
    intercept[SyntaxException](
      executeWithAllPlannersAndCompatibilityMode("MATCH (n:Person)-->() USING SCAN x:Person return n"))
  }

  test("scan hint must fail if using label not used in the query") {
    // GIVEN

    // WHEN
    intercept[SyntaxException](
      executeWithAllPlannersAndCompatibilityMode("MATCH n-->() USING SCAN n:Person return n"))
  }

  test("should succeed (i.e. no warnings or errors) if executing a query using a 'USING SCAN'") {
    runWithConfig() {
      engine =>
        shouldHaveNoWarnings(
          engine.execute(s"EXPLAIN MATCH (n:Person) USING SCAN n:Person WHERE n.name = 'John' RETURN n")
        )
    }
  }

  test("should succeed if executing a query using both 'USING SCAN' and 'USING INDEX' if index exists") {
    runWithConfig() {
      engine =>
        engine.execute("CREATE INDEX ON :Person(name)")
        shouldHaveNoWarnings(
          engine.execute(s"EXPLAIN MATCH (n:Person)-[:WORKS_FOR]->(c:Company) USING INDEX n:Person(name) USING SCAN c:Company WHERE n.name = 'John' RETURN n")
        )
    }
  }

  test("should fail outright if executing a query using a 'USING SCAN' and 'USING INDEX' on the same variable, even if index exists") {
    runWithConfig() {
      engine =>
        engine.execute("CREATE INDEX ON :Person(name)")
        intercept[QueryExecutionException](
          engine.execute(s"EXPLAIN MATCH (n:Person) USING INDEX n:Person(name) USING SCAN n:Person WHERE n.name = 'John' RETURN n")
        ).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
    }
  }

  test("should notify unfulfillable when join hint is applied to the start node of a single hop pattern") {
    val initQuery = "CREATE (a:A {prop: 'foo'})-[:R]->(b:B {prop: 'bar'})"

    val query =
      s"""MATCH (a:A)-->(b:B)
          |USING JOIN ON a
          |RETURN a.prop AS res""".stripMargin

    // Should give either warning or error depending on configuration
    verifyJoinHintUnfulfillableOnRunWithConfig(initQuery, query, expectedResult = List(Map("res" -> "foo")))
  }

  test("should notify unfulfillable when join hint is applied to the end node of a single hop pattern") {
    val initQuery = "CREATE (a:A {prop: 'foo'})-[:R]->(b:B {prop: 'bar'})"

    val query =
      s"""MATCH (a:A)-->(b:B)
          |USING JOIN ON b
          |RETURN b.prop AS res""".stripMargin

    // Should give either warning or error depending on configuration
    verifyJoinHintUnfulfillableOnRunWithConfig(initQuery, query, expectedResult = List(Map("res" -> "bar")))
  }

  val plannersThatSupportJoinHints = Seq(IDPPlannerName)

  plannersThatSupportJoinHints.foreach { planner =>

    val plannerName = planner.name.toLowerCase

    test(s"$plannerName should fail when join hint is applied to an undefined node") {
      val error = intercept[SyntaxException](
        executeWithCostPlannerOnly(
          s"""
             |CYPHER planner=$plannerName
             |MATCH (a:A)-->(b:B)<--(c:C)
             |USING JOIN ON d
             |RETURN a.prop
          """.stripMargin))

      error.getMessage should include("Variable `d` not defined")
    }

    test(s"$plannerName should fail when join hint is applied to a single node") {
      val error = intercept[SyntaxException](
        executeWithCostPlannerOnly(
          s"""
             |CYPHER planner=$plannerName
             |MATCH (a:A)
             |USING JOIN ON a
             |RETURN a.prop
          """.stripMargin))

      error.getMessage should include("Cannot use join hint for single node pattern")
    }

    test(s"$plannerName should fail when join hint is applied to a relationship") {
      val error = intercept[SyntaxException](
        executeWithCostPlannerOnly(
          s"""
             |CYPHER planner=$plannerName
             |MATCH (a:A)-[r1]->(b:B)-[r2]->(c:C)
             |USING JOIN ON r1
             |RETURN a.prop
          """.stripMargin))

      error.getMessage should include("Type mismatch: expected Node but was Relationship")
    }

    test(s"$plannerName should fail when join hint is applied to a path") {
      val error = intercept[SyntaxException](
        executeWithCostPlannerOnly(
          s"""
             |CYPHER planner=$plannerName
             |MATCH p=(a:A)-->(b:B)-->(c:C)
             |USING JOIN ON p
             |RETURN a.prop
          """.stripMargin))

      error.getMessage should include("Type mismatch: expected Node but was Path")
    }

    test(s"$plannerName should be able to use join hints for multiple hop pattern") {
      val a = createNode(("prop", "foo"))
      val b = createNode()
      val c = createNode()
      val d = createNode()
      val e = createNode(("prop", "foo"))

      relate(a, b, "X")
      relate(b, c, "X")
      relate(c, d, "X")
      relate(d, e, "X")

      val result = executeWithCostPlannerOnly(
        s"""
           |CYPHER planner=$plannerName
           |MATCH (a)-[:X]->(b)-[:X]->(c)-[:X]->(d)-[:X]->(e)
           |USING JOIN ON c
           |WHERE a.prop = e.prop
           |RETURN c""".stripMargin)

      result.toList should equal(List(Map("c" -> c)))
      result.executionPlanDescription() should includeOnlyOneHashJoinOn("c")
    }

    test(s"$plannerName should be able to use join hints for queries with var length pattern") {
      val a = createLabeledNode(Map("prop" -> "foo"), "Foo")
      val b = createNode()
      val c = createNode()
      val d = createNode()
      val e = createLabeledNode(Map("prop" -> "foo"), "Bar")

      relate(a, b, "X")
      relate(b, c, "X")
      relate(c, d, "X")
      relate(e, d, "Y")

      val result = executeWithCostPlannerOnly(
        s"""
           |CYPHER planner=$plannerName
           |MATCH (a:Foo)-[:X*]->(b)<-[:Y]->(c:Bar)
           |USING JOIN ON b
           |WHERE a.prop = c.prop
           |RETURN c""".stripMargin)

      result.toList should equal(List(Map("c" -> e)))
      result.executionPlanDescription() should includeOnlyOneHashJoinOn("b")
    }

    test(s"$plannerName should be able to use multiple join hints") {
      val a = createNode(("prop", "foo"))
      val b = createNode()
      val c = createNode()
      val d = createNode()
      val e = createNode(("prop", "foo"))

      relate(a, b, "X")
      relate(b, c, "X")
      relate(c, d, "X")
      relate(d, e, "X")

      val result = executeWithCostPlannerOnly(
        s"""
           |CYPHER planner=$plannerName
           |MATCH (a)-[:X]->(b)-[:X]->(c)-[:X]->(d)-[:X]->(e)
           |USING JOIN ON b
           |USING JOIN ON c
           |USING JOIN ON d
           |WHERE a.prop = e.prop
           |RETURN b, d""".stripMargin)

      result.toList should equal(List(Map("b" -> b, "d" -> d)))
      result.executionPlanDescription() should includeOnlyOneHashJoinOn("b")
      result.executionPlanDescription() should includeOnlyOneHashJoinOn("c")
      result.executionPlanDescription() should includeOnlyOneHashJoinOn("d")
    }

    test(s"$plannerName should work when join hint is applied to x in (a)-->(x)<--(b)") {
      val a = createNode()
      val b = createNode()
      val x = createNode()

      relate(a, x)
      relate(b, x)

      val query =
        s"""CYPHER planner=$plannerName
            |MATCH (a)-->(x)<--(b)
            |USING JOIN ON x
            |RETURN x""".stripMargin

      val result = executeWithCostPlannerOnly(query)

      result.executionPlanDescription() should includeOnlyOneHashJoinOn("x")
    }

    test(s"$plannerName should work when join hint is applied to x in (a)-->(x)<--(b) where a and b can use an index") {
      graph.createIndex("Person", "name")

      val tom = createLabeledNode(Map("name" -> "Tom Hanks"), "Person")
      val meg = createLabeledNode(Map("name" -> "Meg Ryan"), "Person")

      val harrysally = createLabeledNode(Map("title" -> "When Harry Met Sally"), "Movie")

      relate(tom, harrysally, "ACTS_IN")
      relate(meg, harrysally, "ACTS_IN")

      1 until 10 foreach { i =>
        createLabeledNode(Map("name" -> s"Person $i"), "Person")
      }

      1 until 90 foreach { i =>
        createLabeledNode("Person")
      }

      1 until 20 foreach { i =>
        createLabeledNode("Movie")
      }

      val query =
        s"""CYPHER planner=$plannerName
            |MATCH (a:Person {name:"Tom Hanks"})-[:ACTS_IN]->(x)<-[:ACTS_IN]-(b:Person {name:"Meg Ryan"})
            |USING JOIN ON x
            |RETURN x""".stripMargin

      val result = executeWithCostPlannerOnly(query)
    }

    test(s"$plannerName should work when join hint is applied to x in (a)-->(x)<--(b) where a and b are labeled") {
      val tom = createLabeledNode(Map("name" -> "Tom Hanks"), "Person")
      val meg = createLabeledNode(Map("name" -> "Meg Ryan"), "Person")

      val harrysally = createLabeledNode(Map("title" -> "When Harry Met Sally"), "Movie")

      relate(tom, harrysally, "ACTS_IN")
      relate(meg, harrysally, "ACTS_IN")

      1 until 10 foreach { i =>
        createLabeledNode(Map("name" -> s"Person $i"), "Person")
      }

      1 until 90 foreach { i =>
        createLabeledNode("Person")
      }

      1 until 20 foreach { i =>
        createLabeledNode("Movie")
      }

      val query =
        s"""CYPHER planner=$plannerName
            |MATCH (a:Person {name:"Tom Hanks"})-[:ACTS_IN]->(x)<-[:ACTS_IN]-(b:Person {name:"Meg Ryan"})
            |USING JOIN ON x
            |RETURN x""".stripMargin

      val result = executeWithCostPlannerOnly(query)

      result.executionPlanDescription() should includeOnlyOneHashJoinOn("x")
      result.executionPlanDescription().toString should not include "AllNodesScan"
    }

    test(s"$plannerName should work when join hint is applied to x in (a)-->(x)<--(b) where using index hints on a and b") {
      graph.createIndex("Person", "name")

      val tom = createLabeledNode(Map("name" -> "Tom Hanks"), "Person")
      val meg = createLabeledNode(Map("name" -> "Meg Ryan"), "Person")

      val harrysally = createLabeledNode(Map("title" -> "When Harry Met Sally"), "Movie")

      relate(tom, harrysally, "ACTS_IN")
      relate(meg, harrysally, "ACTS_IN")

      1 until 10 foreach { i =>
        createLabeledNode(Map("name" -> s"Person $i"), "Person")
      }

      1 until 90 foreach { i =>
        createLabeledNode("Person")
      }

      1 until 20 foreach { i =>
        createLabeledNode("Movie")
      }

      val query =
        s"""CYPHER planner=$plannerName
            |MATCH (a:Person {name:"Tom Hanks"})-[:ACTS_IN]->(x)<-[:ACTS_IN]-(b:Person {name:"Meg Ryan"})
            |USING INDEX a:Person(name)
            |USING INDEX b:Person(name)
            |USING JOIN ON x
            |RETURN x""".stripMargin

      val result = executeWithCostPlannerOnly(query)

      result.executionPlanDescription() should includeOnlyOneHashJoinOn("x")
      result.executionPlanDescription().toString should not include "AllNodesScan"
    }

    test(s"$plannerName should work when join hint is applied to x in (a)-->(x)<--(b) where x can use an index") {
      graph.createIndex("Movie", "title")

      val tom = createLabeledNode(Map("name" -> "Tom Hanks"), "Person")
      val meg = createLabeledNode(Map("name" -> "Meg Ryan"), "Person")

      val harrysally = createLabeledNode(Map("title" -> "When Harry Met Sally"), "Movie")

      relate(tom, harrysally, "ACTS_IN")
      relate(meg, harrysally, "ACTS_IN")

      1 until 10 foreach { i =>
        createLabeledNode(Map("name" -> s"Person $i"), "Person")
      }

      1 until 90 foreach { i =>
        createLabeledNode("Person")
      }

      1 until 20 foreach { i =>
        createLabeledNode(Map("title" -> s"Movie $i"), "Movie")
      }

      val query =
        s"""CYPHER planner=$plannerName
            |MATCH (a:Person)-[:ACTS_IN]->(x:Movie {title: "When Harry Met Sally"})<-[:ACTS_IN]-(b:Person)
            |USING JOIN ON x
            |RETURN x""".stripMargin

      val result = executeWithCostPlannerOnly(query)

      result.executionPlanDescription() should includeOnlyOneHashJoinOn("x")
      result.executionPlanDescription() should includeAtLeastOne(classOf[NodeIndexSeek], withVariable = "x")
    }
  }

  test("rule planner should ignore join hint") {
    val a = createNode()
    val m = createNode()
    val z = createNode()
    relate(a, m)
    relate(z, m)

    val result = innerExecute(
      """
        |CYPHER planner=rule
        |MATCH (a)-->(m)<--(z)
        |USING JOIN ON m
        |RETURN DISTINCT m""".stripMargin).toList

    result should equal(List(Map("m" -> m)))
  }

  test("USING INDEX hint should not clash with used variables") {
    graph.createIndex("PERSON", "id")

    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode(
      """MATCH (actor:PERSON {id: 1})
        |USING INDEX actor:PERSON(id)
        |WITH 14 as id
        |RETURN 13 as id""".stripMargin)

    result.toList should be(empty)
  }

  //---------------------------------------------------------------------------
  // Verification helpers

  private def verifyJoinHintUnfulfillableOnRunWithConfig(initQuery: String, query: String, expectedResult: Any): Unit = {
    runWithConfig(GraphDatabaseSettings.cypher_hints_error -> "false") {
      db =>
        db.execute(initQuery)
        val result = db.execute(query)
        shouldHaveNoWarnings(result)
        import scala.collection.JavaConverters._
        result.asScala.toList.map(_.asScala) should equal(expectedResult)

        val explainResult = db.execute(s"EXPLAIN $query")
        shouldHaveWarning(explainResult, Status.Statement.JoinHintUnfulfillableWarning)
    }

    runWithConfig(GraphDatabaseSettings.cypher_hints_error -> "true") {
      db =>
        db.execute(initQuery)
        intercept[QueryExecutionException](db.execute(query)).getStatusCode should equal("Neo.DatabaseError.Statement.ExecutionFailed")
        intercept[QueryExecutionException](db.execute(s"EXPLAIN $query")).getStatusCode should equal("Neo.DatabaseError.Statement.ExecutionFailed")
    }
  }
}
