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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.v3_3.logical.plans.{CartesianProduct, NodeIndexSeek}
import org.neo4j.cypher.{ExecutionEngineFunSuite, _}
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.graphdb.{Node, QueryExecutionException}


import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._
import org.neo4j.kernel.api.exceptions.Status

class UsingAcceptanceTest extends ExecutionEngineFunSuite with RunWithConfigTestSupport with CypherComparisonSupport {
  override def databaseConfig(): Map[Setting[_], String] = Map(GraphDatabaseSettings.cypher_hints_error -> "true")

  val expectedToSucceed = Configs.All
  val allPossibleConfigs = Configs.All + TestConfiguration(Versions.Default, Planners.Default,
    Runtimes(Runtimes.Default, Runtimes.ProcedureOrSchema, Runtimes.CompiledSource, Runtimes.CompiledBytecode))

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

    val result = executeWith(expectedToSucceed, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeAtLeastOne(classOf[NodeIndexSeek], withVariable = "f"),
        expectPlansToFail = Configs.AllRulePlanners))

    result.columnAs[Node]("f").toList should equal(List(node))
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

    val result = executeWith(Configs.Interpreted - Configs.Version2_3, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeAtLeastOne(classOf[NodeIndexSeek], withVariable = "f"),
        expectPlansToFail = Configs.AllRulePlanners))

    result.columnAs[Node]("f").toSet should equal(Set(nodes(123)))
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

    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeAtLeastOne(classOf[NodeIndexSeek], withVariable = "f"),
        expectPlansToFail = Configs.AllRulePlanners))

    result.columnAs[Node]("f").toList should equal(List(node))
  }

  test("fail if using index with start clause") {
    // GIVEN
    graph.createIndex("Person", "name")

    // WHEN & THEN
    failWithError(allPossibleConfigs, "start n=node(*) using index n:Person(name) where n:Person and n.name = 'kabam' return n", List("Invalid input"))
  }

  test("fail if using a variable with label not used in match") {
    // GIVEN
    graph.createIndex("Person", "name")

    // WHEN
    failWithError(allPossibleConfigs - Configs.Version2_3, "match n-->() using index n:Person(name) where n.name = 'kabam' return n",
      List("Unknown variable `n`.", "Parentheses are required to identify nodes in patterns, i.e. (n)"))
  }

  test("fail if using an hint for a non existing index") {
    // GIVEN: NO INDEX

    // WHEN
    failWithError(allPossibleConfigs, "match (n:Person)-->() using index n:Person(name) where n.name = 'kabam' return n", List("No such index"))
  }

  test("fail if using hints with unusable equality predicate") {
    // GIVEN
    graph.createIndex("Person", "name")

    // WHEN
    failWithError(allPossibleConfigs, "match (n:Person)-->() using index n:Person(name) where n.name <> 'kabam' return n", List("Cannot use index hint in this context"))
  }

  test("fail if joining index hints in equality predicates") {
    // GIVEN
    graph.createIndex("Person", "name")
    graph.createIndex("Food", "name")

    // WHEN
    failWithError(allPossibleConfigs,
      "match (n:Person)-->(m:Food) using index n:Person(name) using index m:Food(name) where n.name = m.name return n",
      List("Failed to fulfil the hints of the query.",
        "Unknown variable",
        "Cannot use index hint in this context",
        "The given query is not currently supported in the selected cost-based planner"))
  }

  test("scan hints are handled by ronja") {
    executeWith(expectedToSucceed, "match (n:Person) using scan n:Person return n").toList
  }

  test("fail when equality checks are done with OR") {
    // GIVEN
    graph.createIndex("Person", "name")

    // WHEN
    failWithError(allPossibleConfigs - Configs.Version2_3, "match n-->() using index n:Person(name) where n.name = 'kabam' OR n.name = 'kaboom' return n",
      List("Parentheses are required to identify nodes in patterns, i.e. (n)"))
  }

  test("correct status code when no index") {

    // GIVEN
    val query =
      """MATCH (n:Test)
        |USING INDEX n:Test(foo)
        |WHERE n.foo = {foo}
        |RETURN n""".stripMargin

    // WHEN
    failWithError(allPossibleConfigs, query, List("No such index"), params = "foo" -> 42)
  }

  test("should succeed (i.e. no warnings or errors) if executing a query using a 'USING INDEX' which can be fulfilled") {
    runWithConfig() {
      db =>
        db.execute("CREATE INDEX ON :Person(name)")
        db.execute("CALL db.awaitIndex(':Person(name)')")
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
    val result = executeWith(expectedToSucceed, "MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN ['Jacob'] RETURN n")

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
    val result = executeWith(expectedToSucceed, "MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN ['Jacob','Jacob'] RETURN n")

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
    val result = executeWith(expectedToSucceed, "MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN null RETURN n")

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
    val result = executeWith(expectedToSucceed, "MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN {coll} RETURN n",
      params = Map("coll" -> List("Jacob")))

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

    failWithError(allPossibleConfigs,
      "MATCH (n:Entity:Person) " +
        "USING INDEX n:Person(first_name) " +
        "USING INDEX n:Entity(source) " +
        "WHERE n.first_name = \"John\" AND n.source = \"form1\" " +
        "RETURN n;",
      List("Multiple hints for same variable are not supported", "Multiple hints for same identifier are not supported"))
  }

  test("does not accept multiple scan hints for the same variable") {
    failWithError(allPossibleConfigs,
      "MATCH (n:Entity:Person) " +
        "USING SCAN n:Person " +
        "USING SCAN n:Entity " +
        "WHERE n.first_name = \"John\" AND n.source = \"form1\" " +
        "RETURN n;",
      List("Multiple hints for same variable are not supported", "Multiple hints for same identifier are not supported"))

  }

  test("does not accept multiple mixed hints for the same variable") {
    failWithError(allPossibleConfigs,
      "MATCH (n:Entity:Person) " +
        "USING SCAN n:Person " +
        "USING INDEX n:Entity(first_name) " +
        "WHERE n.first_name = \"John\" AND n.source = \"form1\" " +
        "RETURN n;",
     List("Multiple hints for same variable are not supported", "Multiple hints for same identifier are not supported"))
  }

  test("scan hint must fail if using a variable not used in the query") {
    // GIVEN

    // WHEN
    failWithError(allPossibleConfigs, "MATCH (n:Person)-->() USING SCAN x:Person return n", List("Variable `x` not defined", "x not defined"))
  }

  test("scan hint must fail if using label not used in the query") {
    // GIVEN

    // WHEN
   failWithError(allPossibleConfigs, "MATCH n-->() USING SCAN n:Person return n",
     List("Cannot use label scan hint in this context.", "Parentheses are required to identify nodes in patterns, i.e. (n)"))
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
        engine.execute("CALL db.awaitIndex(':Person(name)')")
        shouldHaveNoWarnings(
          engine.execute(s"EXPLAIN MATCH (n:Person), (c:Company) USING INDEX n:Person(name) USING SCAN c:Company WHERE n.name = 'John' RETURN n")
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

  test("should handle join hint on the start node of a single hop pattern") {
    val initQuery = "CREATE (a:A {prop: 'foo'})-[:R]->(b:B {prop: 'bar'})"
    graph.execute(initQuery)

    val query =
      s"""
         |MATCH (a:A)-->(b:B)
         |USING JOIN ON a
         |RETURN a.prop AS res""".stripMargin

    val result = executeWith(Configs.Version3_3 - Configs.SlottedInterpreted + Configs.AllRulePlanners , query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeOnlyOneHashJoinOn("a"),
        expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal (List(Map("res" -> "foo")))
  }

  test("should handle join hint on the end node of a single hop pattern") {
    val initQuery = "CREATE (a:A {prop: 'foo'})-[:R]->(b:B {prop: 'bar'})"
    graph.execute(initQuery)

    val query =
      s"""
          |MATCH (a:A)-->(b:B)
          |USING JOIN ON b
          |RETURN b.prop AS res""".stripMargin

    val result = executeWith(Configs.Version3_3 - Configs.SlottedInterpreted + Configs.AllRulePlanners, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeOnlyOneHashJoinOn("b"),
        expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal (List(Map("res" -> "bar")))
  }

  test("should fail when join hint is applied to an undefined node") {
    failWithError(allPossibleConfigs,
      s"""
         |MATCH (a:A)-->(b:B)<--(c:C)
         |USING JOIN ON d
         |RETURN a.prop""".stripMargin,
      List("Variable `d` not defined", "d not defined"))
  }

  test("should fail when join hint is applied to a single node") {
    failWithError(allPossibleConfigs,
      s"""
         |MATCH (a:A)
         |USING JOIN ON a
         |RETURN a.prop""".stripMargin,
      List("Cannot use join hint for single node pattern"))
    }

  test("should fail when join hint is applied to a relationship") {
      failWithError(allPossibleConfigs,
        s"""
           |MATCH (a:A)-[r1]->(b:B)-[r2]->(c:C)
           |USING JOIN ON r1
           |RETURN a.prop""".stripMargin,
        List("Type mismatch: expected Node but was Relationship"))
    }

  test("should fail when join hint is applied to a path") {
      failWithError(allPossibleConfigs,
        s"""
           |MATCH p=(a:A)-->(b:B)-->(c:C)
           |USING JOIN ON p
           |RETURN a.prop""".stripMargin,
        List("Type mismatch: expected Node but was Path"))
    }

  test("should be able to use join hints for multiple hop pattern") {
      val a = createNode(("prop", "foo"))
      val b = createNode()
      val c = createNode()
      val d = createNode()
      val e = createNode(("prop", "foo"))

      relate(a, b, "X")
      relate(b, c, "X")
      relate(c, d, "X")
      relate(d, e, "X")

      val result = executeWith(Configs.AllExceptSlotted,
        s"""
           |MATCH (a)-[:X]->(b)-[:X]->(c)-[:X]->(d)-[:X]->(e)
           |USING JOIN ON c
           |WHERE a.prop = e.prop
           |RETURN c""".stripMargin,
        planComparisonStrategy = ComparePlansWithAssertion(_ should includeOnlyOneHashJoinOn("c"),
          expectPlansToFail = Configs.AllRulePlanners))

      result.toList should equal(List(Map("c" -> c)))
    }

  test("should be able to use join hints for queries with var length pattern") {
      val a = createLabeledNode(Map("prop" -> "foo"), "Foo")
      val b = createNode()
      val c = createNode()
      val d = createNode()
      val e = createLabeledNode(Map("prop" -> "foo"), "Bar")

      relate(a, b, "X")
      relate(b, c, "X")
      relate(c, d, "X")
      relate(e, d, "Y")

      val result = executeWith(Configs.CommunityInterpreted,
        s"""
           |MATCH (a:Foo)-[:X*]->(b)<-[:Y]->(c:Bar)
           |USING JOIN ON b
           |WHERE a.prop = c.prop
           |RETURN c""".stripMargin,
        planComparisonStrategy = ComparePlansWithAssertion(_ should includeOnlyOneHashJoinOn("b"),
          expectPlansToFail = Configs.AllRulePlanners))

      result.toList should equal(List(Map("c" -> e)))
    }

  test("should be able to use multiple join hints") {
      val a = createNode(("prop", "foo"))
      val b = createNode()
      val c = createNode()
      val d = createNode()
      val e = createNode(("prop", "foo"))

      relate(a, b, "X")
      relate(b, c, "X")
      relate(c, d, "X")
      relate(d, e, "X")

    executeWith(Configs.AllExceptSlotted,
        s"""
           |MATCH (a)-[:X]->(b)-[:X]->(c)-[:X]->(d)-[:X]->(e)
           |USING JOIN ON b
           |USING JOIN ON c
           |USING JOIN ON d
           |WHERE a.prop = e.prop
           |RETURN b, d""".stripMargin,
        planComparisonStrategy = ComparePlansWithAssertion(planDescription => {
          planDescription should includeOnlyOneHashJoinOn("b")
          planDescription should includeOnlyOneHashJoinOn("c")
          planDescription should includeOnlyOneHashJoinOn("d")
        }, expectPlansToFail = Configs.AllRulePlanners))
    }

  test("should work when join hint is applied to x in (a)-->(x)<--(b)") {
      val a = createNode()
      val b = createNode()
      val x = createNode()

      relate(a, x)
      relate(b, x)

      val query =
        s"""
            |MATCH (a)-->(x)<--(b)
            |USING JOIN ON x
            |RETURN x""".stripMargin

      executeWith(Configs.AllExceptSlotted, query,
        planComparisonStrategy = ComparePlansWithAssertion(_ should includeOnlyOneHashJoinOn("x"),
          expectPlansToFail = Configs.AllRulePlanners))
    }

  test("should work when join hint is applied to x in (a)-->(x)<--(b) where a and b can use an index") {
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
        s"""
            |MATCH (a:Person {name:"Tom Hanks"})-[:ACTS_IN]->(x)<-[:ACTS_IN]-(b:Person {name:"Meg Ryan"})
            |USING JOIN ON x
            |RETURN x""".stripMargin

      executeWith(Configs.AllExceptSlotted, query)
    }

  test("should work when join hint is applied to x in (a)-->(x)<--(b) where a and b are labeled") {
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
        s"""
            |MATCH (a:Person {name:"Tom Hanks"})-[:ACTS_IN]->(x)<-[:ACTS_IN]-(b:Person {name:"Meg Ryan"})
            |USING JOIN ON x
            |RETURN x""".stripMargin

      executeWith(Configs.AllExceptSlotted, query,
        planComparisonStrategy = ComparePlansWithAssertion(planDescription => {
          planDescription should includeOnlyOneHashJoinOn("x")
          planDescription.toString should not include "AllNodesScan"
        }, expectPlansToFail = Configs.AllRulePlanners))
    }

  test("should work when join hint is applied to x in (a)-->(x)<--(b) where using index hints on a and b") {
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
        s"""
            |MATCH (a:Person {name:"Tom Hanks"})-[:ACTS_IN]->(x)<-[:ACTS_IN]-(b:Person {name:"Meg Ryan"})
            |USING INDEX a:Person(name)
            |USING INDEX b:Person(name)
            |USING JOIN ON x
            |RETURN x""".stripMargin

      executeWith(Configs.AllExceptSlotted, query,
        planComparisonStrategy = ComparePlansWithAssertion(planDescription => {
          planDescription should includeOnlyOneHashJoinOn("x")
          planDescription.toString should not include "AllNodesScan"
        }, expectPlansToFail = Configs.AllRulePlanners))
    }

  test("should work when join hint is applied to x in (a)-->(x)<--(b) where x can use an index") {
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
        s"""
            |MATCH (a:Person)-[:ACTS_IN]->(x:Movie {title: "When Harry Met Sally"})<-[:ACTS_IN]-(b:Person)
            |USING JOIN ON x
            |RETURN x""".stripMargin

      executeWith(Configs.AllExceptSlotted, query,
        planComparisonStrategy = ComparePlansWithAssertion(planDescription => {
          planDescription should includeOnlyOneHashJoinOn("x")
          planDescription should includeAtLeastOne(classOf[NodeIndexSeek], withVariable = "x")
        }, expectPlansToFail = Configs.AllRulePlanners))
  }

  test("should handle using index hint on both ends of pattern") {
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
      """MATCH (a:Person {name:"Tom Hanks"})-[:ACTS_IN]->(x)<-[:ACTS_IN]-(b:Person {name:"Meg Ryan"})
        |USING INDEX a:Person(name)
        |USING INDEX b:Person(name)
        |RETURN x""".stripMargin

    executeWith(Configs.AllExceptSlotted, query,
      planComparisonStrategy = ComparePlansWithAssertion(planDescription => {
        planDescription should includeOnlyOneHashJoinOn("x")
        planDescription.toString should not include "AllNodesScan"
      }, expectPlansToFail = Configs.AllRulePlanners))
  }

  test("Using index hints with two indexes should produce cartesian product"){
    val startNode = createLabeledNode(Map("name" -> "Neo"), "Person")
    val endNode = createLabeledNode(Map("name" -> "Trinity"), "Person")
    relate(startNode, endNode, "knows")
    graph.createIndex("Person","name")

    val query =
      """MATCH (k:Person {name: 'Neo'}), (t:Person {name: 'Trinity'})
        |using index k:Person(name)
        |using index t:Person(name)
        |MATCH p=(k)-[:knows*0..5]-(t)
        |RETURN count(p)
        |""".stripMargin

    executeWith(Configs.CommunityInterpreted - Configs.Cost3_2 - Configs.Cost3_1 - Configs.Cost2_3, query,
      planComparisonStrategy = ComparePlansWithAssertion(planDescription => {
        planDescription should includeAtLeastOne(classOf[NodeIndexSeek], withVariable = "k")
        planDescription should includeAtLeastOne(classOf[NodeIndexSeek], withVariable = "t")
        planDescription should includeOnlyOne(classOf[CartesianProduct])
        planDescription.toString should not include "AllNodesScan"
      }, expectPlansToFail = Configs.AllRulePlanners))
  }

  test("USING INDEX hint should not clash with used variables") {
    graph.createIndex("PERSON", "id")

    val result = executeWith(expectedToSucceed,
      """MATCH (actor:PERSON {id: 1})
        |USING INDEX actor:PERSON(id)
        |WITH 14 as id
        |RETURN 13 as id""".stripMargin)

    result.toList should be(empty)
  }

  test("should accept two hints on a single relationship") {
    val startNode = createLabeledNode(Map("prop" -> 1), "PERSON")
    val endNode = createLabeledNode(Map("prop" -> 2), "PERSON")
    relate(startNode, endNode)
    graph.createIndex("PERSON", "prop")

    val query =
      """MATCH (a:PERSON {prop: 1})-->(b:PERSON {prop: 2})
        |USING INDEX a:PERSON(prop)
        |USING INDEX b:PERSON(prop)
        |RETURN a, b""".stripMargin
    val result = executeWith(Configs.All - Configs.Cost3_2 - Configs.Cost3_1 - Configs.Cost2_3, query,
      planComparisonStrategy = ComparePlansWithAssertion(planDescription => {
        planDescription should includeAtLeastOne(classOf[NodeIndexSeek], withVariable = "a")
        planDescription should includeAtLeastOne(classOf[NodeIndexSeek], withVariable = "b")
    }, expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(Map("a" -> startNode, "b" -> endNode)))
  }

  test("should handle multiple hints on longer pattern") {
    val initQuery = "CREATE (a:Node {prop: 'foo'})-[:R]->(b:Node {prop: 'bar'})-[:R]->(c:Node {prop: 'baz'})"
    graph.execute(initQuery)
    graph.createIndex("Node", "prop")

    val query =
      s"""MATCH (a:Node)-->(b:Node)-->(c:Node)
          |USING INDEX a:Node(prop)
          |USING INDEX b:Node(prop)
          |WHERE a.prop = 'foo' and b.prop = 'bar'
          |RETURN b.prop AS res""".stripMargin

    val result = executeWith(Configs.All - Configs.Cost3_2 - Configs.Cost3_1 - Configs.Cost2_3, query,
      planComparisonStrategy = ComparePlansWithAssertion(planDescription => {
        planDescription should includeAtLeastOne(classOf[NodeIndexSeek], withVariable = "a")
        planDescription should includeAtLeastOne(classOf[NodeIndexSeek], withVariable = "b")
      }, expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal (List(Map("res" -> "bar")))
  }

  test("should accept hint on composite index") {
    val node = createLabeledNode(Map("bar" -> 5, "baz" -> 3), "Foo")
    graph.createIndex("Foo", "bar", "baz")

    val query =
      """ MATCH (f:Foo)
        | USING INDEX f:Foo(bar,baz)
        | WHERE f.bar=5 and f.baz=3
        | RETURN f
      """.stripMargin
    val result = executeWith(Configs.Version3_3 + Configs.Cost3_2 - Configs.Compiled - Configs.AllRulePlanners, query,
      planComparisonStrategy = ComparePlansWithAssertion(planDescription => {
        planDescription should includeAtLeastOne(classOf[NodeIndexSeek], withVariable = "f")
      }, expectPlansToFail = Configs.AllRulePlanners))

    result.columnAs[Node]("f").toList should equal(List(node))
  }
}
