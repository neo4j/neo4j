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
package org.neo4j.internal.cypher.acceptance

import java.time.{LocalDate, LocalTime, OffsetTime, ZoneOffset}

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.graphdb.Node
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.values.storable.{CoordinateReferenceSystem, DurationValue, Values}
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.JavaConverters._

/**
  * These tests are testing the actual index implementation, thus they should all check the actual result.
  * If you only want to verify that plans using indexes are actually planned, please use
  * [[org.neo4j.cypher.internal.compiler.v3_4.planner.logical.LeafPlanningIntegrationTest]]
  */
class CompositeIndexAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should succeed in creating and deleting composite index") {
    // When
    graph.createIndex("Person", "firstname")
    graph.createIndex("Person", "firstname", "lastname")

    // Then
    graph should haveIndexes(":Person(firstname)")
    graph should haveIndexes(":Person(firstname)", ":Person(firstname,lastname)")

    // When
    executeWith(Configs.Procs, "DROP INDEX ON :Person(firstname , lastname)")

    // Then
    graph should haveIndexes(":Person(firstname)")
    graph should not(haveIndexes(":Person(firstname,lastname)"))
  }

  test("should use composite index when all predicates are present") {
    // Given
    graph.createIndex("User", "firstname", "lastname")
    val n1 = createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Smoke"), "User")
    createLabeledNode(Map("firstname" -> "Jake", "lastname" -> "Soap"), "User")

    // When
    val result = executeWith(Configs.Interpreted, "MATCH (n:User) WHERE n.lastname = 'Soap' AND n.firstname = 'Joe' RETURN n",
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators("NodeIndexSeek")
      }, Configs.OldAndRule))

    // Then
    result.toComparableResult should equal(List(Map("n" -> n1)))
  }

  test("should not use composite index when not all predicates are present") {
    // Given
    graph.createIndex("User", "firstname", "lastname")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Smoke"), "User")
    val n3 = createLabeledNode(Map("firstname" -> "Jake", "lastname" -> "Soap"), "User")
    for (_ <- 1 to 100) {
      createLabeledNode(Map("firstname" -> "Joe"), "User")
      createLabeledNode(Map("lastname" -> "Soap"), "User")
    }

    // When
    val result = executeWith(Configs.All, "MATCH (n:User) WHERE n.firstname = 'Jake' RETURN n",
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should not(useOperators("NodeIndexSeek"))
      }))

    // Then
    result.toComparableResult should equal(List(Map("n" -> n3)))
  }

  test("should use composite index when all predicates are present even in competition with other single property indexes with similar cardinality") {
    // Given
    graph.createIndex("User", "firstname")
    graph.createIndex("User", "lastname")
    graph.createIndex("User", "firstname", "lastname")
    val n1 = createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Smoke"), "User")
    createLabeledNode(Map("firstname" -> "Jake", "lastname" -> "Soap"), "User")
    for (_ <- 1 to 100) {
      createLabeledNode("User")
      createLabeledNode("User")
    }

    // When
    val result = executeWith(Configs.Interpreted, "MATCH (n:User) WHERE n.lastname = 'Soap' AND n.firstname = 'Joe' RETURN n",
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperatorWithText("NodeIndexSeek", ":User(firstname,lastname)")
        plan should not(useOperatorWithText("NodeIndexSeek", ":User(firstname)"))
        plan should not(useOperatorWithText("NodeIndexSeek", ":User(lastname)"))
      }, Configs.OldAndRule))

    // Then
    result.toComparableResult should equal(List(Map("n" -> n1)))
  }

  test("should not use composite index when index hint for alternative index is present") {
    // Given
    graph.createIndex("User", "firstname")
    graph.createIndex("User", "lastname")
    graph.createIndex("User", "firstname", "lastname")
    val n1 = createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Smoke"), "User")
    createLabeledNode(Map("firstname" -> "Jake", "lastname" -> "Soap"), "User")
    for (_ <- 1 to 100) {
      createLabeledNode("User")
      createLabeledNode("User")
    }

    // When
    val result = executeWith(Configs.All,
      """
        |MATCH (n:User)
        |USING INDEX n:User(lastname)
        |WHERE n.lastname = 'Soap' AND n.firstname = 'Joe'
        |RETURN n
        |""".stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should not(useOperatorWithText("NodeIndexSeek", ":User(firstname,lastname)"))
        plan should not(useOperatorWithText("NodeIndexSeek", ":User(firstname)"))
        plan should useOperatorWithText("NodeIndexSeek", ":User(lastname)")
      }, Configs.AllRulePlanners))

    // Then
    result.toComparableResult should equal(List(Map("n" -> n1)))
  }

  ignore("should use composite index with combined equality and existence predicates") { // Enable once we support index scan on composite indexes
    // Given
    graph.createIndex("User", "firstname")
    graph.createIndex("User", "lastname")
    graph.createIndex("User", "firstname", "lastname")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Smoke"), "User")
    val n3 = createLabeledNode(Map("firstname" -> "Jake", "lastname" -> "Soap"), "User")
    for (_ <- 1 to 100) {
      createLabeledNode("User")
      createLabeledNode("User")
    }

    // When
    val result = executeWith(Configs.Interpreted, "MATCH (n:User) WHERE exists(n.lastname) AND n.firstname = 'Jake' RETURN n",
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperatorWithText("NodeIndexScan", ":User(firstname,lastname)")
        plan should not(useOperatorWithText("NodeIndexSeek", ":User(firstname)"))
        plan should not(useOperatorWithText("NodeIndexSeek", ":User(lastname)"))
      }))

    // Then
    result.toComparableResult should equal(List(Map("n" -> n3)))
  }

  test("should be able to update composite index when only one property has changed") {
    graph.createIndex("Person", "firstname", "lastname")
    val n = graph.execute("CREATE (n:Person {firstname:'Joe', lastname:'Soap'}) RETURN n").columnAs("n").next().asInstanceOf[Node]
    graph.execute("MATCH (n:Person) SET n.lastname = 'Bloggs'")
    val result = executeWith(Configs.Interpreted, "MATCH (n:Person) where n.firstname = 'Joe' and n.lastname = 'Bloggs' RETURN n",
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators("NodeIndexSeek")
      }, Configs.OldAndRule))
    result.toComparableResult should equal(List(Map("n" -> n)))
  }

  test("should plan a composite index seek for a multiple property predicate expression when index is created after data") {
    executeWith(Configs.UpdateConf, "WITH RANGE(0,10) AS num CREATE (:Person {id:num})") // ensure label cardinality favors index
    executeWith(Configs.UpdateConf, "CREATE (n:Person {firstname:'Joe', lastname:'Soap'})")
    graph.createIndex("Person", "firstname")
    graph.createIndex("Person", "firstname", "lastname")
    executeWith(Configs.Interpreted, "MATCH (n:Person) WHERE n.firstname = 'Joe' AND n.lastname = 'Soap' RETURN n",
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperatorWithText("NodeIndexSeek", ":Person(firstname,lastname)")
      }, Configs.OldAndRule))
  }

  test("should use composite index correctly given two IN predicates") {
    // Given
    graph.createIndex("Foo", "bar", "baz")

    (0 to 9) foreach { bar =>
      (0 to 9) foreach { baz =>
        createLabeledNode(Map("bar" -> bar, "baz" -> baz, "idx" -> (bar * 10 + baz)), "Foo")
      }
    }

    // When
    val result = executeWith(Configs.Interpreted,
      """MATCH (n:Foo)
        |WHERE n.bar IN [0,1,2,3,4,5,6,7,8,9]
        |  AND n.baz IN [0,1,2,3,4,5,6,7,8,9]
        |RETURN n.idx as x
        |ORDER BY x""".stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperatorWithText("NodeIndexSeek", ":Foo(bar,baz)")
      }, Configs.OldAndRule))

    // Then
    result.toComparableResult should equal((0 to 99).map(i => Map("x" -> i)).toList)
  }

  test("should use composite index correctly with a single exact together with a List of seeks") {
    // Given
    graph.createIndex("Foo", "bar", "baz")

    (0 to 9) foreach { baz =>
      createLabeledNode(Map("bar" -> 1, "baz" -> baz), "Foo")
    }

    // When
    val result = executeWith(Configs.Interpreted,
      """MATCH (n:Foo)
        |WHERE n.bar = 1
        |  AND n.baz IN [0,1,2,3,4,5,6,7,8,9]
        |RETURN n.baz as x
        |ORDER BY x""".stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperatorWithText("NodeIndexSeek", ":Foo(bar,baz)")
      }, Configs.OldAndRule))

    // Then
    result.toComparableResult should equal((0 to 9).map(i => Map("x" -> i)).toList)
  }

  test("should use composite index correctly with a single exact together with a List of seeks, with the exact seek not being first") {
    // Given
    graph.createIndex("Foo", "bar", "baz")

    (0 to 9) foreach { baz =>
      createLabeledNode(Map("baz" -> (baz % 2), "bar" -> baz), "Foo")
    }

    // When
    val result = executeWith(Configs.Interpreted,
      """MATCH (n:Foo)
        |WHERE n.baz = 1
        |  AND n.bar IN [0,1,2,3,4,5,6,7,8,9]
        |RETURN n.bar as x
        |ORDER BY x""".stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperatorWithText("NodeIndexSeek", ":Foo(bar,baz)")
      }, Configs.OldAndRule))

    // Then
    result.toComparableResult should equal(List(Map("x" -> 1), Map("x" -> 3), Map("x" -> 5), Map("x" -> 7), Map("x" -> 9)))
  }

  test("should handle missing properties when populating index") {
    // Given
    createLabeledNode(Map("foo" -> 42), "PROFILES")
    createLabeledNode(Map("foo" -> 42, "bar" -> 1337, "baz" -> 1980), "L")

    // When
    graph.createIndex("L", "foo", "bar", "baz")

    // Then
    graph should haveIndexes(":L(foo,bar,baz)")
    val result = executeWith(Configs.Interpreted, "MATCH (n:L {foo: 42, bar: 1337, baz: 1980}) RETURN count(n)",
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperatorWithText("NodeIndexSeek", ":L(foo,bar,baz")
      }, Configs.OldAndRule))
    result.toComparableResult should equal(Seq(Map("count(n)" -> 1)))
  }

  test("should handle missing properties when adding to index after creation") {
    // Given
    createLabeledNode(Map("foo" -> 42), "PROFILES")

    // When
    graph.createIndex("L", "foo", "bar", "baz")
    createLabeledNode(Map("foo" -> 42, "bar" -> 1337, "baz" -> 1980), "L")

    // Then
    graph should haveIndexes(":L(foo,bar,baz)")
    val result = executeWith(Configs.Interpreted, "MATCH (n:L {foo: 42, bar: 1337, baz: 1980}) RETURN count(n)",
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperatorWithText("NodeIndexSeek", ":L(foo,bar,baz)")
      }, Configs.OldAndRule))
    result.toComparableResult should equal(Seq(Map("count(n)" -> 1)))
  }

  test("should not fail on multiple attempts to create a composite index") {
    // Given
    executeWith(Configs.Procs, "CREATE INDEX ON :Person(firstname, lastname)")
    executeWith(Configs.Procs, "CREATE INDEX ON :Person(firstname, lastname)")
  }

  test("should not use range queries against a composite index") {
    // Given
    graph.createIndex("X", "p1", "p2")
    val n = createLabeledNode(Map("p1" -> 1, "p2" -> 1), "X")

    // When
    val result = executeWith(Configs.Interpreted, "match (n:X) where n.p1 = 1 AND n.p2 > 0 return n;",
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan shouldNot useOperatorWithText("NodeIndexSeek", ":X(p1,p2)")
      }))

    // Then
    result.toComparableResult should equal(Seq(Map("n" -> n)))
  }

  test("should not use composite index for range, prefix, contains and exists predicates") {
    // Given
    graph.createIndex("User", "name", "surname", "age", "active")
    val n = createLabeledNode(Map("name" -> "joe", "surname" -> "soap", "age" -> 25, "active" -> true), "User")

    // For all combinations
    Seq(
      (Configs.Interpreted, "n.name = 'joe' AND n.surname = 'soap' AND n.age = 25 AND n.active = true", true), // all equality
      (Configs.Interpreted, "n.surname = 'soap' AND n.age = 25 AND n.active = true AND n.name = 'joe'", true), // different order
      (Configs.Interpreted, "n.name = 'joe' AND n.surname = 'soap' AND n.age = 25 AND exists(n.active)", false), // exists()
      (Configs.Interpreted, "n.name = 'joe' AND n.surname = 'soap' AND n.age >= 25 AND n.active = true", false), // inequality
      (Configs.Interpreted, "n.name = 'joe' AND n.surname STARTS WITH 's' AND n.age = 25 AND n.active = true", false), // prefix
      (Configs.Interpreted, "n.name = 'joe' AND n.surname ENDS WITH 'p' AND n.age = 25 AND n.active = true", false), // suffix
      (Configs.Interpreted, "n.name >= 'i' AND n.surname = 'soap' AND n.age = 25 AND n.active = true", false), // inequality first
      (Configs.Interpreted, "n.name STARTS WITH 'j' AND n.surname = 'soap' AND n.age = 25 AND n.active = true", false), // prefix first
      (Configs.Interpreted, "n.name CONTAINS 'j' AND n.surname = 'soap' AND n.age = 25 AND n.active = true", false), // contains first
      (Configs.Interpreted, "n.name = 'joe' AND n.surname STARTS WITH 'soap' AND n.age <= 25 AND exists(n.active)", false) // combination: equality, prefix, inequality, exists()
    ).foreach {
      case (testConfig, predicates, valid) =>

        // When
        val query = s"MATCH (n:User) WHERE $predicates RETURN n"
        val result = try {
          if (valid)
            executeWith(testConfig, query,
              planComparisonStrategy = ComparePlansWithAssertion((plan) => {
                //THEN
                plan should useOperatorWithText("NodeIndexSeek", ":User(name,surname,age,active)")
              }, Configs.OldAndRule))
          else
            executeWith(testConfig, query,
              planComparisonStrategy = ComparePlansWithAssertion((plan) => {
                //THEN
                plan shouldNot useOperatorWithText("NodeIndexSeek", ":User(name,surname,age,active)")
              }))
        } catch {
          case e: Exception =>
            System.err.println(query)
            throw e
        }
        try {

          // Then
          result.toComparableResult should equal(Seq(Map("n" -> n)))
        } catch {
          case e: Exception =>
            System.err.println(s"Failed for predicates: $predicates")
            System.err.println(result.executionPlanDescription())
            throw e
        }
    }
  }

  test("nested index join with composite indexes") {
    // given
    graph.createIndex("X", "p1", "p2")
    (1 to 1000) foreach { _ => // Get the planner to do what we expect it to!
      createLabeledNode("X")
    }
    val a = createNode("p1" -> 1, "p2" -> 1)
    val b = createLabeledNode(Map("p1" -> 1, "p2" -> 1), "X")

    // 2.3 excluded because the params syntax was not supported in that version
    val result = executeWith(Configs.Interpreted - Configs.Version2_3, "match (a), (b:X) where id(a) = $id AND b.p1 = a.p1 AND b.p2 = 1 return b",
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperatorWithText("NodeIndexSeek", ":X(p1,p2)")
      }, Configs.OldAndRule), params = Map("id" -> a.getId))

    result.toComparableResult should equal(Seq(Map("b" -> b)))
  }

  test("should use composite index with spatial") {
    // Given
    val n1 = createLabeledNode(Map("name" -> "Joe", "city" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 1.2, 5.6).asObjectCopy()), "User")
    createLabeledNode(Map("name" -> "Joe", "city" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 1.2, 3.4).asObjectCopy()), "User")
    createLabeledNode(Map("name" -> "Joe", "city" -> "P:2-7203[1.2, 5.6]"), "User")

    // When
    val query = "MATCH (n:User) WHERE n.name = 'Joe' AND n.city = point({x: 1.2, y: 5.6}) RETURN n"

    val resultNoIndex = executeWith(Configs.Interpreted - Configs.OldAndRule, query)

    graph.createIndex("User", "name", "city")
    val resultIndex = executeWith(Configs.Interpreted - Configs.OldAndRule, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators("NodeIndexSeek")
      }))

    // Then
    resultNoIndex.toComparableResult should equal(List(Map("n" -> n1)))
    resultIndex.toComparableResult should equal(resultNoIndex.toComparableResult)
  }

  test("should use composite index with temporal") {
    // Given
    val n1 = createLabeledNode(Map("date" -> LocalDate.of(1991, 10, 18), "time" -> LocalTime.of(21, 22, 0)), "Label")
    val n2 = createLabeledNode(Map("date" -> LocalDate.of(1991, 10, 18), "time" -> OffsetTime.of(21, 22, 0, 0, ZoneOffset.of("+00:00"))), "Label")
    val n3 = createLabeledNode(Map("date" -> "1991-10-18", "time" -> LocalTime.of(21, 22, 0)), "Label")
    val n4 = createLabeledNode(Map("date" -> LocalDate.of(1991, 10, 18).toEpochDay, "time" -> LocalTime.of(21, 22, 0)), "Label")

    // When
    val query =
      """
        |MATCH (n:Label)
        |WHERE n.date = date('1991-10-18') AND n.time = localtime('21:22')
        |RETURN n
      """.stripMargin

    val resultNoIndex = executeWith(Configs.Interpreted - Configs.OldAndRule, query)

    graph.createIndex("Label", "date", "time")
    val resultIndex = executeWith(Configs.Interpreted - Configs.OldAndRule, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators("NodeIndexSeek")
      }))

    // Then
    resultNoIndex.toComparableResult should equal(List(Map("n" -> n1)))
    resultIndex.toComparableResult should equal(resultNoIndex.toComparableResult)
  }

  test("should use composite index with duration") {
    // Given
    val n1 = createLabeledNode(Map("name" -> "Neo", "result" -> DurationValue.duration(0, 0, 1800, 0).asObject()), "Runner")
    createLabeledNode(Map("name" -> "Neo", "result" -> LocalTime.of(0, 30, 0)), "Runner")
    createLabeledNode(Map("name" -> "Neo", "result" -> "PT30M"), "Runner")

    // When
    val query =
      """
        |MATCH (n:Runner)
        |WHERE n.name = 'Neo' AND n.result = duration('PT30M')
        |RETURN n
      """.stripMargin

    val resultNoIndex = executeWith(Configs.Interpreted - Configs.OldAndRule, query)

    graph.createIndex("Runner", "name", "result")
    val resultIndex = executeWith(Configs.Interpreted - Configs.OldAndRule, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators("NodeIndexSeek")
      }))

    // Then
    resultNoIndex.toComparableResult should equal(List(Map("n" -> n1)))
    resultIndex.toComparableResult should equal(resultNoIndex.toComparableResult)
  }

  case class haveIndexes(expectedIndexes: String*) extends Matcher[GraphDatabaseQueryService] {
    def apply(graph: GraphDatabaseQueryService): MatchResult = {
      graph.inTx {
        val indexNames = graph.schema().getIndexes.asScala.toList.map(i => s":${i.getLabel}(${i.getPropertyKeys.asScala.toList.mkString(",")})")
        val result = expectedIndexes.forall(i => indexNames.contains(i.toString))
        MatchResult(
          result,
          s"Expected graph to have indexes ${expectedIndexes.mkString(", ")}, but it was ${indexNames.mkString(", ")}",
          s"Expected graph to not have indexes ${expectedIndexes.mkString(", ")}, but it did."
        )
      }
    }
  }

}
