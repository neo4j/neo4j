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

import org.neo4j.cypher.internal.InternalExecutionResult
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport, QueryStatisticsTestSupport}
import org.scalatest.matchers.{MatchResult, Matcher}

class MatchAggregationsBackedByCountStoreAcceptanceTest
  extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  test("do not plan counts store lookup for loop matches") {
    val n = createNode()
    // two loops
    relate(n, n)
    relate(n, n)
    // one non-loop
    relate(n, createNode())

    val resultStar = executeWithAllPlannersAndCompatibilityMode("MATCH (a)-->(a) RETURN count(*)")
    val resultVar = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (a)-[r]->(a) RETURN count(r)")

    resultStar.toList should equal(List(Map("count(*)" -> 2)))
    resultVar.toList should equal(List(Map("count(r)" -> 2)))

    resultStar.executionPlanDescription() shouldNot includeOperation("RelationshipCountFromCountStore")
    resultVar.executionPlanDescription() shouldNot includeOperation("RelationshipCountFromCountStore")
  }

  test("counts nodes using count store") {
    // Given
    withModel(

      // When
      query = "MATCH (n) RETURN count(n)", f = { result =>

        // Then
        result.columnAs("count(n)").toSet[Int] should equal(Set(3))

    }, allRuntimes = true)
  }

  test("capitalized COUNTS nodes using count store") {
    // Given
    withModel(

      // When
      query = "MATCH (n) RETURN COUNT(n)", f = { result =>

        // Then
        result.columnAs("COUNT(n)").toSet[Int] should equal(Set(3))

      }, allRuntimes = true)
  }

  test("counts nodes using count store with count(*)") {
    // Given
    withModel(

      // When
      query = "MATCH (n) RETURN count(*)", f = { result =>

        // Then
        result.columnAs("count(*)").toSet[Int] should equal(Set(3))

    }, allRuntimes = true)
  }

  test("counts labeled nodes using count store") {
    // Given
    withModel(label1 = "Admin",

      // When
      query = "MATCH (n:User) RETURN count(n)", f = { result =>

        // Then
        result.columnAs("count(n)").toSet[Int] should equal(Set(1))

      }, allRuntimes = true)
  }

  test("counts nodes using count store and projection expression") {
    // Given
    withModel(label1 = "Admin",

      // When
      query = "MATCH (n:User) RETURN count(n) > 0", f = { result =>

        // Then
        result.columnAs("count(n) > 0").toSet[Boolean] should equal(Set(true))

      }, expectedResultOnEmptyDatabase = Set(false))
  }

  test("counts nodes using count store and projection expression with variable") {
    // Given
    withModel(

      // When
      query = "MATCH (n) RETURN count(n)/2.0*5 as someNum", f = { result =>

        // Then
        result.columnAs("someNum").toSet[Int] should equal(Set(7.5))

      })
  }

  test("counts relationships with unspecified type using count store") {
    // Given
    withRelationshipsModel(

      // When
      query = "MATCH ()-[r]->() RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(2))

      }, allRuntimes = true)
  }

  test("counts relationships with unspecified type using count store with count(*)") {
    // Given
    withRelationshipsModel(

      // When
      query = "MATCH ()-->() RETURN count(*)", f = { result =>

        // Then
        result.columnAs("count(*)").toSet[Int] should equal(Set(2))

      }, allRuntimes = true)
  }

  test("counts relationships with type using count store") {
    // Given
    withRelationshipsModel(

      // When
      query = "MATCH ()-[r:KNOWS]->() RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(1))

      }, allRuntimes = true)
  }

  test("counts relationships with type using count store with count(*)") {
    // Given
    withRelationshipsModel(

      // When
      query = "MATCH ()-[r:KNOWS]->() RETURN count(*)", f = { result =>

        // Then
        result.columnAs("count(*)").toSet[Int] should equal(Set(1))

      }, allRuntimes = true)
  }

  test("counts relationships with unspecified type and labeled source node using count store") {
    // Given
    withRelationshipsModel(

      // When
      query = "MATCH (:User)-[r]->() RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(1))

      }, allRuntimes = true)
  }

  test("counts relationships with type and labeled source node using count store") {
    // Given
    withRelationshipsModel(

      // When
      query = "MATCH ()<-[r:KNOWS]-(:User) RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(1))

      }, allRuntimes = true)
  }

  test("counts relationships with unspecified type and labeled destination node using count store") {
    // Given
    withRelationshipsModel(

      // When
      query = "MATCH ()-[r]->(:User) RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(1))

      }, allRuntimes = true)
  }

  test("counts relationships with type and labeled destination node using count store") {
    // Given
    withRelationshipsModel(

      // When
      query = "MATCH ()-[r:KNOWS]->(:User) RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(1))

      }, allRuntimes = true)
  }

  test("counts relationships with type and labeled source and destination without using count store") {
    // Given
    withRelationshipsModel(expectedLogicalPlan = "NodeByLabelScan",

      // When
      query = "MATCH (:User)-[r:KNOWS]->(:User) RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(1))

      }, allRuntimes = true)
  }

  test("counts relationships with unspecified type and labeled source and destination without using count store") {
    // Given
    withRelationshipsModel(expectedLogicalPlan = "NodeByLabelScan",

      // When
      query = "MATCH (:User)-[r]->(:User) RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(1))

      }, allRuntimes = true)
  }

  test("counts relationships with type, reverse direction and labeled source node using count store") {
    // Given
    withRelationshipsModel(

      // When
      query = "MATCH (:User)<-[r:KNOWS]-() RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(1))

      }, allRuntimes = true)
  }

  test("counts relationships with type, reverse direction and labeled destination node using count store") {
    // Given
    withRelationshipsModel(

      // When
      query = "MATCH ()<-[r:KNOWS]-(:User) RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(1))

      }, allRuntimes = true)
  }

  test("counts relationships with type, any direction and labeled source node without using count store") {
    // Given
    withRelationshipsModel(expectedLogicalPlan = "NodeByLabelScan",

      // When
      query = "MATCH (:User)-[r:KNOWS]-() RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(2))

      }, allRuntimes = true)
  }

  test("counts relationships with type, any direction and labeled destination node without using count store") {
    // Given
    withRelationshipsModel(expectedLogicalPlan = "NodeByLabelScan",

      // When
      query = "MATCH ()-[r:KNOWS]-(:User) RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(2))

      }, allRuntimes = true)
  }

  test("counts relationships with type, any direction and no labeled nodes without using count store") {
    // Given
    withRelationshipsModel(expectedLogicalPlan = "AllNodesScan",

      // When
      query = "MATCH ()-[r:KNOWS]-() RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(2))

      }, allRuntimes = true)
  }

  test("counts nodes using count store considering transaction state") {
    // Given
    withModelAndTransaction(

      // When
      query = "MATCH (n:User) RETURN count(n)", f = { result =>

        // Then
        result.columnAs("count(n)").toSet[Int] should equal(Set(3))

      }, allRuntimes = true)
  }

  test("counts labeled nodes using count store considering transaction state (test1)") {
    // Given
    withModelAndTransaction(label1 = "Admin", label2 = "User",

      // When
      query = "MATCH (n:User) RETURN count(n)", f = { result =>

        // Then
        result.columnAs("count(n)").toSet[Int] should equal(Set(2))

      }, allRuntimes = true)
  }

  test("counts labeled nodes using count store considering transaction state (test2)") {
    // Given
    withModelAndTransaction(label1 = "Admin", label2 = "User",

      // When
      query = "MATCH (n:Admin) RETURN count(n)", f = { result =>

        // Then
        result.columnAs("count(n)").toSet[Int] should equal(Set(1))

      }, allRuntimes = true)
  }

  test("counts labeled nodes using count store considering transaction state containing newly created label (test1)") {
    // Given
    withModelAndTransaction(label3 = "Admin",

      // When
      query = "MATCH (n:Admin) RETURN count(n)", f = { result =>

        // Then
        result.columnAs("count(n)").toSet[Int] should equal(Set(1))

      }, allRuntimes = true)
  }


  test("counts labeled nodes using count store considering transaction state containing newly created label (test2)") {
    // Given
    withModelAndTransaction(label3 = "Admin",

      // When
      query = "MATCH (n:User) RETURN count(n)", f = { result =>

        // Then
        result.columnAs("count(n)").toSet[Int] should equal(Set(2))

      }, allRuntimes = true)
  }

  test("counts nodes using count store and projection expression considering transaction state") {
    // Given
    withModelAndTransaction(label1 = "Admin",

      // When
      query = "MATCH (n:User) RETURN count(n) > 1", f = { result =>

        // Then
        result.columnAs("count(n) > 1").toSet[Boolean] should equal(Set(true))

      },
      expectedResultOnEmptyDatabase = Set(false))
  }

  test("counts nodes using count store and projection expression with variable considering transaction state") {
    // Given
    withModelAndTransaction(

      // When
      query = "MATCH (n) RETURN count(n)/3*5 as someNum", f = { result =>

        // Then
        result.columnAs("someNum").toSet[Int] should equal(Set(5))

      })
  }

  test("counts relationships using count store considering transaction state") {
    // Given
    withRelationshipsModelAndTransaction(

      // When
      query = "MATCH ()-[r]->() RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(3))

      }, allRuntimes = true)
  }

  test("counts relationships with type using count store considering transaction state") {
    // Given
    withRelationshipsModelAndTransaction(

      // When
      query = "MATCH ()-[r:KNOWS]->() RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(3))

      }, allRuntimes = true)
  }

  test("counts relationships with multiple types using count store considering transaction state") {
    // Given
    withRelationshipsModelAndTransaction(type1 = "KNOWS", type2 = "FOLLOWS", type3 = "FRIEND_OF",

      // When
      query = "MATCH ()-[r:KNOWS|FOLLOWS]->() RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(2))

      }, allRuntimes = true)
  }

  test("counts relationships using count store and projection with expression considering transaction state") {
    // Given
    withRelationshipsModelAndTransaction(

      // When
      query = "MATCH ()-[r]->() RETURN count(r) > 2", f = { result =>

        // Then
        result.columnAs("count(r) > 2").toSet[Boolean] should equal(Set(true))

      },
      expectedResultOnEmptyDatabase = Set(false))
  }

  test("counts relationships using count store and projection with expression and variable considering transaction state") {
    // Given
    withRelationshipsModelAndTransaction(

      // When
      query = "MATCH ()-[r]->() RETURN count(r)/3*5 as someNum", f = { result =>

        // Then
        result.columnAs("someNum").toSet[Int] should equal(Set(5))

      })
  }

  test("counts relationships using count store and horizon with further query") {
    // Given
    withRelationshipsModelAndTransaction(label1 = "User", label2 = "Admin", label3 = "Person",

      // When
      query =
        """
          |MATCH (:User)-[r:KNOWS]->() WITH count(r) as userKnows
          |MATCH (n)-[r:KNOWS]->() WITH count(r) as otherKnows, n, userKnows WHERE otherKnows <> userKnows
          |RETURN userKnows, otherKnows
        """.stripMargin, f = { result =>

        // Then
        result.toList should equal(List(Map("userKnows" -> 2, "otherKnows" -> 1)))

      },
      expectedResultOnEmptyDatabase = Set.empty)
  }

//  MATCH (n:X)-[r:Y]->() WITH count(r) as rcount MATCH (n)-[r:Y]->() WHERE count(r) = rcount RETURN rcount, labels(n)
  test("counts relationships with type using count store considering transaction state and multiple types in model") {
    // Given
    withRelationshipsModelAndTransaction(type1 = "KNOWS", type2 = "FOLLOWS", type3 = "KNOWS",

      // When
      query = "MATCH ()-[r:KNOWS]->() RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(2))

      }, allRuntimes = true)
  }

  test("counts relationships with type and labeled source using count store considering transaction state") {
    // Given
    withRelationshipsModelAndTransaction(

      // When
      query = "MATCH (:User)-[r:KNOWS]->() RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(3))

      }, allRuntimes = true)
  }

  test("counts relationships with type, reverse direction and labeled source using count store considering transaction state") {
    // Given
    withRelationshipsModelAndTransaction(

      // When
      query = "MATCH (:User)<-[r:KNOWS]-() RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(3))

      }, allRuntimes = true)
  }

  test("counts relationships with unspecified type and labeled source using count store considering transaction state") {
    // Given
    withRelationshipsModelAndTransaction(

      // When
      query = "MATCH (:User)-[r]->() RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(3))

      }, allRuntimes = true)
  }

  test("counts relationships with type and labeled destination using count store considering transaction state") {
    // Given
    withRelationshipsModelAndTransaction(

      // When
      query = "MATCH ()-[r:KNOWS]->(:User) RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(3))

      }, allRuntimes = true)
  }

  test("counts relationships with type, reverse direction and labeled destination using count store considering transaction state") {
    // Given
    withRelationshipsModelAndTransaction(

      // When
      query = "MATCH ()<-[r:KNOWS]-(:User) RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(3))

      }, allRuntimes = true)
  }

  test("counts relationships with unspecified type and labeled destination using count store considering transaction state") {
    // Given
    withRelationshipsModelAndTransaction(

      // When
      query = "MATCH ()-[r]->(:User) RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(3))

      }, allRuntimes = true)
  }

  test("counts relationships with type and labeled source and destination without using count store considering transaction state") {
    // Given
    withRelationshipsModelAndTransaction(expectedLogicalPlan = "NodeByLabelScan",

      // When
      query = "MATCH (:User)-[r:KNOWS]->(:User) RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(3))

      }, allRuntimes = true)
  }

  test("counts relationships with unspecified type and labeled source and destination without using count store considering transaction state") {
    // Given
    withRelationshipsModelAndTransaction(expectedLogicalPlan = "NodeByLabelScan",

      // When
      query = "MATCH (:User)-[r]->(:User) RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(3))

      }, allRuntimes = true)
  }

  test("should work even when the tokens are already known") {
    innerExecute(
      s"""
         |CREATE (p:User {name: 'Petra'})
         |CREATE (s:User {name: 'Steve'})
         |CREATE (p)-[:KNOWS]->(s)
      """.stripMargin)

    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (:User)-[r:KNOWS]->() RETURN count(r)")

    result.columnAs("count(r)").toSet[Int] should equal(Set(1))
  }

  test("runtime checking of tokens - nodes - not existing when planning nor when running") {
    createLabeledNode("NotRelated")
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n:Nonexistent) RETURN count(n)")
    result.toList should equal(List(Map("count(n)" -> 0)))
  }

  test("runtime checking of tokens - nodes - not existing when planning but exists when running") {
    createLabeledNode("NotRelated")
    val result1 = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n:justCreated) RETURN count(n)")
    result1.toList should equal(List(Map("count(n)" -> 0)))
    createLabeledNode("justCreated")
    val result2 = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n:justCreated) RETURN count(n)")
    result2.toList should equal(List(Map("count(n)" -> 1)))
  }

  test("runtime checking of tokens - relationships - not existing when planning nor when running") {
    relate(createNode(), createNode(), "UNRELATED")
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH ()-[r:Nonexistent]->() RETURN count(r)")
    result.toList should equal(List(Map("count(r)" -> 0)))
  }

  test("runtime checking of tokens - relationships - not existing when planning but exists when running") {
    relate(createNode(), createNode(), "UNRELATED")
    val result1 = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH ()-[r:justCreated]->() RETURN count(r)")
    result1.toList should equal(List(Map("count(r)" -> 0)))
    relate(createNode(), createNode(), "justCreated")
    val result2 = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH ()-[r:justCreated]->() RETURN count(r)")
    result2.toList should equal(List(Map("count(r)" -> 1)))
  }

  test("count store on two unlabeled nodes") {
    // Given
    withModel(

      // When
      query = "MATCH (n), (m) RETURN count(n)", f = { result =>

        // Then
        result.columnAs("count(n)").toSet[Int] should equal(Set(9))

      }, allRuntimes = true)
  }

  test("count store on two unlabeled nodes and count(*)") {
    // Given
    withModel(

      // When
      query = "MATCH (n), (m) RETURN count(*)", f = { result =>

        // Then
        result.columnAs("count(*)").toSet[Int] should equal(Set(9))

      }, allRuntimes = true)
  }

  test("count store on one labeled node and one unlabeled") {
    // Given
    withModel(

      // When
      query = "MATCH (n:User),(m) RETURN count(n)", f = { result =>

        // Then
        result.columnAs("count(n)").toSet[Int] should equal(Set(6))

      }, allRuntimes = true)
  }

  test("count store on one labeled node and one unlabeled and count(*)") {
    // Given
    withModel(

      // When
      query = "MATCH (n:User),(m) RETURN count(*)", f = { result =>

        // Then
        result.columnAs("count(*)").toSet[Int] should equal(Set(6))

      }, allRuntimes = true)
  }

  test("count store on two labeled nodes") {
    // Given
    withModel(

      // When
      query = "MATCH (n:User),(m:User) RETURN count(n)", f = { result =>

        // Then
        result.columnAs("count(n)").toSet[Int] should equal(Set(4))

      }, allRuntimes = true)
  }

  test("count store with many nodes") {
    // Given
    withModel(

      // When
      query = "MATCH (n:User),(m),(o:User),(p) RETURN count(*)", f = { result =>

        // Then
        result.columnAs("count(*)").toSet[Int] should equal(Set(36))

      }, allRuntimes = true)
  }

  test("count store with many but odd number of nodes") {
    // Given
    withModel(

      // When
      query = "MATCH (n:User),(m),(o:User),(p), (q) RETURN count(*)", f = { result =>

        // Then
        result.columnAs("count(*)").toSet[Int] should equal(Set(108))

      }, allRuntimes = true)
  }

  def withModel(label1: String = "User",
                label2: String = "User",
                type1: String = "KNOWS",
                expectedLogicalPlan: String = "NodeCountFromCountStore",
                query: String, f: InternalExecutionResult => Unit,
                expectedResultOnEmptyDatabase: Set[Any] = Set(0),
                allRuntimes: Boolean = false): Unit = {
    verifyOnEmptyDatabase(expectedLogicalPlan, query, expectedResultOnEmptyDatabase, allRuntimes)

    innerExecute(
      s"""
         |CREATE (p:$label1 {name: 'Petra'})
         |CREATE (s:$label2 {name: 'Steve'})
         |CREATE (p)-[:$type1]->(s)
         |CREATE (a)-[:LOOP]->(a)
      """.stripMargin)

    val result: InternalExecutionResult =
      if (allRuntimes) executeWithAllPlannersAndRuntimesAndCompatibilityMode(query)
      else executeWithAllPlannersAndCompatibilityMode(query)
    result.executionPlanDescription() should includeOperation(expectedLogicalPlan)
    f(result)

    deleteAllEntities()

    verifyOnEmptyDatabase(expectedLogicalPlan, query, expectedResultOnEmptyDatabase, allRuntimes)
  }

  private def verifyOnEmptyDatabase(expectedLogicalPlan: String, query: String,
                                   expectedResult: Set[Any], allRuntimes: Boolean): Unit = {
    val resultOnEmptyDb: InternalExecutionResult =
      if (allRuntimes) executeWithAllPlannersAndRuntimesAndCompatibilityMode(query)
      else  executeWithAllPlannersAndCompatibilityMode(query)
    resultOnEmptyDb.executionPlanDescription() should includeOperation(expectedLogicalPlan)
    withClue("should return a count of 0 on an empty database") {
      resultOnEmptyDb.columnAs(resultOnEmptyDb.columns.head).toSet[Int] should equal(expectedResult)
    }
  }

  def withRelationshipsModel(label1: String = "User",
                             label2: String = "User",
                             type1: String = "KNOWS",
                             expectedLogicalPlan: String = "RelationshipCountFromCountStore",
                             query: String, f: InternalExecutionResult => Unit,
                             allRuntimes: Boolean = false): Unit = {
    withModel(label1, label2, type1, expectedLogicalPlan, query, f, allRuntimes = allRuntimes)
  }

  def withModelAndTransaction(label1: String = "User",
                label2: String = "User",
                label3: String = "User",
                type1: String = "KNOWS",
                type2: String = "KNOWS",
                type3: String = "KNOWS",
                expectedLogicalPlan: String = "NodeCountFromCountStore",
                query: String, f: InternalExecutionResult => Unit,
                expectedResultOnEmptyDatabase: Set[Any] = Set(0),
                allRuntimes: Boolean = false): Unit = {
    verifyOnEmptyDatabase(expectedLogicalPlan, query, expectedResultOnEmptyDatabase, allRuntimes)

    innerExecute(
      s"""
         |CREATE (m:X {name: 'Mats'})
         |CREATE (p:$label1 {name: 'Petra'})
         |CREATE (s:$label2 {name: 'Steve'})
         |CREATE (p)-[:$type1]->(s)
         |CREATE (m)-[:$type1]->(s)
      """.stripMargin)

    graph.inTx {
      executeWithCostPlannerAndInterpretedRuntimeOnly("MATCH (m:X)-[r]->() DELETE m, r")
      executeWithCostPlannerAndInterpretedRuntimeOnly(
        s"""
           |MATCH (p:$label1 {name: 'Petra'})
           |MATCH (s:$label2 {name: 'Steve'})
           |CREATE (c:$label3 {name: 'Craig'})
           |CREATE (p)-[:$type2]->(c)
           |CREATE (c)-[:$type3]->(s)
        """.stripMargin)
      val result: InternalExecutionResult =
        if (allRuntimes) executeWithAllPlannersAndRuntimesAndCompatibilityMode(query)
        else executeWithCostPlannerAndInterpretedRuntimeOnly(query)
      result.executionPlanDescription() should includeOperation(expectedLogicalPlan)
      f(result)
    }

    deleteAllEntities()
    verifyOnEmptyDatabase(expectedLogicalPlan, query, expectedResultOnEmptyDatabase, allRuntimes = allRuntimes)
  }

  def withRelationshipsModelAndTransaction(label1: String = "User",
                        label2: String = "User",
                        label3: String = "User",
                        type1: String = "KNOWS",
                        type2: String = "KNOWS",
                        type3: String = "KNOWS",
                        expectedLogicalPlan: String = "RelationshipCountFromCountStore",
                        query: String, f: InternalExecutionResult => Unit,
                        expectedResultOnEmptyDatabase: Set[Any] = Set(0),
                        allRuntimes: Boolean = false): Unit = {
    withModelAndTransaction(label1, label2, label3, type1, type2, type3,
                            expectedLogicalPlan, query, f, expectedResultOnEmptyDatabase, allRuntimes)
  }

  case class includeOperation(operationName: String) extends Matcher[InternalPlanDescription] {

    override def apply(result: InternalPlanDescription): MatchResult = {
      val operationExists = result.flatten.exists { description =>
        description.name == operationName
      }

      MatchResult(operationExists, matchResultMsg(negated = false, result), matchResultMsg(negated = true, result))
    }

    private def matchResultMsg(negated: Boolean, result: InternalPlanDescription) =
      s"$operationName ${if (negated) "" else "not"} found in plan description\n $result"
  }
}
