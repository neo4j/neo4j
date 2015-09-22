/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription
import org.neo4j.cypher.{NewPlannerTestSupport, QueryStatisticsTestSupport, ExecutionEngineFunSuite}
import org.scalatest.matchers.{MatchResult, Matcher}

class MatchAggregationsBackedByCountStoreAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  test("counts nodes using count store") {
    // Given
    withModel(

      // When
      query = "MATCH (n) RETURN count(n)", f = { result =>

        // Then
        result.columnAs("count(n)").toSet[Int] should equal(Set(2))

    })
  }

  test("counts labeled nodes using count store") {
    // Given
    withModel(label1 = "Admin",

      // When
      query = "MATCH (n:User) RETURN count(n)", f = { result =>

        // Then
        result.columnAs("count(n)").toSet[Int] should equal(Set(1))

      })
  }

  test("counts nodes using count store and projection expression") {
    // Given
    withModel(label1 = "Admin",

      // When
      query = "MATCH (n:User) RETURN count(n) > 0", f = { result =>

        // Then
        result.columnAs("count(n) > 0").toSet[Boolean] should equal(Set(true))

      })
  }

  test("counts nodes using count store and projection expression with identifier") {
    // Given
    withModel(

      // When
      query = "MATCH (n) RETURN count(n)/2*5 as someNum", f = { result =>

        // Then
        result.columnAs("someNum").toSet[Int] should equal(Set(5))

      })
  }

  test("counts relationships with unspecified type using count store") {
    // Given
    withRelationshipsModel(

      // When
      query = "MATCH ()-[r]->() RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(1))

      })
  }

  test("counts relationships with type using count store") {
    // Given
    withRelationshipsModel(

      // When
      query = "MATCH ()-[r:KNOWS]->() RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(1))

      })
  }

  test("counts relationships with unspecified type and labeled source node using count store") {
    // Given
    withRelationshipsModel(

      // When
      query = "MATCH (:User)-[r]->() RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(1))

      })
  }

  test("counts relationships with type and labeled source node using count store") {
    // Given
    withRelationshipsModel(

      // When
      query = "MATCH (:User)-[r:KNOWS]->() RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(1))

      })
  }

  test("counts relationships with unspecified type and labeled destination node using count store") {
    // Given
    withRelationshipsModel(

      // When
      query = "MATCH ()-[r]->(:User) RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(1))

      })
  }

  test("counts relationships with type and labeled destination node using count store") {
    // Given
    withRelationshipsModel(

      // When
      query = "MATCH ()-[r:KNOWS]->(:User) RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(1))

      })
  }

  test("counts relationships with type and labeled source and destination without using count store") {
    // Given
    withRelationshipsModel(expectedLogicalPlan = "NodeByLabelScan",

      // When
      query = "MATCH (:User)-[r:KNOWS]->(:User) RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(1))

      })
  }

  test("counts relationships with unspecified type and labeled source and destination without using count store") {
    // Given
    withRelationshipsModel(expectedLogicalPlan = "NodeByLabelScan",

      // When
      query = "MATCH (:User)-[r]->(:User) RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(1))

      })
  }

  test("counts nodes using count store considering transaction state") {
    // Given
    withModelAndTransaction(

      // When
      query = "MATCH (n:User) RETURN count(n)", f = { result =>

        // Then
        result.columnAs("count(n)").toSet[Int] should equal(Set(3))

      })
  }

  test("counts labeled nodes using count store considering transaction state (test1)") {
    // Given
    withModelAndTransaction(label1 = "Admin", label2 = "User",

      // When
      query = "MATCH (n:User) RETURN count(n)", f = { result =>

        // Then
        result.columnAs("count(n)").toSet[Int] should equal(Set(2))

      })
  }

  test("counts labeled nodes using count store considering transaction state (test2)") {
    // Given
    withModelAndTransaction(label1 = "Admin", label2 = "User",

      // When
      query = "MATCH (n:Admin) RETURN count(n)", f = { result =>

        // Then
        result.columnAs("count(n)").toSet[Int] should equal(Set(1))

      })
  }

  test("counts labeled nodes using count store considering transaction state containing newly created label (test1)") {
    // Given
    withModelAndTransaction(label3 = "Admin",

      // When
      query = "MATCH (n:Admin) RETURN count(n)", f = { result =>

        // Then
        result.columnAs("count(n)").toSet[Int] should equal(Set(1))

      })
  }


  test("counts labeled nodes using count store considering transaction state containing newly created label (test2)") {
    // Given
    withModelAndTransaction(label3 = "Admin",

      // When
      query = "MATCH (n:User) RETURN count(n)", f = { result =>

        // Then
        result.columnAs("count(n)").toSet[Int] should equal(Set(2))

      })
  }

  test("counts nodes using count store and projection expression considering transaction state") {
    // Given
    withModelAndTransaction(label1 = "Admin",

      // When
      query = "MATCH (n:User) RETURN count(n) > 1", f = { result =>

        // Then
        result.columnAs("count(n) > 1").toSet[Boolean] should equal(Set(true))

      })
  }

  test("counts nodes using count store and projection expression with identifier considering transaction state") {
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

      })
  }

  test("counts relationships with type using count store considering transaction state") {
    // Given
    withRelationshipsModelAndTransaction(

      // When
      query = "MATCH ()-[r:KNOWS]->() RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(3))

      })
  }

  test("counts relationships with multiple types using count store considering transaction state") {
    // Given
    withRelationshipsModelAndTransaction(type1 = "KNOWS", type2 = "FOLLOWS", type3 = "FRIEND_OF",

      // When
      query = "MATCH ()-[r:KNOWS|FOLLOWS]->() RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(2))

      })
  }

  test("counts relationships using count store and projection with expression considering transaction state") {
    // Given
    withRelationshipsModelAndTransaction(

      // When
      query = "MATCH ()-[r]->() RETURN count(r) > 2", f = { result =>

        // Then
        result.columnAs("count(r) > 2").toSet[Boolean] should equal(Set(true))

      })
  }

  test("counts relationships using count store and projection with expression and identifier considering transaction state") {
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

      })
  }

//  MATCH (n:X)-[r:Y]->() WITH count(r) as rcount MATCH (n)-[r:Y]->() WHERE count(r) = rcount RETURN rcount, labels(n)
  test("counts relationships with type using count store considering transaction state and multiple types in model") {
    // Given
    withRelationshipsModelAndTransaction(type1 = "KNOWS", type2 = "FOLLOWS", type3 = "KNOWS",

      // When
      query = "MATCH ()-[r:KNOWS]->() RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(2))

      })
  }

  test("counts relationships with type and labeled source using count store considering transaction state") {
    // Given
    withRelationshipsModelAndTransaction(

      // When
      query = "MATCH (:User)-[r:KNOWS]->() RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(3))

      })
  }

  test("counts relationships with unspecified type and labeled source using count store considering transaction state") {
    // Given
    withRelationshipsModelAndTransaction(

      // When
      query = "MATCH (:User)-[r]->() RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(3))

      })
  }

  test("counts relationships with type and labeled destination using count store considering transaction state") {
    // Given
    withRelationshipsModelAndTransaction(

      // When
      query = "MATCH ()-[r:KNOWS]->(:User) RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(3))

      })
  }

  test("counts relationships with unspecified type and labeled destination using count store considering transaction state") {
    // Given
    withRelationshipsModelAndTransaction(

      // When
      query = "MATCH ()-[r]->(:User) RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(3))

      })
  }

  test("counts relationships with type and labeled source and destination without using count store considering transaction state") {
    // Given
    withRelationshipsModelAndTransaction(expectedLogicalPlan = "NodeByLabelScan",

      // When
      query = "MATCH (:User)-[r:KNOWS]->(:User) RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(3))

      })
  }

  test("counts relationships with unspecified type and labeled source and destination without using count store considering transaction state") {
    // Given
    withRelationshipsModelAndTransaction(expectedLogicalPlan = "NodeByLabelScan",

      // When
      query = "MATCH (:User)-[r]->(:User) RETURN count(r)", f = { result =>

        // Then
        result.columnAs("count(r)").toSet[Int] should equal(Set(3))

      })
  }

  def withModel(label1: String = "User",
                label2: String = "User",
                type1: String = "KNOWS",
                expectedLogicalPlan: String = "CountStoreNodeAggregation",
                query: String, f: InternalExecutionResult => Unit): Unit = {
    executeWithRulePlanner(
      s"""
         |CREATE (p:$label1 {name: 'Petra'})
         |CREATE (s:$label2 {name: 'Steve'})
         |CREATE (p)-[:$type1]->(s)
      """.stripMargin)

    val result: InternalExecutionResult = executeWithCostPlannerOnly(query)
    result.executionPlanDescription() should includeOperation(expectedLogicalPlan)
    f(result)
  }

  def withRelationshipsModel(label1: String = "User",
                             label2: String = "User",
                             type1: String = "KNOWS",
                             expectedLogicalPlan: String = "CountStoreRelationshipAggregation",
                             query: String, f: InternalExecutionResult => Unit): Unit = {
    withModel(label1, label2, type1, expectedLogicalPlan, query, f)
  }

  def withModelAndTransaction(label1: String = "User",
                label2: String = "User",
                label3: String = "User",
                type1: String = "KNOWS",
                type2: String = "KNOWS",
                type3: String = "KNOWS",
                expectedLogicalPlan: String = "CountStoreNodeAggregation",
                query: String, f: InternalExecutionResult => Unit): Unit = {
    executeWithRulePlanner(
      s"""
         |CREATE (p:$label1 {name: 'Petra'})
         |CREATE (s:$label2 {name: 'Steve'})
         |CREATE (p)-[:$type1]->(s)
      """.stripMargin)

    graph.inTx {
      executeWithRulePlanner(
        s"""
           |MATCH (p:$label1 {name: 'Petra'})
           |MATCH (s:$label2 {name: 'Steve'})
           |CREATE (c:$label3 {name: 'Craig'})
           |CREATE (p)-[:$type2]->(c)
           |CREATE (c)-[:$type3]->(s)
        """.stripMargin)
      val result: InternalExecutionResult = executeWithCostPlannerOnly(query)
      result.executionPlanDescription() should includeOperation(expectedLogicalPlan)
      f(result)
    }
  }

  def withRelationshipsModelAndTransaction(label1: String = "User",
                        label2: String = "User",
                        label3: String = "User",
                        type1: String = "KNOWS",
                        type2: String = "KNOWS",
                        type3: String = "KNOWS",
                        expectedLogicalPlan: String = "CountStoreRelationshipAggregation",
                        query: String, f: InternalExecutionResult => Unit): Unit = {
    withModelAndTransaction(label1, label2, label3, type1, type2, type3, expectedLogicalPlan, query, f)
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
