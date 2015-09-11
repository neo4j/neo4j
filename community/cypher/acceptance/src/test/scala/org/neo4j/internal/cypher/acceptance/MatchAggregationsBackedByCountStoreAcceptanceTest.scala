package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription
import org.neo4j.cypher.{NewPlannerTestSupport, QueryStatisticsTestSupport, ExecutionEngineFunSuite}
import org.neo4j.graphdb.Transaction
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
