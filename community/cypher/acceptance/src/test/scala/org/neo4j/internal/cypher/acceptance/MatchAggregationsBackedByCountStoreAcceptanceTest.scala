package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription
import org.neo4j.cypher.{NewPlannerTestSupport, QueryStatisticsTestSupport, ExecutionEngineFunSuite}
import org.scalatest.matchers.{MatchResult, Matcher}

class MatchAggregationsBackedByCountStoreAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  test("counts nodes using count store") {
    executeWithRulePlanner(
      """
        |CREATE (p:User {name: 'Petra'})
        |CREATE (s:User {name: 'Steve'})
        |CREATE (p)-[:KNOWS]->(s)
      """.stripMargin)
    val result = executeWithCostPlannerOnly("MATCH (n) RETURN count(n)")
    result.executionPlanDescription() should includeOperation("CountStoreNodeAggregation")
    result.columnAs("count(n)").toSet[Int] should equal(Set(2))
  }

  test("counts labeled nodes using count store") {
    executeWithRulePlanner(
      """
        |CREATE (p:Admin {name: 'Petra'})
        |CREATE (s:User {name: 'Steve'})
        |CREATE (p)-[:KNOWS]->(s)
      """.stripMargin)
    val result = executeWithCostPlannerOnly("MATCH (n:User) RETURN count(n)")
    result.executionPlanDescription() should includeOperation("CountStoreNodeAggregation")
    result.columnAs("count(n)").toSet[Int] should equal(Set(1))
  }

  test("counts relationships using count store") {
    executeWithRulePlanner(
      """
        |CREATE (p:User {name: 'Petra'})
        |CREATE (s:User {name: 'Steve'})
        |CREATE (p)-[:KNOWS]->(s)
      """.stripMargin)
    val result = executeWithCostPlannerOnly("MATCH ()-[r]->() RETURN count(r)")
    result.executionPlanDescription() should includeOperation("CountStoreRelationshipAggregation")
    result.columnAs("count(r)").toSet[Int] should equal(Set(1))
  }

  test("counts nodes using count store considering transaction state") {
    executeWithRulePlanner(
      """
        |CREATE (p:User {name: 'Petra'})
        |CREATE (s:User {name: 'Steve'})
        |CREATE (p)-[:KNOWS]->(s)
      """.stripMargin)

    graph.inTx {
      executeWithRulePlanner(
        """
          |MATCH (p:User {name: 'Petra'})
          |MATCH (s:User {name: 'Steve'})
          |CREATE (c:User {name: 'Craig'})
          |CREATE (p)-[:KNOWS]->(c)
          |CREATE (c)-[:KNOWS]->(s)
        """.stripMargin)

      val result = executeWithCostPlannerOnly("MATCH (n) RETURN count(n)")
      result.executionPlanDescription() should includeOperation("CountStoreNodeAggregation")
      result.columnAs("count(n)").toSet[Int] should equal(Set(3))
    }
  }

  test("counts labeled nodes using count store considering transaction state") {
    executeWithRulePlanner(
      """
        |CREATE (p:Admin {name: 'Petra'})
        |CREATE (s:User {name: 'Steve'})
        |CREATE (p)-[:KNOWS]->(s)
      """.stripMargin)

    graph.inTx {
      executeWithRulePlanner(
        """
          |MATCH (p:Admin {name: 'Petra'})
          |MATCH (s:User {name: 'Steve'})
          |CREATE (c:User {name: 'Craig'})
          |CREATE (p)-[:KNOWS]->(c)
          |CREATE (c)-[:KNOWS]->(s)
        """.stripMargin)

      Map("User" -> 2, "Admin" -> 1).collect {
        case (labelName, expectedCount) =>
          val result = executeWithCostPlannerOnly(s"MATCH (n:$labelName) RETURN count(n)")
          println(result.executionPlanDescription())
          result.executionPlanDescription() should includeOperation("CountStoreNodeAggregation")
          result.columnAs("count(n)").toSet[Int] should equal(Set(expectedCount))
      }
    }
  }

  test("counts labeled nodes using count store considering transaction state containing newly created label") {
    executeWithRulePlanner(
      """
        |CREATE (p:User {name: 'Petra'})
        |CREATE (s:User {name: 'Steve'})
        |CREATE (p)-[:KNOWS]->(s)
      """.stripMargin)

    graph.inTx {
      executeWithRulePlanner(
        """
          |MATCH (p:User {name: 'Petra'})
          |MATCH (s:User {name: 'Steve'})
          |CREATE (c:Admin {name: 'Craig'})
          |CREATE (p)-[:KNOWS]->(c)
          |CREATE (c)-[:KNOWS]->(s)
        """.stripMargin)

      Map("User" -> 2, "Admin" -> 1).collect {
        case (labelName, expectedCount) =>
          val result = executeWithCostPlannerOnly(s"MATCH (n:$labelName) RETURN count(n)")
          println(result.executionPlanDescription())
          result.executionPlanDescription() should includeOperation("CountStoreNodeAggregation")
          result.columnAs("count(n)").toSet[Int] should equal(Set(expectedCount))
      }
    }
  }

  test("counts relationships using count store considering transaction state") {
    executeWithRulePlanner(
      """
        |CREATE (p:User {name: 'Petra'})
        |CREATE (s:User {name: 'Steve'})
        |CREATE (p)-[:KNOWS]->(s)
      """.stripMargin)

    graph.inTx {
      executeWithRulePlanner(
        """
          |MATCH (p:User {name: 'Petra'})
          |MATCH (s:User {name: 'Steve'})
          |CREATE (c:User {name: 'Craig'})
          |CREATE (p)-[:KNOWS]->(c)
          |CREATE (c)-[:KNOWS]->(s)
        """.stripMargin)

      val result = executeWithCostPlannerOnly("MATCH ()-[r]->() RETURN count(r)")
      println(result.executionPlanDescription())
      result.executionPlanDescription() should includeOperation("CountStoreRelationshipAggregation")
      result.columnAs("count(r)").toSet[Int] should equal(Set(3))
    }
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
