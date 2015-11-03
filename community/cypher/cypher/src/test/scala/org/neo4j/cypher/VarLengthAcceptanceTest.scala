package org.neo4j.cypher

import org.neo4j.cypher.internal.compiler.v2_2.commands.NoneInCollection
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.InternalExecutionResult
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.InternalPlanDescription.Arguments.LegacyExpression
import org.scalatest.matchers.{MatchResult, Matcher}

class VarLengthAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  private def makeTreeModel(relType: String) = {

    //TODO - ret nodes and put in results
    //root
    val n0 = createNode(Map("id" -> "A"))

    //parents
    val n00 = createNode(Map("id" -> "B-1"))
    val n01 = createNode(Map("id" -> "B-2"))

    //grandchildren
    val n000 = createNode(Map("id" -> "C-1"))
    val n001 = createNode(Map("id" -> "C-2"))

    val n010 = createNode(Map("id" -> "C-3"))
    val n011 = createNode(Map("id" -> "C-4"))

    //great-grandchildren
    val n0000 = createNode(Map("id" -> "D-1"))
    val n0001 = createNode(Map("id" -> "D-2"))

    val n0010 = createNode(Map("id" -> "D-3"))
    val n0011 = createNode(Map("id" -> "D-4"))

    val n0100 = createNode(Map("id" -> "D-5"))
    val n0101 = createNode(Map("id" -> "D-6"))

    val n0110 = createNode(Map("id" -> "D-7"))
    val n0111 = createNode(Map("id" -> "D-8"))

    //parents
    relate(n0, n00, relType)
    relate(n0, n01, relType)

    //grandchildren
    relate(n00, n000, relType)
    relate(n00, n001, relType)

    relate(n01, n010, relType)
    relate(n01, n011, relType)

    //great-grandchildren
    relate(n000, n0000, relType)
    relate(n000, n0001, relType)

    relate(n001, n0010, relType)
    relate(n001, n0011, relType)

    relate(n010, n0100, relType)
    relate(n010, n0101, relType)

    relate(n011, n0110, relType)
    relate(n011, n0111, relType)
  }

  //'.' indicates concatenation of relationship types

  test("should handle LIKES*") {
    //Given
    makeTreeModel("LIKES")
    //When
    val result = executeWithCostPlannerOnly("MATCH (p { id:'A' }) MATCH (p)-[:LIKES*]->(c) RETURN c")
    //Then
    result.length should be(14)
  }

  test("should handle LIKES*0") {
    //Given
    makeTreeModel("LIKES")
    //When
    val result = executeWithCostPlannerOnly("MATCH (p { id:'A' }) MATCH (p)-[:LIKES*0]->(c) RETURN c")
    //Then
    result.length should be(1)
  }

  test("should handle LIKES*0..2") {
    //Given
    makeTreeModel("LIKES")
    //When
    val result = executeWithCostPlannerOnly("MATCH (p { id:'A' }) MATCH (p)-[:LIKES*0..2]->(c) RETURN c")
    //Then
    result.length should be(7)
  }

  test("should handle LIKES*..") {
    //Given
    makeTreeModel("LIKES")
    //When
    val result = executeWithCostPlannerOnly("MATCH (p { id:'A' }) MATCH (p)-[:LIKES*..]->(c) RETURN c")
    //Then
    result.length should be(14)
  }

  test("should not accept LIKES..") {
    //Given
    makeTreeModel("LIKES")
    //Then
    a[SyntaxException] should be thrownBy {
      executeWithCostPlannerOnly("MATCH (p { id:'A' }) MATCH (p)-[:LIKES..]->(c) RETURN c")
    }
  }

  test("should handle LIKES*1") {
    //Given
    makeTreeModel("LIKES")
    //When
    val result = executeWithCostPlannerOnly("MATCH (p { id:'A' }) MATCH (p)-[:LIKES*1]->(c) RETURN c")
    //Then
    result.length should be(2)
  }

  test("should handle LIKES*1..2") {
    //Given
    makeTreeModel("LIKES")
    //When
    val result = executeWithCostPlannerOnly("MATCH (p { id:'A' }) MATCH (p)-[:LIKES*1..2]->(c) RETURN c")
    //Then
    result.length should be(6)
  }

  test("should handle LIKES*0..0") {
    //Given
    makeTreeModel("LIKES")
    //When
    val result = executeWithCostPlannerOnly("MATCH (p { id:'A' }) MATCH (p)-[:LIKES*0..0]->(c) RETURN c")
    //Then
    result.length should be(1)
  }

  test("should handle LIKES*1..1") {
    //Given
    makeTreeModel("LIKES")
    //When
    val result = executeWithCostPlannerOnly("MATCH (p { id:'A' }) MATCH (p)-[:LIKES*1..1]->(c) RETURN c")
    //Then
    result.length should be(2)
  }

  test("should handle LIKES*2..2") {
    //Given
    makeTreeModel("LIKES")
    //When
    val result = executeWithCostPlannerOnly("MATCH (p { id:'A' }) MATCH (p)-[:LIKES*2..2]->(c) RETURN c")
    //Then
    result.length should be(4)
  }

  test("should handle LIKES*2") {
    //Given
    makeTreeModel("LIKES")
    //When
    val result = executeWithCostPlannerOnly( "MATCH (p { id:'A' }) MATCH (p)-[:LIKES*2]->(c) RETURN c")
    //Then
    result.length should be(4)

  }

  ignore("should handle LIKES*..0") {
    //Given
    makeTreeModel("LIKES")
    //When
    val result = executeWithCostPlannerOnly( "MATCH (p { id:'A' }) MATCH (p)-[:LIKES*..0]->(c) RETURN c")
    //Then
    result.length should be(1)
  }

  test("should handle LIKES*..1") {
    //Given
    makeTreeModel("LIKES")
    //When
    val result = executeWithCostPlannerOnly( "MATCH (p { id:'A' }) MATCH (p)-[:LIKES*..1]->(c) RETURN c")
    //Then
    result.length should be(2)

  }

  test("should handle LIKES*..2") {
    //Given
    makeTreeModel("LIKES")
    //When
    val result = executeWithCostPlannerOnly( "MATCH (p { id:'A' }) MATCH (p)-[:LIKES*..2]->(c) RETURN c")
    //Then
    result.length should be(6)

  }

  test("should handle LIKES*0..") {
    //Given
    makeTreeModel("LIKES")
    //When
    val result = executeWithCostPlannerOnly( "MATCH (p { id:'A' }) MATCH (p)-[:LIKES*0..]->(c) RETURN c")
    //Then
    result.length should be(15)

  }

  test("should handle LIKES*1..") {
    //Given
    makeTreeModel("LIKES")
    //When
    val result = executeWithCostPlannerOnly( "MATCH (p { id:'A' }) MATCH (p)-[:LIKES*1..]->(c) RETURN c")
    //Then
    result.length should be(14)

  }

  test("should handle LIKES*2..") {
    //Given
    makeTreeModel("LIKES")
    //When
    val result = executeWithCostPlannerOnly( "MATCH (p { id:'A' }) MATCH (p)-[:LIKES*2..]->(c) RETURN c")
    //Then
    result.length should be(12)

  }

  test("should handle LIKES*0.LIKES") {
    //Given
    makeTreeModel("LIKES")
    //When
    val result = executeWithCostPlannerOnly("MATCH (p { id:'A' }) MATCH (p)-[:LIKES*0]->()-[r:LIKES]->(c) RETURN c")
    //Then
    result should haveNoneRelFilter
    result.length should be(2)
  }

  test("should handle LIKES.LIKES*0") {
    //Given
    makeTreeModel("LIKES")
    //When
    val result = executeWithCostPlannerOnly("MATCH (p { id:'A' }) MATCH (p)-[:LIKES]->()-[r:LIKES*0]->(c) RETURN c")
    //Then
    result should haveNoneRelFilter
    result.length should be(2)
  }

  test("should handle LIKES*1.LIKES") {
    //Given
    makeTreeModel("LIKES")
    //When
    val result = executeWithCostPlannerOnly("MATCH (p { id:'A' }) MATCH (p)-[:LIKES*1]->()-[r:LIKES]->(c) RETURN c")
    //Then
    result should haveNoneRelFilter
    result.length should be(4)
  }

  test("should handle LIKES.LIKES*1") {
    //Given
    makeTreeModel("LIKES")
    //When
    val result = executeWithCostPlannerOnly("MATCH (p { id:'A' }) MATCH (p)-[:LIKES]->()-[r:LIKES*1]->(c) RETURN c")
    //Then
    result should haveNoneRelFilter
    result.length should be(4)
  }

  test("should handle LIKES*2.LIKES") {
    //Given
    makeTreeModel("LIKES")
    //When
    val result = executeWithCostPlannerOnly("MATCH (p { id:'A' }) MATCH (p)-[:LIKES*2]->()-[r:LIKES]->(c) RETURN c")
    //Then
    result should haveNoneRelFilter
    result.length should be(8)
  }

  test("should handle LIKES.LIKES*2") {
    //Given
    makeTreeModel("LIKES")
    //When
    val result = executeWithCostPlannerOnly("MATCH (p { id:'A' }) MATCH (p)-[:LIKES]->()-[r:LIKES*2]->(c) RETURN c")
    //Then
    result should haveNoneRelFilter
    result.length should be(8)
  }


  def haveNoneRelFilter(): Matcher[InternalExecutionResult] = new Matcher[InternalExecutionResult] {
    override def apply(result: InternalExecutionResult): MatchResult = {
      val plan: InternalPlanDescription = result.executionPlanDescription()
      val res = plan.find("Filter").exists { p =>
        p.arguments.exists {
          case LegacyExpression(NoneInCollection(_, _, _)) => true
          case _ => false
        }
      }
      MatchResult(
        matches = res,
        rawFailureMessage = s"Plan should have Filter with NONE comprehension:\n$plan",
        rawNegatedFailureMessage = s"Plan should not have Filter with NONE comprehension:\n$plan")
    }
  }

}
