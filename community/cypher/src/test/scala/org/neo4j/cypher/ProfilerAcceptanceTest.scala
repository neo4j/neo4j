package org.neo4j.cypher

import org.scalatest.Assertions
import org.junit.Test
import org.junit.Assert._

class ProfilerAcceptanceTest extends ExecutionEngineHelper with Assertions {
  @Test
  def unfinished_profiler_complains() {
    //GIVEN
    val engine = new ExecutionEngine(graph)
    val result: ExecutionResult = engine.profile("START n=node(0) RETURN n")

    //WHEN THEN
    intercept[ProfilerStatisticsNotReadyException](result.executionPlanDescription())
  }

  @Test
  def profile_shit() {
    //GIVEN
    val engine = new ExecutionEngine(graph)
    createNode("foo" -> "bar")
    val result: ExecutionResult = engine.profile("START n=node(1) RETURN n")
    materialise(result)

    //WHEN THEN
    assertTrue(result.executionPlanDescription().contains("rows=1"))
  }

  private def materialise(result: ExecutionResult) {
    result.size
  }
}
