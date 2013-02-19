package org.neo4j.cypher

import org.scalatest.Assertions
import org.junit.Test


class ProfilerAcceptanceTest extends ExecutionEngineHelper with Assertions {
  @Test
  def unfinished_profiler_complains() {
    //GIVEN
    val engine = new ExecutionEngine(graph)
    val result: ExecutionResult = engine.profile("START n=node(0) RETURN n")

    //WHEN THEN
    intercept[ProfilerStatisticsNotReadyException](result.executionPlanDescription())
  }
}