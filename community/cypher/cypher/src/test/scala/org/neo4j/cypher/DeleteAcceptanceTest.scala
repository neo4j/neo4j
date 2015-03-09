package org.neo4j.cypher

class DeleteAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  test("stuff") {
    val node = createNode()
    val other = createNode()
    relate(node, other)
    relate(other, createNode)
    val res = execute("""EXPLAIN MATCH n OPTIONAL MATCH n-[r]-() WHERE (ID(n) = {ID_n}) DELETE n, r""", "ID_n"-> node.getId)

    println(res.dumpToString())
    println(res.executionPlanDescription())
  }
}
