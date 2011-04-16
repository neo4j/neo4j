package org.neo4j.lab.cypher

import org.junit.Assert._

import org.junit.matchers.JUnitMatchers._
import org.junit.{After, Before, Test}
import org.neo4j.kernel.{AbstractGraphDatabase, ImpermanentGraphDatabase}
import org.neo4j.graphdb.Node

class ExecutionEngineTest {
  var graph:AbstractGraphDatabase = null
  var engine:ExecutionEngine = null

  @Before def init() {
    graph = new ImpermanentGraphDatabase()
    engine = new ExecutionEngine(graph)
  }

  @After def cleanUp() {
    graph.shutdown()
  }

  @Test def shouldGetReferenceNode() {
    //    FROM node = NODE(0)
    //    SELECT node

    val query = Query(
      Select(VariableOutput("node")),
      List(VariableAssignment("node", NodeById(List[Long](0))))
    )

    val result = execute(query)
    assertEquals(List(graph.getReferenceNode), result)
  }

  @Test def shouldGetOtherNode() {
    //    FROM node = NODE(0)
    //    SELECT node

    val node:Node = createNode()

    val query = Query(
      Select(VariableOutput("node")),
      List(VariableAssignment("node", NodeById(List(node.getId))))
    )

    val result = execute(query)
    assertEquals(List(node), result)
  }

  def execute(query: Query) = {
    val result = engine.execute(query)
    println(query)
    result
  }

  def createNode():Node = {
    val tx = graph.beginTx
    val node = graph.createNode()
    tx.success()
    tx.finish()
    node
  }
}