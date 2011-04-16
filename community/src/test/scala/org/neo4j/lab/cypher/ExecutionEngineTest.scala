package org.neo4j.lab.cypher

import org.junit.Assert._

import org.junit.{After, Before, Test}
import org.neo4j.kernel.{AbstractGraphDatabase, ImpermanentGraphDatabase}
import org.neo4j.graphdb.Node

class ExecutionEngineTest {
  var graph: AbstractGraphDatabase = null
  var engine: ExecutionEngine = null
  var refNode: Node = null

  @Before def init() {
    graph = new ImpermanentGraphDatabase()
    engine = new ExecutionEngine(graph)
    refNode = graph.getReferenceNode
  }

  @After def cleanUp() {
    graph.shutdown()
  }

  @Test def shouldGetReferenceNode() {
    //    FROM node = NODE(0)
    //    SELECT node

    val query = Query(
      Select(NodeOutput("node")),
      List(VariableAssignment("node", NodeById(List[Long](0))))
    )

    val result = execute(query)
    assertEquals(List(refNode), result)
  }

  @Test def shouldGetOtherNode() {
    //    FROM node = NODE(1)
    //    SELECT node

    val node: Node = createNode()

    val query = Query(
      Select(NodeOutput("node")),
      List(VariableAssignment("node", NodeById(List(node.getId))))
    )

    val result = execute[Node](query)
    assertEquals(List(node), result)
  }

  @Test def shouldGetTwoNodes() {
    //    FROM node = NODE(0, 17)
    //    SELECT node

    val node: Node = createNode()

    val query = Query(
      Select(NodeOutput("node")),
      List(VariableAssignment("node", NodeById(List(refNode.getId, node.getId))))
    )

    val result = execute(query)
    assertEquals(List(refNode, node), result)
  }

  @Test def shouldGetNodeProperty() {
    //    FROM node = NODE(17)
    //    SELECT node.name

    val name = "Andres"
    val node: Node = createNode(Map("name" -> name))

    val query = Query(
      Select(NodePropertyOutput("node", "name")),
      List(VariableAssignment("node", NodeById(List(node.getId))))
    )

    val result = execute[String](query)
    assertEquals(List(name), result)
  }

  @Test def shouldFilterOutBasedOnName() {
    //    FROM node = NODE(1,2)
    //    WHERE node.name = "Andres"
    //    SELECT node

    val name = "Andres"
    val node1: Node = createNode(Map("name" -> name))
    val node2: Node = createNode(Map("name" -> "Someone Else"))

    val query = Query(
      Select(NodeOutput("node")),
      List(VariableAssignment("node", NodeById(List(node1.getId, node2.getId)))),
      Some(Where(StringEquals("node", "name", name)))
    )

    val result = execute[String](query)
    assertEquals(List(node1), result)
  }

  def execute[T](query: Query) = {
    val result = engine.execute[T](query)
    println(query)
    result
  }

  def createNode(): Node = createNode(Map[String, Any]())

  def createNode(props: Map[String, Any]): Node = {
    val tx = graph.beginTx
    val node = graph.createNode()

    props.foreach((kv) => node.setProperty(kv._1, kv._2))

    tx.success()
    tx.finish()
    node
  }
}