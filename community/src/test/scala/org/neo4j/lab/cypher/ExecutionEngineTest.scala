package org.neo4j.lab.cypher

import commands._
import org.junit.Assert._

import org.junit.{After, Before, Test}
import org.neo4j.kernel.{AbstractGraphDatabase, ImpermanentGraphDatabase}
import org.neo4j.graphdb.Node
import pipes.Pipe
import projections.Projection

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
//    assertEquals("node", result.columnNames.head)
    assertEquals(List(refNode), result.columnAs[Node]("node").toList )
  }

  @Test def shouldGetOtherNode() {
    //    FROM node = NODE(1)
    //    SELECT node

    val node: Node = createNode()

    val query = Query(
      Select(NodeOutput("node")),
      List(VariableAssignment("node", NodeById(List(node.getId))))
    )

    val result = execute(query)
    assertEquals(List(node), result.columnAs[Node]("node").toList)
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
    assertEquals(List(refNode, node), result.columnAs[Node]("node").toList)
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

    val result = execute(query)
    var list = result.columnAs[String]("node.name").toList
    assertEquals(List(name), list)
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

    val result = execute(query)
    assertEquals(List(node1), result.columnAs[Node]("node").toList)
  }

  @Test def shouldOutputTheCartesianProductOfTwoNodes() {
    //    FROM n1 = NODE(1), n2 = NODE(2)
    //    SELECT n1, n2

    val node1: Node = createNode()
    val node2: Node = createNode()

    val query = Query(
      Select(NodeOutput("n1"), NodeOutput("n2")),
      List(
        VariableAssignment("n1", NodeById(List(node1.getId))),
        VariableAssignment("n2", NodeById(List(node2.getId)))
      )
    )

    val result = execute(query)
    val n1 = result.columnAs[Node]("n1").toList
    val n2 = result.columnAs[Node]("n2").toList
    assertEquals(List(node1), n1)
    assertEquals(List(node2), n2)
  }

  def execute(query: Query) = {
    val result = engine.execute(query)
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