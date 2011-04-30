package org.neo4j.lab.cypher

import commands._
import org.junit.Assert._

import org.junit.{After, Before, Test}
import org.neo4j.kernel.{AbstractGraphDatabase, ImpermanentGraphDatabase}
import org.neo4j.graphdb.{Direction, DynamicRelationshipType, Node}
import java.lang.String

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
      List(NodeById("node", List[Long](0)))
    )

    val result = execute(query)
    assertEquals(List(refNode), result.columnAs[Node]("node").toList)
  }

  @Test def shouldGetOtherNode() {
    //    FROM node = NODE(1)
    //    SELECT node

    val node: Node = createNode()

    val query = Query(
      Select(NodeOutput("node")),
      List(NodeById("node", List(node.getId)))
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
      List(NodeById("node", List(refNode.getId, node.getId)))
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
      List(NodeById("node", List(node.getId)))
    )

    val result = execute(query)
    val list = result.columnAs[String]("node.name").toList
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
      List(NodeById("node", List(node1.getId, node2.getId))),
      Some(Where(StringEquals("node", "name", name)))
    )

    val result = execute(query)
    assertEquals(List(node1), result.columnAs[Node]("node").toList)
  }

  @Test def shouldOutputTheCartesianProductOfTwoNodes() {
    //    FROM n1 = NODE(1), n2 = NODE(2)
    //    SELECT n1, n2

    val n1: Node = createNode()
    val n2: Node = createNode()

    val query = Query(
      Select(NodeOutput("n1"), NodeOutput("n2")),
      List(
        NodeById("n1", List(n1.getId)),
        NodeById("n2", List(n2.getId))
      )
    )

    val result = execute(query)

    assertEquals(List(Map("n1" -> n1, "n2" -> n2)), result.toList)
  }

  @Test def shouldGetNeighbours() {
    //    FROM n1 = NODE(1),
    //      n1 -KNOWS-> n2
    //    SELECT n1, n2

    val n1: Node = createNode()
    val n2: Node = createNode()
    relate(n1, n2, "KNOWS")

    val query = Query(
      Select(NodeOutput("n1"), NodeOutput("n2")),
      List(
        NodeById("n1", List(n1.getId)),
        RelatedTo("n1", "n2", "x", "KNOWS", Direction.OUTGOING)
      ))

    val result = execute(query)

    assertEquals(List(Map("n1" -> n1, "n2" -> n2)), result.toList)
  }

  @Test def shouldGetTwoRelatedNodes() {
    //    FROM start = NODE(1),
    //      start -KNOWS-> x
    //    SELECT x

    val n1: Node = createNode()
    val n2: Node = createNode()
    val n3: Node = createNode()
    relate(n1, n2, "KNOWS")
    relate(n1, n3, "KNOWS")

    val query = Query(
      Select(NodeOutput("x")),
      List(
        NodeById("start", List(n1.getId)),
        RelatedTo("start", "x", "a", "KNOWS", Direction.OUTGOING)
      ))

    val result = execute(query)

    assertEquals(List(Map("x" -> n2), Map("x" -> n3)), result.toList)
  }

  @Test def shouldGetRelatedToRelatedTo() {
    //    FROM start = NODE(1),
    //      start -KNOWS-> a,
    //      a -FRIEND- b
    //    SELECT n3

    val n1: Node = createNode()
    val n2: Node = createNode()
    val n3: Node = createNode()
    relate(n1, n2, "KNOWS")
    relate(n2, n3, "FRIEND")

    val query = Query(
      Select(NodeOutput("b")),
      List(
        NodeById("start", List(n1.getId)),
        RelatedTo("start", "a", "r", "KNOWS", Direction.OUTGOING),
        RelatedTo("a", "b", "foo", "FRIEND", Direction.OUTGOING)
      ))

    val result = execute(query)

    assertEquals(List(Map("b" -> n3)), result.toList)
  }

  @Test def shouldFindNodesByIndex() {
    //    FROM n = NODE("idxName", "key", "andres"),
    //    SELECT n

    val n: Node = createNode()
    val idxName: String = "idxName"
    val key: String = "key"
    val value: String = "andres"
    indexNode(n, idxName, key, value)

    val query = Query(
      Select(NodeOutput("n")),
      List(
        NodeByIndex("n", idxName, key, value)
      ))

    val result = execute(query)

    assertEquals(List(Map("n" -> n)), result.toList)
  }

  @Test def shouldBeAbleToFilterOnRelationProps() {
    //    FROM me = NODE(1),
    //      (me) -[r,'SHARE_OWNER']- (company)
    //    WHERE r.amount > 1000
    //    SELECT company.name

    val me = createNode()
    val msft = createNode(Map("name" -> "Microsoft"))
    val orcl = createNode(Map("name" -> "Oracle"))
    relate(me, msft, "SHARE_OWNER", Map("amount" -> 500f))
    relate(me, orcl, "SHARE_OWNER", Map("amount" -> 1500f))

    val query = Query(
      Select(NodePropertyOutput("company", "name")),
      List(
        NodeById("me", List(me.getId)),
        RelatedTo("me", "company", "r", "SHARE_OWNER", Direction.OUTGOING)
      ),
      Some(Where(NumberLargerThan("r", "amount", 1000f)))
    )

    val result = execute(query)

    assertEquals(List(Map("company.name" -> "Oracle")), result.toList)
  }


  def execute(query: Query) = {
    val result = engine.execute(query)
    println(query)
    result
  }

  def indexNode(n: Node, idxName: String, key: String, value: String) {
    inTx(() => n.getGraphDatabase.index.forNodes(idxName).add(n, key, value))
  }

  def createNode(): Node = createNode(Map[String, Any]())

  def inTx[T](f: () => T): T = {
    val tx = graph.beginTx

    val result = f.apply()

    tx.success()
    tx.finish()

    result
  }


  def relate(n1: Node, n2: Node, relType: String, props: Map[String, Any] = Map()) {
    inTx(() => {
      val r = n1.createRelationshipTo(n2, DynamicRelationshipType.withName(relType))

      props.foreach((kv) => r.setProperty(kv._1, kv._2))
    })
  }

  def createNode(props: Map[String, Any]): Node = {
    inTx(() => {
      val node = graph.createNode()

      props.foreach((kv) => node.setProperty(kv._1, kv._2))
      node
    }).asInstanceOf[Node]
  }
}