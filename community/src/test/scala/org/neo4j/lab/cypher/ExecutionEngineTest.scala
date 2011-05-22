package org.neo4j.lab.cypher

import commands._
import org.junit.Assert._

import org.neo4j.kernel.{AbstractGraphDatabase, ImpermanentGraphDatabase}
import java.lang.String
import org.junit.{Ignore, After, Before, Test}
import org.neo4j.graphdb.{Relationship, Direction, DynamicRelationshipType, Node}

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
    val query = Query(
      Select(EntityOutput("node")),
      Start(NodeById("node", 0))
    )

    val result = execute(query)
    assertEquals(List(refNode), result.columnAs[Node]("node").toList)
  }

  @Test def shouldGetOtherNode() {
    val node: Node = createNode()

    val query = Query(
      Select(EntityOutput("node")),
      Start(NodeById("node", node.getId))
    )

    val result = execute(query)
    assertEquals(List(node), result.columnAs[Node]("node").toList)
  }

  @Ignore("graph-matching doesn't support using relationships as start points. revisit when it does.")
  @Test def shouldGetRelationship() {
    val node: Node = createNode()
    val rel: Relationship = relate(refNode, node, "yo")

    val query = Query(
      Select(EntityOutput("rel")),
      Start(RelationshipById("rel", rel.getId))
    )

    val result = execute(query)
    assertEquals(List(rel), result.columnAs[Relationship]("rel").toList)
  }

  @Test def shouldGetTwoNodes() {
    val node: Node = createNode()

    val query = Query(
      Select(EntityOutput("node")),
      Start(NodeById("node", refNode.getId, node.getId))
    )

    val result = execute(query)
    assertEquals(List(refNode, node), result.columnAs[Node]("node").toList)
  }

  @Test def shouldGetNodeProperty() {
    val name = "Andres"
    val node: Node = createNode(Map("name" -> name))

    val query = Query(
      Select(PropertyOutput("node", "name")),
      Start(NodeById("node", node.getId))
    )

    val result = execute(query)
    val list = result.columnAs[String]("node.name").toList
    assertEquals(List(name), list)
  }

  @Test def shouldFilterOutBasedOnNodePropName() {
    val name = "Andres"
    val start: Node = createNode()
    val a1: Node = createNode(Map("name" -> "Someone Else"))
    val a2: Node = createNode(Map("name" -> name))
    relate(start, a1, "x")
    relate(start, a2, "x")

    val query = Query(
      Select(EntityOutput("a")),
      Start(NodeById("start", start.getId)),
      Match(RelatedTo("start", "a", None, Some("x"), Direction.BOTH)),
      Where(StringEquals("a", "name", name))
    )

    val result = execute(query)
    assertEquals(List(a2), result.columnAs[Node]("a").toList)
  }

  @Test def shouldFilterBasedOnRelPropName() {
    val start: Node = createNode()
    val a: Node = createNode()
    val b: Node = createNode()
    relate(start, a, "KNOWS", Map("name" -> "monkey"))
    relate(start, b, "KNOWS")

    val query = Query(
      Select(EntityOutput("a")),
      Start(NodeById("start", start.getId)),
      Match(RelatedTo("start", "a", Some("r"), Some("KNOWS"), Direction.BOTH)),
      Where(StringEquals("r", "name", "monkey"))
    )

    val result = execute(query)
    assertEquals(List(a), result.columnAs[Node]("a").toList)
  }

  @Ignore("Maybe later")
  @Test def shouldOutputTheCartesianProductOfTwoNodes() {
    val n1: Node = createNode()
    val n2: Node = createNode()

    val query = Query(
      Select(EntityOutput("n1"), EntityOutput("n2")),
      Start(
        NodeById("n1", n1.getId),
        NodeById("n2", n2.getId)
      )
    )

    val result = execute(query)

    assertEquals(List(Map("n1" -> n1, "n2" -> n2)), result.toList)
  }

  @Test def shouldGetNeighbours() {
    val n1: Node = createNode()
    val n2: Node = createNode()
    relate(n1, n2, "KNOWS")

    val query = Query(
      Select(EntityOutput("n1"), EntityOutput("n2")),
      Start(NodeById("n1", n1.getId)),
      Match(RelatedTo("n1", "n2", None, Some("KNOWS"), Direction.OUTGOING))
    )

    val result = execute(query)

    assertEquals(List(Map("n1" -> n1, "n2" -> n2)), result.toList)
  }

  @Test def shouldGetTwoRelatedNodes() {
    val n1: Node = createNode()
    val n2: Node = createNode()
    val n3: Node = createNode()
    relate(n1, n2, "KNOWS")
    relate(n1, n3, "KNOWS")

    val query = Query(
      Select(EntityOutput("x")),
      Start(NodeById("start", n1.getId)),
      Match(RelatedTo("start", "x", None, Some("KNOWS"), Direction.OUTGOING))
    )

    val result = execute(query)

    assertEquals(List(Map("x" -> n2), Map("x" -> n3)), result.toList)
  }

  @Test def toStringTest() {
    val n1: Node = createNode()
    val n2: Node = createNode()
    val n3: Node = createNode()
    relate(n1, n2, "KNOWS")
    relate(n1, n3, "KNOWS")

    val query = Query(
      Select(EntityOutput("x"), EntityOutput("start")),
      Start(NodeById("start", n1.getId)),
      Match(RelatedTo("start", "x", None, Some("KNOWS"), Direction.OUTGOING))
    )

    val result = execute(query)

    println(result)
  }

  @Test def shouldGetRelatedToRelatedTo() {
    val n1: Node = createNode()
    val n2: Node = createNode()
    val n3: Node = createNode()
    relate(n1, n2, "KNOWS")
    relate(n2, n3, "FRIEND")

    val query = Query(
      Select(EntityOutput("b")),
      Start(NodeById("start", n1.getId)),
      Match(
        RelatedTo("start", "a", None, Some("KNOWS"), Direction.OUTGOING),
        RelatedTo("a", "b", None, Some("FRIEND"), Direction.OUTGOING))
    )

    val result = execute(query)

    assertEquals(List(Map("b" -> n3)), result.toList)
  }

  @Test def shouldFindNodesByIndex() {
    val n: Node = createNode()
    val idxName: String = "idxName"
    val key: String = "key"
    val value: String = "andres"
    indexNode(n, idxName, key, value)

    val query = Query(
      Select(EntityOutput("n")),
      Start(NodeByIndex("n", idxName, key, value))
    )

    val result = execute(query)

    assertEquals(List(Map("n" -> n)), result.toList)
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


  def relate(n1: Node, n2: Node, relType: String, props: Map[String, Any] = Map()):Relationship = {
    inTx(() => {
      val r = n1.createRelationshipTo(n2, DynamicRelationshipType.withName(relType))

      props.foreach((kv) => r.setProperty(kv._1, kv._2))
      r
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