/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v2_3

import java.lang.{Iterable => JIterable}
import java.util
import java.util.{Iterator => JIterator}

import org.junit.Assert._
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.collection.primitive.PrimitiveLongCollections
import org.neo4j.cypher._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Identifier, Literal}
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.{GreaterThan, True}
import org.neo4j.cypher.internal.compiler.v2_3.pipes._
import org.neo4j.cypher.internal.compiler.v2_3.pipes.matching._
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.Argument
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.symbols.CTInteger
import org.neo4j.cypher.internal.spi.v2_3.MonoDirectionalTraversalMatcher
import org.neo4j.cypher.internal.{ExecutionPlan, CypherCompiler => Compiler}
import org.neo4j.graphdb.Traverser.Order
import org.neo4j.graphdb._
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.api.{ReadOperations, Statement}
import org.neo4j.kernel.impl.api.OperationsFacade
import org.neo4j.kernel.impl.core.{NodeManager, NodeProxy, ThreadToStatementContextBridge}
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore
import org.neo4j.tooling.GlobalGraphOperations

import scala.collection.JavaConverters._

// TODO: this test is horribly broken, it relies on mocking the core API for verification, but the internals don't use the core API
class LazyTest extends ExecutionEngineFunSuite {

  var aNode: Node = null
  var bNode: Node = null
  var cNode: Node = null

  private implicit val pipeMonitor = mock[PipeMonitor]

  override protected def initTest() {
    super.initTest()

    aNode = createNode()
    bNode = createNode()
    cNode = createNode()
    relate(aNode, bNode)
    relate(aNode, cNode)
    relate(aNode, createNode())
    relate(aNode, createNode())
    relate(aNode, createNode())
    relate(aNode, createNode())
    relate(aNode, createNode())
    relate(aNode, createNode())
    relate(aNode, createNode())
    relate(aNode, createNode())
  }

  test("get first relationship does not iterate through all") {
    //Given:
    val limiter = new Limiter(1)
    val monitoredNode = new MonitoredNode(aNode, limiter.monitor)

    //When:
    graph.inTx(monitoredNode.getRelationships(Direction.OUTGOING).iterator().next())

    //Then does not throw exception
  }

  test("traversal matcher is lazy") {
    //Given:
    val tx = graph.beginTx()
    val limiter = new Limiter(2)
    val monitoredNode = new MonitoredNode(aNode, limiter.monitor)

    val step = SingleStep(0, Seq(), SemanticDirection.OUTGOING, None, True(), True())
    val producer = EntityProducer[Node]("test", mock[Argument]) { (ctx, state) => Iterator(monitoredNode) }
    val matcher = new MonoDirectionalTraversalMatcher(step, producer)
    val ctx = ExecutionContext().newWith("a" -> monitoredNode)

    //When:
    val iter = matcher.findMatchingPaths(QueryStateHelper.queryStateFrom(graph, tx), ctx)

    //Then:
    assert(limiter.count === 0)

    //Also then, does not throw exception
    iter.next()
    tx.close()
  }

  test("execution of query is lazy") {
    //Given:
    val limiter = new Limiter(3)
    val monitoredNode = new MonitoredNode(aNode, limiter.monitor)

    val engine = new ExecutionEngine(graph)

    //When:
    val iter: ExecutionResult = engine.execute("match n-->x where n = {foo} return x", Map("foo" -> monitoredNode))

    //Then:
    assert(limiter.count === 0)

    //Also then does not step over the limit
    iter.next()
    iter.close()
  }

  test("distinct is lazy") {
    //Given:
    val a = createNode(Map("name" -> "Andres"))
    val b = createNode(Map("name" -> "Jake"))

    val c = mock[Node]

    // Because we use a prefetching iterator, it will cache one more result than we have pulled
    // if it doesn't it will try to get the name property from the mock c and fail

    when(c.hasProperty("name")).thenThrow(new RuntimeException("Distinct was not lazy!"))

    val engine = new ExecutionEngine(graph)

    //When:
    val iter = engine.execute("match n where n IN {foo} return distinct n.name", Map("foo" -> Seq(a, b, c)))

    //Then, no Runtime exception is thrown
    iter.next()
    iter.close()
  }

  test("union is lazy") {
    //Given:
    val a = createNode(Map("name" -> "Andres"))
    val b = createNode(Map("name" -> "Jake"))

    val c = mock[Node]

    // Because we use a pre-fetching iterator, it will cache one more result than we have pulled
    when(c.hasProperty("name")).thenThrow(new RuntimeException("Union was not lazy!"))

    val engine = new ExecutionEngine(graph)

    //When:
    val iter = engine.execute("match n where n = {a} return n.name UNION ALL match n where n IN {b} return n.name", Map("a" -> a, "b" -> Seq(b, c)))

    //Then, no Runtime exception is thrown
    iter.next()
    iter.close()
  }

  ignore("execution of query with mutation is eager") {
    //Given:
    var touched = false
    val monitoredNode = new MonitoredNode(aNode, () => touched = true)

    val engine = new ExecutionEngine(graph)

    //When:
    val iter: ExecutionResult = engine.execute("start n=node({foo}) match n-->x create n-[:FOO]->x", Map("foo" -> monitoredNode))

    //Then:
    assert(touched, "Query should have been executed")
  }

  test("graph global queries are lazy") {
    //Given:
    val counter = new CountingJIterator()

    val fakeGraph = mock[GraphDatabaseAPI]
    val tx = mock[Transaction]
    val nodeManager = mock[NodeManager]
    val dependencies = mock[DependencyResolver]
    val bridge = mock[ThreadToStatementContextBridge]
    val monitors = new org.neo4j.kernel.monitoring.Monitors()

    val fakeDataStatement = mock[OperationsFacade]
    val fakeReadStatement = mock[ReadOperations]
    val fakeStatement = mock[Statement]
    val idStore = mock[TransactionIdStore]

    when(idStore.getLastCommittedTransactionId).thenReturn(0)
    when(nodeManager.newNodeProxyById(anyLong())).thenAnswer( new Answer[NodeProxy] {
      def answer( invocation: InvocationOnMock ): NodeProxy = new NodeProxy( null, invocation.getArguments( )( 0 ).asInstanceOf[Long] )
    })
    when(bridge.get()).thenReturn(fakeStatement)
    when(fakeStatement.readOperations()).thenReturn(fakeReadStatement)
    when(fakeStatement.dataWriteOperations()).thenReturn(fakeDataStatement)
    when(fakeGraph.getDependencyResolver).thenReturn(dependencies)
    when(dependencies.resolveDependency(classOf[ThreadToStatementContextBridge])).thenReturn(bridge)
    when(dependencies.resolveDependency(classOf[NodeManager])).thenReturn(nodeManager)
    when(dependencies.resolveDependency(classOf[TransactionIdStore])).thenReturn(idStore)
    when(dependencies.resolveDependency(classOf[org.neo4j.kernel.monitoring.Monitors])).thenReturn(monitors)
    when(fakeGraph.beginTx()).thenReturn(tx)
    val nodesIterator = PrimitiveLongCollections.iterator( 0L, 1L, 2L, 3L, 4L, 5L, 6L )
    when(fakeReadStatement.nodesGetAll()).thenReturn(nodesIterator)

    val lruCache: LRUCache[String, (ExecutionPlan, Map[String, Any])] = new LRUCache[String, (ExecutionPlan, Map[String, Any])](1)
    val cacheAccessor = new MonitoringCacheAccessor[String, (ExecutionPlan, Map[String, Any])](mock[CypherCacheHitMonitor[String]])
    val cache = new QueryCache[String, (ExecutionPlan, Map[String, Any])](cacheAccessor, lruCache)

    when(fakeReadStatement.schemaStateGetOrCreate(any(), any())).thenAnswer(
      new Answer[QueryCache[String, (ExecutionPlan, Map[String, Any])]]() {
        def answer(invocation: InvocationOnMock) = { cache }
    })

    val engine = new ExecutionEngine(fakeGraph)

    //When:
    graph.inTx {
      counter.source = GlobalGraphOperations.at(graph).getAllNodes.iterator()
      engine.execute("match n return n limit 5", Map.empty[String,Any]).toList
    }

    //Then:
    assert( nodesIterator.hasNext )
    assert( nodesIterator.next() === 5L )
  }

  test("traversalmatcherpipe is lazy") {
    //Given:
    val tx = graph.beginTx()
    val limiter = new Limiter(2)
    val traversalMatchPipe = createTraversalMatcherPipe(limiter)

    //When:
    val result = traversalMatchPipe.createResults(QueryStateHelper.queryStateFrom(graph, tx))

    //Then:
    assert(limiter.count === 0)

    //Also then:
    result.next() // throws exception if we iterate over more than expected to fill buffers
    tx.close()
  }

  test("filterpipe is lazy") {
    //Given:
    val limited = new LimitedIterator[Map[String, Any]](4, (x) => Map("val" -> x))
    val input = new FakePipe(limited, "val" -> CTInteger)
    val pipe = new FilterPipe(input, GreaterThan(Identifier("val"), Literal(3)))()

    //When:
    val iter = pipe.createResults(QueryStateHelper.empty)

    //Then:
    assert(limited.count === 0)

    //Also then:
    iter.next() // throws exception if we iterate over more than expected to
  }

  private def createTraversalMatcherPipe(limiter: Limiter): TraversalMatchPipe = {
    val monitoredNode = new MonitoredNode(aNode, limiter.monitor)

    val end = EndPoint("b")
    val trail = SingleStepTrail(end, SemanticDirection.OUTGOING, "r", Seq(), "a", True(), True(), null, Seq())
    val step = trail.toSteps(0).get
    val producer = EntityProducer[Node]("test", mock[Argument]) { (ctx, state) => Iterator(monitoredNode) }
    val matcher = new MonoDirectionalTraversalMatcher(step, producer)
    new TraversalMatchPipe(SingleRowPipe(), matcher, trail)
  }
}

class CountingJIterator extends JIterator[Node] {
  private var seenNodes = 0
  private var _source: Option[JIterator[Node]] = None

  def source = _source.getOrElse(Seq.empty.asJava)

  def source_= (newSource: JIterator[Node]): Unit = _source = Some(newSource)

  def hasNext: Boolean = _source.exists(_.hasNext)

  def next(): Node = _source match {
    case Some(jIterator) =>
      val result = jIterator.next()
      seenNodes  = seenNodes + 1
      result

    case _  =>
      throw new NoSuchElementException()
  }

  def remove() { throw new UnsupportedOperationException }

  def count = seenNodes
}

class LimitedIterator[T](limit: Int, f: Int => T, message: String = "Limit reached!") extends Iterator[T] {
  var count = 0

  def hasNext = true

  def next() = {
    count += 1
    if ( count > limit ) {
      fail(message)
    }
    f(count)
  }
}

class Limiter(limit: Int) {
  var count: Int = 0

  def monitor() {
    count += 1
    if ( count > limit )    {
      fail("Limit passed!")
    }
  }
}

class MonitoredNode(inner: Node, monitor: () => Unit) extends Node {
  def getId: Long = inner.getId

  def getRelationships(types: RelationshipType*): JIterable[Relationship] = null

  def delete() {}

  def getRelationships: JIterable[Relationship] = null

  def hasRelationship: Boolean = false

  def getRelationships(direction: Direction, types: RelationshipType*): JIterable[Relationship] = null

  def hasRelationship(types: RelationshipType*): Boolean = false

  def hasRelationship(direction: Direction, types: RelationshipType*): Boolean = false

  def getRelationships(dir: Direction): JIterable[Relationship] = new AIteratable(inner.getRelationships(dir).iterator(), monitor)

  def hasRelationship(dir: Direction): Boolean = false

  def getRelationships(`type`: RelationshipType, dir: Direction): JIterable[Relationship] = null

  def hasRelationship(`type`: RelationshipType, dir: Direction): Boolean = false

  def getSingleRelationship(`type`: RelationshipType, dir: Direction): Relationship = null

  def createRelationshipTo(otherNode: Node, `type`: RelationshipType): Relationship = null

  def traverse(traversalOrder: Order, stopEvaluator: StopEvaluator, returnableEvaluator: ReturnableEvaluator, relationshipType: RelationshipType, direction: Direction): Traverser = null

  def traverse(traversalOrder: Order, stopEvaluator: StopEvaluator, returnableEvaluator: ReturnableEvaluator, firstRelationshipType: RelationshipType, firstDirection: Direction, secondRelationshipType: RelationshipType, secondDirection: Direction): Traverser = null

  def traverse(traversalOrder: Order, stopEvaluator: StopEvaluator, returnableEvaluator: ReturnableEvaluator, relationshipTypesAndDirections: AnyRef*): Traverser = null

  def getGraphDatabase: GraphDatabaseService = null

  def hasProperty(key: String): Boolean = false

  def getProperty(key: String): AnyRef = inner.getProperty(key)

  def getProperty(key: String, defaultValue: AnyRef): AnyRef = null

  def setProperty(key: String, value: AnyRef) {}

  def removeProperty(key: String): AnyRef = null

  def getPropertyKeys: JIterable[String] = null

  def getProperties( keys: String* ): util.Map[String, AnyRef] = null

  def getAllProperties: util.Map[String, AnyRef] = null

  override def toString = "°" + inner.toString + "°"

  def addLabel(label: Label) {
    ???
  }

  def removeLabel(label: Label) {
    ???
  }

  def hasLabel(label: Label) = ???

  def getLabels = ???

  def getRelationshipTypes = ???

  def getDegree:Int = ???

  def getDegree( direction:Direction ):Int = ???

  def getDegree( relType:RelationshipType ):Int = ???

  def getDegree( relType:RelationshipType, direction:Direction ):Int = ???
}


class AIteratable(iter: JIterator[Relationship], monitor: () => Unit) extends JIterable[Relationship] {
  def iterator() = new AIterator(iter, monitor)
}


class AIterator(iter: JIterator[Relationship], monitor: () => Unit) extends JIterator[Relationship] {
  def hasNext = iter.hasNext

  def next() = {
    monitor()
    iter.next()
  }

  def remove() {
    iter.remove()
  }
}

