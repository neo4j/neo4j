/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0

import java.lang.{Iterable => JIterable}
import java.util
import java.util.{Iterator => JIterator}

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.collection.primitive.PrimitiveLongIterator
import org.neo4j.cypher._
import org.neo4j.cypher.internal.compiler.v3_0.commands.expressions.{Literal, Variable}
import org.neo4j.cypher.internal.compiler.v3_0.commands.predicates.{GreaterThan, True}
import org.neo4j.cypher.internal.compiler.v3_0.helpers.{Counter, CountingIterator}
import org.neo4j.cypher.internal.compiler.v3_0.pipes._
import org.neo4j.cypher.internal.compiler.v3_0.pipes.matching._
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.Argument
import org.neo4j.cypher.internal.frontend.v3_0.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_0.symbols.CTInteger
import org.neo4j.cypher.internal.spi.v3_0.MonoDirectionalTraversalMatcher
import org.neo4j.cypher.internal.{ExecutionEngine, ExecutionPlan, ExecutionResult}
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.graphdb._
import org.neo4j.kernel.api.KernelTransaction.Revertable
import org.neo4j.kernel.api.security.AccessMode
import org.neo4j.kernel.api.{KernelTransaction, ReadOperations, Statement}
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.api.OperationsFacade
import org.neo4j.kernel.impl.core.{NodeManager, NodeProxy, ThreadToStatementContextBridge}
import org.neo4j.kernel.impl.coreapi.{PropertyContainerLocker, InternalTransaction}
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade
import org.neo4j.kernel.impl.query.{QueryEngineProvider, Neo4jTransactionalContext}
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore

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
    val limiter = Counter().values.limit(1) { _ => fail("Limit reached!") }
    val monitoredNode = new MonitoredNode(aNode, limiter.tick)

    //When:
    graph.inTx(monitoredNode.getRelationships(Direction.OUTGOING).iterator().next())

    //Then does not throw exception
  }

  test("traversal matcher is lazy") {
    //Given:
    val tx = graph.beginTransaction( KernelTransaction.Type.explicit, AccessMode.Static.READ )
    val limiter = Counter().values.limit(2) { _ => fail("Limit reached!") }
    val monitoredNode = new MonitoredNode(aNode, limiter.tick)

    val step = SingleStep(0, Seq(), SemanticDirection.OUTGOING, None, True(), True())
    val producer = EntityProducer[Node]("test", mock[Argument]) { (ctx, state) => Iterator(monitoredNode) }
    val matcher = new MonoDirectionalTraversalMatcher(step, producer)
    val ctx = ExecutionContext().newWith("a" -> monitoredNode)

    //When:
    val iter = matcher.findMatchingPaths(QueryStateHelper.queryStateFrom(graph, tx), ctx)

    //Then:
    assert(limiter.counted === 0)

    //Also then, does not throw exception
    iter.next()
    tx.close()
  }

  test("execution of query is lazy") {
    //Given:
    val limiter = Counter().values.limit(3) { _ => fail("Limit reached!") }
    val monitoredNode = new MonitoredNode(aNode, limiter.tick)

    val engine = new ExecutionEngine(graph)

    //When:
    val iter: ExecutionResult = engine.execute("match (n)-->(x) where n = {foo} return x", Map("foo" -> monitoredNode), graph.session())

    //Then:
    assert(limiter.counted === 0)

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
    val iter = engine.execute(
      "match (n) where n IN {foo} return distinct n.name", Map("foo" -> Seq(a, b, c)), graph.session())

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
    val iter =  engine.execute("match (n) where n = {a} return n.name UNION ALL match (n) where n IN {b} return n.name",
      Map("a" -> a, "b" -> Seq(b, c)), graph.session())

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
    val iter: ExecutionResult =
      engine.execute("start n=node({foo}) match (n)-->(x) create n-[:FOO]->x", Map("foo" -> monitoredNode), graph.session())

    //Then:
    assert(touched, "Query should have been executed")
  }

  test("graph global queries are lazy") {
    //Given:
    val fakeGraph = mock[GraphDatabaseFacade]
    val nodeManager = mock[NodeManager]
    val dependencies = mock[DependencyResolver]
    val bridge = mock[ThreadToStatementContextBridge]
    val monitors = new org.neo4j.kernel.monitoring.Monitors()
    val config = Config.empty()

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
    when(dependencies.resolveDependency(classOf[Config])).thenReturn(config)
    when(fakeGraph.beginTransaction(any(classOf[KernelTransaction.Type]), any(classOf[AccessMode]) )).thenReturn(mock[InternalTransaction])
    val n0 = mock[Node]
    val n1 = mock[Node]
    val n2 = mock[Node]
    val n3 = mock[Node]
    val n4 = mock[Node]
    val n5 = mock[Node]
    val n6 = mock[Node]
    val nodesIterator = List( n0, n1, n2, n3, n4, n5, n6 ).iterator
    val allNodeIdsIterator = new PrimitiveLongIterator {
      override def hasNext = nodesIterator.hasNext
      override def next() = nodesIterator.next().getId
    }
    when(fakeReadStatement.nodesGetAll).thenReturn(allNodeIdsIterator)

    val lruCache: LRUCache[String, (ExecutionPlan, Map[String, Any])] = new LRUCache[String, (ExecutionPlan, Map[String, Any])](1)
    val cacheAccessor = new MonitoringCacheAccessor[String, (ExecutionPlan, Map[String, Any])](mock[CypherCacheHitMonitor[String]])
    val cache = new QueryCache[String, (ExecutionPlan, Map[String, Any])](cacheAccessor, lruCache)

    when(fakeReadStatement.schemaStateGetOrCreate(any(), any())).thenAnswer(
      new Answer[QueryCache[String, (ExecutionPlan, Map[String, Any])]]() {
        def answer(invocation: InvocationOnMock) = { cache }
    })

    val service = new GraphDatabaseCypherService(fakeGraph)
    val engine = new ExecutionEngine(service)

    val tx = fakeGraph.beginTransaction(KernelTransaction.Type.`implicit`, AccessMode.Static.FULL)
    val context = new Neo4jTransactionalContext(service, tx, fakeStatement, new PropertyContainerLocker)
    val revertable = mock[Revertable]
    when(context.restrictCurrentTransaction(anyObject())).thenReturn(revertable)
    val session = QueryEngineProvider.embeddedSession(context)

    //When:
    engine.execute("match (n) return n limit 4", Map.empty[String,Any], session).toList

    //Then:
    assert( nodesIterator.hasNext )
    assert( nodesIterator.next() === n5 )
  }

  test("traversalmatcherpipe is lazy") {
    //Given:
    val tx = graph.beginTransaction( KernelTransaction.Type.explicit, AccessMode.Static.FULL )
    val limiter = Counter().values.limit(2) { _ => fail("Limit reached") }
    val traversalMatchPipe = createTraversalMatcherPipe(limiter)

    //When:
    val result = traversalMatchPipe.createResults(QueryStateHelper.queryStateFrom(graph, tx))

    //Then:
    assert(limiter.counted === 0)

    //Also then:
    result.next() // throws exception if we iterate over more than expected to fill buffers
    tx.close()
  }

  test("filterpipe is lazy") {
    //Given:
    val limited = Counter().map[Map[String, Any]](x => Map("val" -> x)).limit(4) { _ => fail("Limit reached!") }
    val input = new FakePipe(limited, "val" -> CTInteger)
    val pipe = new FilterPipe(input, GreaterThan(Variable("val"), Literal(3)))()

    //When:
    val iter = pipe.createResults(QueryStateHelper.empty)

    //Then:
    assert(limited.counted === 0)

    //Also then:
    iter.next() // throws exception if we iterate over more than expected to
  }

  private def createTraversalMatcherPipe(limiter: CountingIterator[Long]): TraversalMatchPipe = {
    val monitoredNode = new MonitoredNode(aNode, limiter.tick)

    val end = EndPoint("b")
    val trail = SingleStepTrail(end, SemanticDirection.OUTGOING, "r", Seq(), "a", True(), True(), null, Seq())
    val step = trail.toSteps(0).get
    val producer = EntityProducer[Node]("test", mock[Argument]) { (ctx, state) => Iterator(monitoredNode) }
    val matcher = new MonoDirectionalTraversalMatcher(step, producer)
    new TraversalMatchPipe(SingleRowPipe(), matcher, trail)
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

