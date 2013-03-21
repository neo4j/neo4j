/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher

import internal.commands.expressions.{Literal, Identifier}
import internal.commands.{GreaterThan, True}
import internal.pipes._
import internal.pipes.QueryStateHelper.queryStateFrom
import internal.pipes.matching._
import internal.symbols.IntegerType
import matching.SingleStep
import org.neo4j.graphdb._
import java.util.{Iterator => JIterator}
import java.lang.{Iterable => JIterable}
import org.junit.{Test, Before}
import org.neo4j.graphdb.Traverser.Order
import org.scalatest.Assertions
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.neo4j.kernel.{ThreadToStatementContextBridge, GraphDatabaseAPI}
import org.neo4j.helpers.collection.IteratorWrapper
import org.neo4j.kernel.impl.core.NodeManager
import org.neo4j.kernel.api.StatementContext
import org.neo4j.kernel.impl.api.{KernelSchemaStateStore, SchemaStateOperations, CompositeStatementContext}

class LazyTest extends ExecutionEngineHelper with Assertions with MockitoSugar {

  var a: Node = null
  var b: Node = null
  var c: Node = null


  @Before def my_init() {
    a = createNode()
    b = createNode()
    c = createNode()
    relate(a, b)
    relate(a, c)
    relate(a, createNode())
    relate(a, createNode())
    relate(a, createNode())
    relate(a, createNode())
    relate(a, createNode())
    relate(a, createNode())
    relate(a, createNode())
    relate(a, createNode())
  }

  @Test def get_first_relationship_does_not_iterate_through_all() {
    //Given:
    val limiter = new Limiter(1)
    val monitoredNode = new MonitoredNode(a, limiter.monitor)
    val iter = monitoredNode.getRelationships(Direction.OUTGOING).iterator()

    //When:
    iter.next()

    //Then does not throw exception
  }

  @Test def traversal_matcher_is_lazy() {
    //Given:
    val limiter = new Limiter(2)
    val monitoredNode = new MonitoredNode(a, limiter.monitor)

    val step = SingleStep(0, Seq(), Direction.OUTGOING, None, True(), True())
    val producer = EntityProducer[Node]("test") { (ctx, state) => Iterator(monitoredNode) }
    val matcher = new MonoDirectionalTraversalMatcher(step, producer)
    val ctx = internal.ExecutionContext().newWith("a" -> monitoredNode)

    //When:
    val iter = matcher.findMatchingPaths(queryStateFrom(graph), ctx)

    //Then:
    assert(limiter.count === 0)

    //Also then, does not throw exception
    iter.next()
  }

  @Test def execution_of_query_is_lazy() {
    //Given:
    val limiter = new Limiter(3)
    val monitoredNode = new MonitoredNode(a, limiter.monitor)

    val engine = new ExecutionEngine(graph)

    //When:
    val iter: ExecutionResult = engine.execute("start n=node({foo}) match n-->x return x", Map("foo" -> monitoredNode))

    //Then:
    assert(limiter.count === 0)

    //Also then does not step over the limit
    iter.next()
  }

  @Test def distinct_is_lazy() {
    //Given:
    val a = mock[Node]
    val b = mock[Node]
    val c = mock[Node]

    when(a.hasProperty("name")).thenReturn(true)
    when(a.getProperty("name", null)).thenReturn("Andres", Array())
    when(b.hasProperty("name")).thenReturn(true)
    when(b.getProperty("name", null)).thenReturn("Jake", Array())

    // Because we use a prefetching iterator, it will cache one more result than we have pulled
    when(c.hasProperty("name")).thenThrow(new RuntimeException("Distinct was not lazy!"))

    val engine = new ExecutionEngine(graph)

    //When:
    val iter = engine.execute("start n=node({foo}) return distinct n.name", Map("foo" -> Seq(a, b, c)))

    //Then, no Runtime exception is thrown
    iter.next()
  }

  @Test def union_is_lazy() {
    //Given:
    val a = mock[Node]
    val b = mock[Node]
    val c = mock[Node]

    when(a.hasProperty("name")).thenReturn(true)
    when(a.getProperty("name", null)).thenReturn("Andres", Array())
    when(b.hasProperty("name")).thenReturn(true)
    when(b.getProperty("name", null)).thenReturn("Jake", Array())

    // Because we use a pre-fetching iterator, it will cache one more result than we have pulled
    when(c.hasProperty("name")).thenThrow(new RuntimeException("Union was not lazy!"))

    val engine = new ExecutionEngine(graph)

    //When:
    val iter = engine.execute("start n=node({a}) return n.name UNION ALL start n=node({b}) return n.name", Map("a" -> a, "b" -> Seq(b, c)))

    //Then, no Runtime exception is thrown
    iter.next()
  }

  @Test def execution_of_query_is_eager() {
    //Given:
    var touched = false
    val monitoredNode = new MonitoredNode(a, () => touched = true)

    val engine = new ExecutionEngine(graph)

    //When:
    val iter: ExecutionResult = engine.execute("start n=node({foo}) match n-->x create n-[:FOO]->x", Map("foo" -> monitoredNode))

    //Then:
    assert(touched, "Query should have been executed")
  }

  @Test def graph_global_queries_are_lazy() {
    //Given:
    val (iter, counter) = countElementsPassedThrough(graph.getAllNodes.iterator())

    val fakeGraph = mock[GraphDatabaseAPI]
    val tx = mock[Transaction]
    val nodeMgre = mock[NodeManager]
    val dependencies = mock[DependencyResolver]
    val bridge = mock[ThreadToStatementContextBridge]
    val fakeCtx = mock[StatementContext]
    val schemaState = new KernelSchemaStateStore()
    val schemaOps = new SchemaStateOperations(fakeCtx, schemaState)
    val comboCtx = new CompositeStatementContext(fakeCtx, schemaOps)
    when(nodeMgre.getAllNodes).thenReturn(iter)
    when(bridge.getCtxForWriting).thenReturn(comboCtx)
    when(fakeGraph.getDependencyResolver).thenReturn(dependencies)
    when(dependencies.resolveDependency(classOf[ThreadToStatementContextBridge])).thenReturn(bridge)
    when(dependencies.resolveDependency(classOf[NodeManager])).thenReturn(nodeMgre)
    when(dependencies.resolveDependency(classOf[ThreadToStatementContextBridge])).thenReturn(bridge)
    when(fakeGraph.beginTx()).thenReturn(tx)

    val engine = new ExecutionEngine(fakeGraph)

    //When:
    engine.execute("start n=node(*) return n.number? limit 5").toList

    //Then:
    assert(counter.count === 5, "Should not have fetched more than this many nodes.")
  }


  @Test def traversalmatcherpipe_is_lazy() {
    //Given:
    val limiter = new Limiter(2)
    val traversalMatchPipe = createTraversalMatcherPipe(limiter)

    //When:
    val result = traversalMatchPipe.createResults(queryStateFrom(graph))

    //Then:
    assert(limiter.count === 0)

    //Also then:
    result.next() // throws exception if we iterate over more than expected to fill buffers
  }

  @Test def filterpipe_is_lazy() {
    //Given:
    val limited = new LimitedIterator[Map[String, Any]](4, (x) => Map("val" -> x))
    val input = new FakePipe(limited, "val" -> IntegerType())
    val pipe = new FilterPipe(input, GreaterThan(Identifier("val"), Literal(3)))

    //When:
    val iter = pipe.createResults(QueryStateHelper.empty)

    //Then:
    assert(limited.count === 0)

    //Also then:
    iter.next() // throws exception if we iterate over more than expected to
  }

  private def createTraversalMatcherPipe(limiter: Limiter): TraversalMatchPipe = {
    val monitoredNode = new MonitoredNode(a, limiter.monitor)

    val end = EndPoint("b")
    val trail = SingleStepTrail(end, Direction.OUTGOING, "r", Seq(), "a", True(), True(), null, Seq())
    val step = trail.toSteps(0).get
    val producer = EntityProducer[Node]("test") { (ctx, state) => Iterator(monitoredNode) }
    val matcher = new MonoDirectionalTraversalMatcher(step, producer)
    new TraversalMatchPipe(NullPipe, matcher, trail)
  }

  trait GetCount {
    def count: Int
  }

  private def countElementsPassedThrough(in: JIterator[Node]): (JIterator[Node], GetCount) = {
    var seenNodes = 0

    val countingIterator = new IteratorWrapper[Node, Node](in) {
      def underlyingObjectToObject(in: Node): Node = {
        seenNodes += 1
        in
      }
    }

    val counter = new GetCount {
      def count: Int = seenNodes
    }

    (countingIterator, counter)
  }
}

class LimitedIterator[T](limit: Int, f: Int => T, message: String = "Limit reached!") extends Iterator[T] {
  var count = 0

  def hasNext = true

  def next() = {
    count += 1
    if (count > limit)
      throw new RuntimeException(message)
    f(count)
  }
}

class Limiter(limit: Int) {
  var count: Int = 0

  def monitor() {
    count += 1
    if (count > limit)
      throw new RuntimeException("Limit passed!")
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

  def getPropertyValues: JIterable[AnyRef] = null

  override def toString = "°" + inner.toString + "°"

  def addLabel(label: Label) {
    ???
  }

  def removeLabel(label: Label) {
    ???
  }

  def hasLabel(label: Label) = ???

  def getLabels() = ???
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

