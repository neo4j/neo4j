/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0

import commands.expressions.{Literal, Identifier}
import commands.{GreaterThan, True}
import pipes._
import pipes.matching._
import symbols.CTInteger
import org.neo4j.cypher.internal.{ExecutionPlan, LRUCache, CypherCompiler => GeneralCypherCompiler}
import org.neo4j.cypher._
import org.neo4j.graphdb._
import org.neo4j.graphdb.Traverser.Order
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.api.{Statement, ReadOperations}
import org.neo4j.kernel.impl.core.{ThreadToStatementContextBridge, NodeManager}
import org.neo4j.tooling.GlobalGraphOperations
import java.util.{Iterator => JIterator}
import java.lang.{Iterable => JIterable}
import org.junit.{Test, Before}
import org.junit.Assert._
import collection.JavaConverters._
import org.scalatest.Assertions
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.neo4j.kernel.impl.api.OperationsFacade

class LazyTest extends ExecutionEngineJUnitSuite with MockitoSugar {

  var a: Node = null
  var b: Node = null
  var c: Node = null



  override protected def initTest() {
    super.initTest()

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

    //When:
    graph.inTx(monitoredNode.getRelationships(Direction.OUTGOING).iterator().next())

    //Then does not throw exception
  }

  @Test def traversal_matcher_is_lazy() {
    //Given:
    val tx = graph.beginTx()
    val limiter = new Limiter(2)
    val monitoredNode = new MonitoredNode(a, limiter.monitor)

    val step = SingleStep(0, Seq(), Direction.OUTGOING, None, True(), True())
    val producer = EntityProducer[Node]("test") { (ctx, state) => Iterator(monitoredNode) }
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
    iter.close()
  }

  @Test def distinct_is_lazy() {
    //Given:
    val a = createNode(Map("name" -> "Andres"))
    val b = createNode(Map("name" -> "Jake"))

    val c = mock[Node]

    // Because we use a prefetching iterator, it will cache one more result than we have pulled
    // if it doesn't it will try to get the name property from the mock c and fail

    when(c.hasProperty("name")).thenThrow(new RuntimeException("Distinct was not lazy!"))

    val engine = new ExecutionEngine(graph)

    //When:
    val iter = engine.execute("start n=node({foo}) return distinct n.name", Map("foo" -> Seq(a, b, c)))

    //Then, no Runtime exception is thrown
    iter.next()
    iter.close()
  }

  @Test def union_is_lazy() {
    //Given:
    val a = createNode(Map("name" -> "Andres"))
    val b = createNode(Map("name" -> "Jake"))

    val c = mock[Node]

    // Because we use a pre-fetching iterator, it will cache one more result than we have pulled
    when(c.hasProperty("name")).thenThrow(new RuntimeException("Union was not lazy!"))

    val engine = new ExecutionEngine(graph)

    //When:
    val iter = engine.execute("start n=node({a}) return n.name UNION ALL start n=node({b}) return n.name", Map("a" -> a, "b" -> Seq(b, c)))

    //Then, no Runtime exception is thrown
    iter.next()
    iter.close()
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
    val counter = new CountingJIterator()

    val fakeGraph = mock[GraphDatabaseAPI]
    val tx = mock[Transaction]
    val nodeManager = mock[NodeManager]
    val dependencies = mock[DependencyResolver]
    val bridge = mock[ThreadToStatementContextBridge]

    val fakeDataStatement = mock[OperationsFacade]
    val fakeReadStatement = mock[ReadOperations]
    val fakeStatement = mock[Statement]

    when(nodeManager.getAllNodes).thenReturn(counter)
    when(bridge.instance()).thenReturn(fakeStatement)
    when(fakeStatement.readOperations()).thenReturn(fakeReadStatement)
    when(fakeStatement.dataWriteOperations()).thenReturn(fakeDataStatement)
    when(fakeReadStatement.schemaStateGetOrCreate[ExecutionEngine,(LRUCache[String, ExecutionPlan], GeneralCypherCompiler.VersionProxy)](anyObject(), anyObject())).thenReturn(
      (new LRUCache[String, ExecutionPlan](1), GeneralCypherCompiler(fakeGraph)))
    when(fakeGraph.getDependencyResolver).thenReturn(dependencies)
    when(dependencies.resolveDependency(classOf[ThreadToStatementContextBridge])).thenReturn(bridge)
    when(dependencies.resolveDependency(classOf[NodeManager])).thenReturn(nodeManager)
    when(fakeGraph.beginTx()).thenReturn(tx)

    val engine = new ExecutionEngine(fakeGraph)

    //When:
    graph.inTx {
      counter.source = GlobalGraphOperations.at(graph).getAllNodes.iterator()
      engine.execute("start n=node(*) return n limit 5", Map.empty[String,Any]).toList
    }

    //Then:
    assert(counter.count === 5, "Should not have fetched more than this many nodes.")
  }

  @Test def traversalmatcherpipe_is_lazy() {
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

  @Test def filterpipe_is_lazy() {
    //Given:
    val limited = new LimitedIterator[Map[String, Any]](4, (x) => Map("val" -> x))
    val input = new FakePipe(limited, "val" -> CTInteger)
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
    new TraversalMatchPipe(NullPipe(), matcher, trail)
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

  override def toString = "°" + inner.toString + "°"

  def addLabel(label: Label) {
    ???
  }

  def removeLabel(label: Label) {
    ???
  }

  def hasLabel(label: Label) = ???

  def getLabels = ???
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

