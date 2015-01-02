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

import commands.expressions.Identifier
import pipes.matching._
import symbols._
import org.neo4j.cypher.GraphDatabaseJUnitSuite
import org.neo4j.graphdb._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{After, Test}
import org.junit.runners.Parameterized.Parameters
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.mockito.Matchers._
import collection.JavaConverters._
import org.neo4j.cypher.internal.compiler.v2_0.pipes._
import org.neo4j.cypher.internal.compiler.v2_0.pipes.matching.PatternGraph
import org.neo4j.cypher.internal.compiler.v2_0.commands._
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Literal
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.CountStar

/*
This test fixture tries to assert that Pipe declaring that they are lazy
in fact are lazy. Every Pipe should be represented here
 */

@RunWith(value = classOf[Parameterized])
class PipeLazynessTest(pipe: Pipe, iter: LazyIterator[_]) extends GraphDatabaseJUnitSuite {
  @Test def test() {
    iter.db = Some(graph)
    val resultIterator = pipe.createResults(queryState)

    if (resultIterator.hasNext)
      resultIterator.next()

    val isEager = iter.isEmpty
    assert(pipe.isLazy !== isEager, s"The lazyness declaration for pipe ${pipe.toString} is not true")
  }

  var tx : org.neo4j.graphdb.Transaction = null

  private def queryState = {
    if(tx == null) tx = graph.beginTx()
    QueryStateHelper.queryStateFrom(graph, tx)
  }

  @After
  def cleanup()
  {
    if(tx != null) tx.close()
  }
}

object PipeLazynessTest extends MockitoSugar {

  @Parameters(name = "{0}")
  def parameters: java.util.Collection[Array[AnyRef]] = {
    val list = new java.util.ArrayList[Array[AnyRef]]()

    def add(objects: Seq[Object]) {
      list.add(Array(objects: _*))
    }

    add(columnFilterPipe)
    add(eagerPipe)
    add(distinctPipe)
    add(eagerAggregationPipe)
    add(emptyResultPipe)
    add(executeUpdateCommandsPipe)
    add(filterPipe)
    add(matchPipe)
    add(namedPathPipe)
    add(shortestPathPipe)
    add(slicePipe)
    add(sortPipe)
    add(startPipe)
    add(topPipe)
    add(traversalMatcherPipe)
    add(unionPipe)

    // TODO add test for NullInsertingPipe
    // constrainOperationPipe and indexOperationPipe do not take a source pipe, and so aren't covered by these tests

    list
  }

  private def columnFilterPipe = {
    val (iter, src) = emptyFakes
    val pipe = new ColumnFilterPipe(src, Seq.empty)
    Seq(pipe, iter)
  }

  private def distinctPipe = {
    val iter = new LazyIterator[Map[String, Any]](10, (n) => Map("x" -> n))
    val src = new FakePipe(iter, "x" -> CTNumber)
    val pipe = new DistinctPipe(src, Map("x" -> Identifier("x")))
    Seq(pipe, iter)
  }

  private def eagerAggregationPipe = {
    val (iter, src) = emptyFakes
    val pipe = new EagerAggregationPipe(src, Map.empty, Map("x" -> CountStar()))
    Seq(pipe, iter)
  }

  private def eagerPipe = {
    val (iter, src) = emptyFakes
    val pipe = new EagerPipe(src)
    Seq(pipe, iter)
  }

  private def emptyResultPipe = {
    val (iter, src) = emptyFakes
    val pipe = new EmptyResultPipe(src)
    Seq(pipe, iter)
  }

  private def executeUpdateCommandsPipe = {
    val iter = new LazyIterator[Map[String, Any]](10, (n) => Map("x" -> n))
    val src = new FakePipe(iter, "x" -> CTNumber)
    val pipe = new ExecuteUpdateCommandsPipe(src, Seq.empty)
    Seq(pipe, iter)
  }

  private def filterPipe = {
    val (iter, src) = emptyFakes
    val pipe = new FilterPipe(src, True())
    Seq(pipe, iter)
  }

  private def namedPathPipe = {
    val node = mock[Node]
    val iter = new LazyIterator[Map[String, Any]](10, (_) => Map("x" -> node))
    val src = new FakePipe(iter, "x" -> CTNode)
    val pipe = new NamedPathPipe(src, "p", Seq(ParsedEntity("x")))
    Seq(pipe, iter)
  }

  private def matchPipe = {
    // Produces a MatchPipe for the pattern (x)-[r]->(y)

    val node1 = mock[Node]
    val node2 = mock[Node]
    val rel1 = mock[Relationship]
    when(node1.getRelationships(Direction.OUTGOING)).thenReturn(Iterable[Relationship](rel1).asJava)

    val iter = new LazyIterator[Map[String, Any]](10, (_, db) => Map("x" -> node1))
    val src = new FakePipe(iter, "x" -> CTNode)
    val x = new PatternNode("x")
    val y = new PatternNode("y")
    val rel = x.relateTo("r", y, Seq.empty, Direction.OUTGOING)

    val patternNodes = Map("x" -> x, "y" -> y)
    val patternRels = Map("r" -> rel)
    val graph = new PatternGraph(patternNodes, patternRels, Seq("x"), Seq.empty)
    val pipe = new MatchPipe(src, Seq(), graph, Set("x", "r", "y"))
    Seq(pipe, iter)
  }

  private def shortestPathPipe = {
    val shortestPath = ShortestPath(pathName = "p", left = SingleNode("start"), right = SingleNode("end"), relTypes = Seq.empty,
      dir = Direction.OUTGOING, maxDepth = None, single = true, relIterator = None)
    val iter = new LazyIterator[Map[String, Any]](10, (_) => Map("start" -> null, "end" -> null))
    val src = new FakePipe(iter, "start" -> CTNode, "end" -> CTNode)
    val pipe = new ShortestPathPipe(src, shortestPath)
    Seq(pipe, iter)
  }

  private def slicePipe = {
    val (iter, src) = emptyFakes
    val pipe = new SlicePipe(src, Some(Literal(2)), Some(Literal(2)))
    Seq(pipe, iter)
  }

  private def sortPipe = {
    val (iter, src) = emptyFakes
    val pipe = new SortPipe(src, sortByX)
    Seq(pipe, iter)
  }

  private val sortByX: List[SortItem] = List(SortItem(Identifier("x"), ascending = true))

  private def startPipe = {
    val node = mock[Node]
    val (iter, src) = emptyFakes
    val pipe = new NodeStartPipe(src, "y", new EntityProducer[Node]() {
      def producerType: String = "SingleNodeMock"

      def apply(v1: ExecutionContext, v2: QueryState): Iterator[Node] = Iterator(node)
    })
    Seq(pipe, iter)
  }

  private def unionPipe = {
    val (iter, src) = emptyFakes
    val (_, src2) = emptyFakes

    val pipe = new UnionPipe(Seq(src, src2), List("x"))

    Seq(pipe, iter)
  }

  private def topPipe = {
    val (iter, src) = emptyFakes
    val pipe = new TopPipe(src, sortByX, Literal(5))
    Seq(pipe, iter)
  }

  private def traversalMatcherPipe = {
    val (iter, src) = emptyFakes
    val traversalMatcher = mock[TraversalMatcher]
    val path = mock[Path]
    val trail = mock[Trail]

    when(traversalMatcher.findMatchingPaths(any(), any())).
      thenReturn(Iterator(path))

    when(path.iterator()).
      thenReturn(Iterator[PropertyContainer]().asJava)

    when(trail.decompose(any())).
      thenReturn(Iterator(Map[String, Any]()))


    val pipe = new TraversalMatchPipe(src, traversalMatcher, trail)
    Seq(pipe, iter)
  }

  private def emptyFakes: (LazyIterator[Map[String, Any]], FakePipe) = {
    val iter = new LazyIterator[Map[String, Any]](10, (x) => Map("x" -> x))
    val src = new FakePipe(iter, "x" -> CTNumber)
    (iter, src)
  }
}
