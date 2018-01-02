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

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.neo4j.cypher.GraphDatabaseFunSuite
import org.neo4j.cypher.internal.compiler.v2_3.commands._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Identifier, Literal}
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.True
import org.neo4j.cypher.internal.compiler.v2_3.pipes._
import org.neo4j.cypher.internal.compiler.v2_3.pipes.matching._
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.Argument
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.graphdb._

import scala.collection.JavaConverters._

/*
This test fixture tries to assert that Pipe declaring that they are lazy
in fact are lazy. Every Pipe should be represented here
 */

class PipeLazynessTest extends GraphDatabaseFunSuite with QueryStateTestSupport {
  private implicit val monitor = mock[PipeMonitor]

  test("test") {
    columnFilterPipe.!!()
    distinctPipe.!!()
    executeUpdateCommandsPipe.!!()
    filterPipe.!!()
    matchPipe.!!()
    namedPathPipe.!!()
    shortestPathPipe.!!()
    slicePipe.!!()
    startPipe.!!()
    traversalMatcherPipe.!!()
    unionPipe.!!()
  }

  implicit class IsNotEager(pair: (Pipe, LazyIterator[Map[String, Any]])) {
      def !!() = {
        val pipe = pair._1
        val iter = pair._2
        iter.db = Some(graph)
        val resultIterator = withQueryState { queryState =>
          pipe.createResults(queryState)
        }

        if (resultIterator.hasNext)
          resultIterator.next()

        assert(iter.nonEmpty, pipe.getClass.getSimpleName)
      }
  }

  private def columnFilterPipe = {
    val (iter, src) = emptyFakes
    val pipe = new ColumnFilterPipe(src, Seq.empty)
    (pipe, iter)
  }

  private def distinctPipe = {
    val iter = new LazyIterator[Map[String, Any]](10, (n) => Map("x" -> n))
    val src = new FakePipe(iter, "x" -> CTNumber)
    val pipe = new DistinctPipe(src, Map("x" -> Identifier("x")))()
    (pipe, iter)
  }

  private def executeUpdateCommandsPipe = {
    val iter = new LazyIterator[Map[String, Any]](10, (n) => Map("x" -> n))
    val src = new FakePipe(iter, "x" -> CTNumber)
    val pipe = new ExecuteUpdateCommandsPipe(src, Seq.empty)
    (pipe, iter)
  }

  private def filterPipe = {
    val (iter, src) = emptyFakes
    val pipe = new FilterPipe(src, True())()(mock[PipeMonitor])
    (pipe, iter)
  }

  private def namedPathPipe = {
    val node = mock[Node]
    val iter = new LazyIterator[Map[String, Any]](10, (_) => Map("x" -> node))
    val src = new FakePipe(iter, "x" -> CTNode)
    val pipe = new NamedPathPipe(src, "p", Seq(ParsedEntity("x")))
    (pipe, iter)
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
    val rel = x.relateTo("r", y, Seq.empty, SemanticDirection.OUTGOING)

    val patternNodes = Map("x" -> x, "y" -> y)
    val patternRels = Map("r" -> Seq(rel))
    val graph = new PatternGraph(patternNodes, patternRels, Seq("x"), Seq.empty)
    val pipe = new MatchPipe(src, Seq(), graph, Set("x", "r", "y"))
    (pipe, iter)
  }

  private def shortestPathPipe = {
    val shortestPath = ShortestPath(pathName = "p", left = SingleNode("start"), right = SingleNode("end"), relTypes = Seq.empty,
      dir = SemanticDirection.OUTGOING, allowZeroLength = true, maxDepth = None, single = true, relIterator = None)
    val n1 = mock[Node]
    when(n1.getRelationships).thenReturn(Iterable.empty[Relationship].asJava)
    val iter = new LazyIterator[Map[String, Any]](10, (_) => Map("start" -> n1, "end" -> n1))
    val src = new FakePipe(iter, "start" -> CTNode, "end" -> CTNode)
    val pipe = new ShortestPathPipe(src, shortestPath)()
    (pipe, iter)
  }

  private def slicePipe = {
    val (iter, src) = emptyFakes
    val pipe = new SlicePipe(src, Some(Literal(2)), Some(Literal(2)))
    (pipe, iter)
  }

  private val sortByX: List[SortItem] = List(SortItem(Identifier("x"), ascending = true))

  private def startPipe = {
    val node = mock[Node]
    val (iter, src) = emptyFakes
    val pipe = new NodeStartPipe(src, "y", new EntityProducer[Node] {
      def producerType: String = "SingleNodeMock"

      def apply(v1: ExecutionContext, v2: QueryState): Iterator[Node] = Iterator(node)

      def arguments: Seq[Argument] = Seq.empty
    })()
    (pipe, iter)
  }

  private def unionPipe = {
    val (iter, src) = emptyFakes
    val (_, src2) = emptyFakes

    val pipe = new UnionPipe(List(src, src2), List("x"))

    (pipe, iter)
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
    (pipe, iter)
  }

  private def emptyFakes: (LazyIterator[Map[String, Any]], FakePipe) = {
    val iter = new LazyIterator[Map[String, Any]](10, (x) => Map("x" -> x))
    val src = new FakePipe(iter, "x" -> CTNumber)
    (iter, src)
  }
}
