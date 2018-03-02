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
package org.neo4j.cypher.internal.compiler.v3_4

import org.mockito.Mockito._
import org.neo4j.cypher.GraphDatabaseFunSuite
import org.neo4j.cypher.internal.runtime.interpreted.commands.{ShortestPath, SingleNode, SortItem}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{ShortestPathExpression, Variable}
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.True
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateTestSupport
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection
import org.neo4j.graphdb._

import scala.collection.JavaConverters._

/*
This test fixture tries to assert that Pipe declaring that they are lazy
in fact are lazy. Every Pipe should be represented here
 */
class PipeLazynessTest extends GraphDatabaseFunSuite with QueryStateTestSupport {

  test("test") {
    distinctPipe.!!()
    filterPipe.!!()
    shortestPathPipe.!!()
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

  private def distinctPipe = {
    val iter = new LazyIterator[Map[String, Any]](10, (n) => Map("x" -> n))
    val src = new FakePipe(iter, "x" -> CTNumber)
    val pipe = new DistinctPipe(src, Map("x" -> Variable("x")))()
    (pipe, iter)
  }

  private def filterPipe = {
    val (iter, src) = emptyFakes
    val pipe = new FilterPipe(src, True())()
    (pipe, iter)
  }

  private def shortestPathPipe = {
    val shortestPath = ShortestPath(pathName = "p", left = SingleNode("start"), right = SingleNode("end"), relTypes = Seq.empty,
      dir = SemanticDirection.OUTGOING, allowZeroLength = true, maxDepth = None, single = true, relIterator = None)
    val n1 = mock[Node]
    when(n1.getRelationships).thenReturn(Iterable.empty[Relationship].asJava)
    val iter = new LazyIterator[Map[String, Any]](10, (_) => Map("start" -> n1, "end" -> n1))
    val src = new FakePipe(iter, "start" -> CTNode, "end" -> CTNode)
    val pipe = new ShortestPathPipe(src, ShortestPathExpression(shortestPath))()
    (pipe, iter)
  }

  private val sortByX: List[SortItem] = List(SortItem(Variable("x"), ascending = true))

  private def unionPipe = {
    val (iter, src) = emptyFakes
    val (_, src2) = emptyFakes

    val pipe = UnionPipe(src, src2)()

    (pipe, iter)
  }

  private def emptyFakes: (LazyIterator[Map[String, Any]], FakePipe) = {
    val iter = new LazyIterator[Map[String, Any]](10, (x) => Map("x" -> x))
    val src = new FakePipe(iter, "x" -> CTNumber)
    (iter, src)
  }
}
