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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.collection.primitive.PrimitiveLongIterable
import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb._
import org.neo4j.kernel.impl.core.NodeProxy

import scala.collection.Map

class TriadicSelectionPipeTest extends CypherFunSuite {
  private implicit val monitor = mock[PipeMonitor]

  test("triadic from input with no cycles") {
    val input = createFakePipeWith(Array("a", "b"), 0 -> List(1, 2))
    val target = createFakeArgumentPipeWith(Array("b", "c"),
      1 -> List(11, 12),
      2 -> List(21, 22)
    )
    val pipe = TriadicSelectionPipe(false, input, "a", "b", "c", target)()
    val queryState = QueryStateHelper.empty
    val ids = pipe.createResults(queryState).map(ctx => ctx("c")).map { case y: Node =>
      y.getId
    }.toSet
    ids should equal(Set(11, 12, 21, 22))
  }

  test("triadic from input with cycles and negative predicate") {
    val input = createFakePipeWith(Array("a", "b"), 0 -> List(1, 2))
    val target = createFakeArgumentPipeWith(Array("b", "c"),
      1 -> List(11, 12, 2),
      2 -> List(21, 22)
    )
    val pipe = TriadicSelectionPipe(false, input, "a", "b", "c", target)()
    val queryState = QueryStateHelper.empty
    val ids = pipe.createResults(queryState).map(ctx => ctx("c")).map { case y: Node =>
      y.getId
    }.toSet
    ids should equal(Set(11, 12, 21, 22))
  }

  test("triadic from input with cycles and positive predicate") {
    val input = createFakePipeWith(Array("a", "b"), 0 -> List(1, 2))
    val target = createFakeArgumentPipeWith(Array("b", "c"),
      1 -> List(11, 12, 2),
      2 -> List(21, 22)
    )
    val pipe = TriadicSelectionPipe(true, input, "a", "b", "c", target)()
    val queryState = QueryStateHelper.empty
    val ids = pipe.createResults(queryState).map(ctx => ctx("c")).map { case y: Node =>
      y.getId
    }.toSet
    ids should equal(Set(2))
  }

  test("triadic from input with two different sources and no cycles") {
    val input = createFakePipeWith(Array("a", "b"), 0 -> List(1, 2), 3 -> List(2, 4))
    val target = createFakeArgumentPipeWith(Array("b", "c"),
      1 -> List(11, 12),
      2 -> List(21, 22),
      4 -> List(41, 42)
    )
    val pipe = TriadicSelectionPipe(false, input, "a", "b", "c", target)()
    val queryState = QueryStateHelper.empty
    //println(pipe.createResults(queryState).toList)
    val ids = pipe.createResults(queryState).map(ctx => (ctx("a"), ctx("c"))).map {
      case (a: Node, c: Node) =>
        (a.getId, c.getId)
    }.toSet
    ids should equal(Set((0, 11), (0, 12), (0, 21), (0, 22), (3, 21), (3, 22), (3, 41), (3, 42)))
  }

  test("triadic from input with two different sources and cycles with negative predicate") {
    val input = createFakePipeWith(Array("a", "b"), 0 -> List(1, 2), 3 -> List(2, 4))
    val target = createFakeArgumentPipeWith(Array("b", "c"),
      1 -> List(11, 12, 2), // same 'a' so should fail predicate
      2 -> List(21, 22),
      4 -> List(41, 42, 1) // different 'a' so should pass predicate
    )
    val pipe = TriadicSelectionPipe(false, input, "a", "b", "c", target)()
    val queryState = QueryStateHelper.empty
    val ids = pipe.createResults(queryState).map(ctx => (ctx("a"), ctx("c"))).map {
      case (a: Node, c: Node) =>
        (a.getId, c.getId)
    }.toSet
    ids should equal(Set((0, 11), (0, 12), (0, 21), (0, 22), (3, 1), (3, 21), (3, 22), (3, 41), (3, 42)))
  }

  test("triadic from input with two different sources and cycles with positive predicate") {
    val input = createFakePipeWith(Array("a", "b"), 0 -> List(1, 2), 3 -> List(2, 4))
    val target = createFakeArgumentPipeWith(Array("b", "c"),
      1 -> List(11, 12, 2), // same 'a' so should pass predicate
      2 -> List(21, 22),
      4 -> List(41, 42, 1) // different 'a' so should fail predicate
    )
    val pipe = TriadicSelectionPipe(true, input, "a", "b", "c", target)()
    val queryState = QueryStateHelper.empty
    val ids = pipe.createResults(queryState).map(ctx => (ctx("a"), ctx("c"))).map {
      case (a: Node, c: Node) =>
        (a.getId, c.getId)
    }.toSet
    ids should equal(Set((0, 2)))
  }

  test("triadic from input with repeats") {
    val input = createFakePipeWith(Array("a", "b"), 0 -> List(1, 2, 1), 3 -> List(2, 4, 4))
    val target = createFakeArgumentPipeWith(Array("b", "c"),
      1 -> List(11, 12),
      2 -> List(21, 22),
      4 -> List(41, 42)
    )
    val pipe = TriadicSelectionPipe(false, input, "a", "b", "c", target)()
    val queryState = QueryStateHelper.empty
    val ids = pipe.createResults(queryState).map(ctx => (ctx("a"), ctx("c"))).map {
      case (a: Node, c: Node) =>
        (a.getId, c.getId)
    }.toSet
    ids should equal(Set((0, 11), (0, 12), (0, 21), (0, 22), (3, 21), (3, 22), (3, 41), (3, 42)))
  }

  test("traidic ignores nulls") {
    val input = createFakePipeWith(Array("a", "b"), 0 -> List(1, 2, null), 3 -> List(2, null, 4))
    val target = createFakeArgumentPipeWith(Array("b", "c"),
      1 -> List(11, 12),
      2 -> List(21, 22),
      4 -> List(41, 42)
    )
    val pipe = TriadicSelectionPipe(false, input, "a", "b", "c", target)()
    val queryState = QueryStateHelper.empty
    val ids = pipe.createResults(queryState).map(ctx => (ctx("a"), ctx("c"))).map {
      case (a: Node, c: Node) =>
        (a.getId, c.getId)
    }.toSet
    ids should equal(Set((0, 11), (0, 12), (0, 21), (0, 22), (3, 21), (3, 22), (3, 41), (3, 42)))
  }

  private def asScalaSet(in: PrimitiveLongIterable): Set[Long] = {
    val builder = Set.newBuilder[Long]
    val iter = in.iterator()
    while (iter.hasNext) {
      builder += iter.next()
    }
    builder.result()
  }

  private def createFakeDataWith(keys: Array[String], data: (Int, List[Any])*) = {
    def nodeWithId(id: Long) = {
      new NodeProxy(null, id)
    }

    data.flatMap {
      case (x, related) =>
        related.map {
          case a: Int => Map(keys(1) -> nodeWithId(a), keys(0) -> nodeWithId(x))
          case null => Map(keys(1) -> null, keys(0) -> nodeWithId(x))
        }
    }
  }

  private def createFakePipeWith(keys: Array[String], data: (Int, List[Any])*): FakePipe = {
    val in = createFakeDataWith(keys, data: _*)

    new FakePipe(in, keys(0) -> CTNode, keys(1) -> CTNode)
  }

  private def createFakeArgumentPipeWith(keys: Array[String], data: (Int, List[Any])*): FakePipe = {
    val in = createFakeDataWith(keys, data: _*)

    new FakePipe(in, keys(0) -> CTNode, keys(1) -> CTNode) {
      override def internalCreateResults(state: QueryState) = state.initialContext match {
        case Some(context: ExecutionContext) =>
          in.flatMap { m =>
            if (m(keys(0)) == context(keys(0))) Some(ExecutionContext(collection.mutable.Map(m.toSeq: _*)))
            else None
          }.iterator
        case _ => Iterator.empty
      }
    }
  }

}
