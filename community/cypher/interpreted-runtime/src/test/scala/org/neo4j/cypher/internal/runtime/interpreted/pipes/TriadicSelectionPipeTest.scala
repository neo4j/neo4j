/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.mockito.ArgumentMatchers.anyLong
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.core.NodeEntity
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.ElementIdMapper
import org.neo4j.values.virtual.VirtualNodeValue

import scala.collection.Map
import scala.collection.mutable

class TriadicSelectionPipeTest extends CypherFunSuite {

  test("triadic from input with no cycles") {
    val left = createFakePipeWith(Array("a", "b"), 0 -> List(1, 2))
    val right = createFakeArgumentPipeWith(Array("b", "c"), 1 -> List(11, 12), 2 -> List(21, 22))
    val pipe = TriadicSelectionPipe(positivePredicate = false, left, "a", "b", "c", right)()
    val queryState = QueryStateHelper.empty
    val ids = pipe.createResults(queryState)
      .map(ctx => ctx.getByName("c"))
      .collect { case y: VirtualNodeValue => y.id() }
      .toSet
    ids should equal(Set(11, 12, 21, 22))
  }

  test("triadic should close LHS and RHS on close") {
    val left = FakePipe(Seq(
      Map("a" -> nodeWithId(0), "b" -> nodeWithId(1))
    ))
    val right = FakePipe(Seq(
      Map("a" -> nodeWithId(0), "b" -> nodeWithId(1), "c" -> nodeWithId(11)),
      Map("a" -> nodeWithId(0), "b" -> nodeWithId(1), "c" -> nodeWithId(12))
    ))
    val pipe = TriadicSelectionPipe(positivePredicate = false, left, "a", "b", "c", right)()
    val queryState = QueryStateHelper.empty
    println(left.createResults(queryState).toList)
    println(right.createResults(queryState).toList)
    val result = pipe.createResults(queryState)
    // when
    result.hasNext
    result.close()
    // then
    left.wasClosed shouldBe true
    right.wasClosed shouldBe true
  }

  test("triadic from input with cycles and negative predicate") {
    val left = createFakePipeWith(Array("a", "b"), 0 -> List(1, 2))
    val right = createFakeArgumentPipeWith(Array("b", "c"), 1 -> List(11, 12, 2), 2 -> List(21, 22))
    val pipe = TriadicSelectionPipe(positivePredicate = false, left, "a", "b", "c", right)()
    val queryState = QueryStateHelper.empty
    val ids = pipe.createResults(queryState)
      .map(ctx => ctx.getByName("c"))
      .collect { case y: VirtualNodeValue => y.id() }
      .toSet
    ids should equal(Set(11, 12, 21, 22))
  }

  test("triadic from input with cycles and positive predicate") {
    val left = createFakePipeWith(Array("a", "b"), 0 -> List(1, 2))
    val right = createFakeArgumentPipeWith(Array("b", "c"), 1 -> List(11, 12, 2), 2 -> List(21, 22))
    val pipe = TriadicSelectionPipe(positivePredicate = true, left, "a", "b", "c", right)()
    val queryState = QueryStateHelper.empty
    val ids = pipe.createResults(queryState)
      .map(ctx => ctx.getByName("c"))
      .collect { case y: VirtualNodeValue => y.id() }
      .toSet
    ids should equal(Set(2))
  }

  test("triadic from input with two different sources and no cycles") {
    val left = createFakePipeWith(Array("a", "b"), 0 -> List(1, 2), 3 -> List(2, 4))
    val right = createFakeArgumentPipeWith(Array("b", "c"), 1 -> List(11, 12), 2 -> List(21, 22), 4 -> List(41, 42))
    val pipe = TriadicSelectionPipe(positivePredicate = false, left, "a", "b", "c", right)()
    val queryState = QueryStateHelper.empty
    val ids = pipe.createResults(queryState).map(ctx => (ctx.getByName("a"), ctx.getByName("c"))).map {
      case (a: VirtualNodeValue, c: VirtualNodeValue) =>
        (a.id(), c.id())
      case (a, b) => throw new IllegalStateException(s"$a and $b must be nodes")
    }.toSet
    ids should equal(Set((0, 11), (0, 12), (0, 21), (0, 22), (3, 21), (3, 22), (3, 41), (3, 42)))
  }

  test("triadic from input with two different sources and cycles with negative predicate") {
    val left = createFakePipeWith(Array("a", "b"), 0 -> List(1, 2), 3 -> List(2, 4))
    val right = createFakeArgumentPipeWith(
      Array("b", "c"),
      1 -> List(11, 12, 2), // same 'a' so should fail predicate
      2 -> List(21, 22),
      4 -> List(41, 42, 1) // different 'a' so should pass predicate
    )
    val pipe = TriadicSelectionPipe(positivePredicate = false, left, "a", "b", "c", right)()
    val queryState = QueryStateHelper.empty
    val ids = pipe.createResults(queryState).map(ctx => (ctx.getByName("a"), ctx.getByName("c"))).map {
      case (a: VirtualNodeValue, c: VirtualNodeValue) =>
        (a.id(), c.id())
      case (a, b) => throw new IllegalStateException(s"$a and $b must be nodes")
    }.toSet
    ids should equal(Set((0, 11), (0, 12), (0, 21), (0, 22), (3, 1), (3, 21), (3, 22), (3, 41), (3, 42)))
  }

  test("triadic from input with two different sources and cycles with positive predicate") {
    val left = createFakePipeWith(Array("a", "b"), 0 -> List(1, 2), 3 -> List(2, 4))
    val right = createFakeArgumentPipeWith(
      Array("b", "c"),
      1 -> List(11, 12, 2), // same 'a' so should pass predicate
      2 -> List(21, 22),
      4 -> List(41, 42, 1) // different 'a' so should fail predicate
    )
    val pipe = TriadicSelectionPipe(positivePredicate = true, left, "a", "b", "c", right)()
    val queryState = QueryStateHelper.empty
    val ids = pipe.createResults(queryState).map(ctx => (ctx.getByName("a"), ctx.getByName("c"))).map {
      case (a: VirtualNodeValue, c: VirtualNodeValue) =>
        (a.id(), c.id())
      case (a, b) => throw new IllegalStateException(s"$a and $b must be nodes")
    }.toSet
    ids should equal(Set((0, 2)))
  }

  test("triadic from input with repeats") {
    val left = createFakePipeWith(Array("a", "b"), 0 -> List(1, 2, 1), 3 -> List(2, 4, 4))
    val right = createFakeArgumentPipeWith(Array("b", "c"), 1 -> List(11, 12), 2 -> List(21, 22), 4 -> List(41, 42))
    val pipe = TriadicSelectionPipe(positivePredicate = false, left, "a", "b", "c", right)()
    val queryState = QueryStateHelper.empty
    val ids = pipe.createResults(queryState).map(ctx => (ctx.getByName("a"), ctx.getByName("c"))).map {
      case (a: VirtualNodeValue, c: VirtualNodeValue) =>
        (a.id, c.id)
      case (a, b) => throw new IllegalStateException(s"$a and $b must be nodes")
    }.toSet
    ids should equal(Set((0, 11), (0, 12), (0, 21), (0, 22), (3, 21), (3, 22), (3, 41), (3, 42)))
  }

  test("triadic ignores nulls") {
    val left = createFakePipeWith(Array("a", "b"), 0 -> List(1, 2, null), 3 -> List(2, null, 4))
    val right = createFakeArgumentPipeWith(Array("b", "c"), 1 -> List(11, 12), 2 -> List(21, 22), 4 -> List(41, 42))
    val pipe = TriadicSelectionPipe(positivePredicate = false, left, "a", "b", "c", right)()
    val queryState = QueryStateHelper.empty
    val ids = pipe.createResults(queryState).map(ctx => (ctx.getByName("a"), ctx.getByName("c"))).map {
      case (a: VirtualNodeValue, c: VirtualNodeValue) =>
        (a.id, c.id())
      case (a, b) => throw new IllegalStateException(s"$a and $b must be nodes")
    }.toSet
    ids should equal(Set((0, 11), (0, 12), (0, 21), (0, 22), (3, 21), (3, 22), (3, 41), (3, 42)))
  }

  private def nodeWithId(id: Long) = {
    new NodeEntity(mockInternalTransaction(), id)
  }

  private def createFakeDataWith(keys: Array[String], data: (Int, List[Any])*) = {
    data.flatMap {
      case (x, related) =>
        related.map {
          case a: Int => Map(keys(1) -> nodeWithId(a), keys(0) -> nodeWithId(x))
          case null   => Map(keys(1) -> null, keys(0) -> nodeWithId(x))
          case _      => throw new RuntimeException("Invalid fake data")
        }
    }
  }

  private def createFakePipeWith(keys: Array[String], data: (Int, List[Any])*): FakePipe = {
    val in = createFakeDataWith(keys, data: _*)

    new FakePipe(in)
  }

  private def createFakeArgumentPipeWith(keys: Array[String], data: (Int, List[Any])*): Pipe = {
    val in = createFakeDataWith(keys, data: _*)

    new Pipe {
      override def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = state.initialContext match {
        case Some(context: CypherRow) =>
          ClosingIterator(in.flatMap { m =>
            if (ValueUtils.of(m(keys(0))) == context.getByName(keys(0))) {
              val stringToProxy: mutable.Map[String, AnyValue] =
                collection.mutable.Map(m.mapValues(ValueUtils.of).toSeq: _*)
              val outRow = state.newRow(CommunityCypherRowFactory())
              outRow.mergeWith(CypherRow(stringToProxy), null)
              Some(outRow)
            } else None
          }.iterator)
        case _ => ClosingIterator.empty
      }

      override def id: Id = Id.INVALID_ID
    }
  }

  private def mockInternalTransaction(): InternalTransaction = {
    val idMapper = mock[ElementIdMapper]
    when(idMapper.nodeElementId(anyLong())).thenAnswer(new Answer[String] {
      override def answer(invocationOnMock: InvocationOnMock): String = invocationOnMock.getArgument(0).toString
    })
    val mockTransaction = mock[InternalTransaction]
    when(mockTransaction.elementIdMapper()).thenReturn(idMapper)
    mockTransaction
  }
}
