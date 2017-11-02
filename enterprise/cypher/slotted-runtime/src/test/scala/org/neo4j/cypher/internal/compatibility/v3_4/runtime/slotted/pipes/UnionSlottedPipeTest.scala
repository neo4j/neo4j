/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.slotted.pipes

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.slotted.PrimitiveExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.slotted.SlottedPipeBuilder.computeUnionMapping
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{LongSlot, PipelineInformation}
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.{Operations, QueryContext}
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.graphdb.{Node, Relationship}
import org.neo4j.values.storable.Values.{longValue, stringArray, stringValue}
import org.neo4j.values.virtual.VirtualValues
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP

import scala.collection.immutable

class UnionSlottedPipeTest extends CypherFunSuite {

  private def union(lhsInfo: PipelineInformation,
                    rhsInfo: PipelineInformation,
                    out: PipelineInformation, lhsData: Traversable[Map[String, Any]],
                                              rhsData: Traversable[Map[String, Any]]) = {
    val lhs = FakeSlottedPipe(lhsData.toIterator, lhsInfo)
    val rhs = FakeSlottedPipe(rhsData.toIterator, rhsInfo)
    val union = UnionSlottedPipe(lhs, rhs, computeUnionMapping(lhsInfo, out), computeUnionMapping(rhsInfo, out) )()
    val context = mock[QueryContext]
    val nodeOps = mock[Operations[Node]]
    when(nodeOps.getById(any())).thenAnswer(new Answer[Node] {
      override def answer(invocation: InvocationOnMock): Node =
        newMockedNode(invocation.getArgument[Long](0))
    })
    when(context.nodeOps).thenReturn(nodeOps)
    val relOps = mock[Operations[Relationship]]
    when(relOps.getById(any())).thenAnswer(new Answer[Relationship] {
      override def answer(invocation: InvocationOnMock): Relationship =
        newMockedRelationship(invocation.getArgument[Long](0))
    })
    when(context.relationshipOps).thenReturn(relOps)
    val res = union.createResults(QueryStateHelper.emptyWith(query = context)).toList.map {
      case e: PrimitiveExecutionContext =>
        e.pipeline.mapSlot {
          case (k, s: LongSlot) =>
            val value = if (s.typ == CTNode) nodeValue(e.getLongAt(s.offset))
            else if (s.typ == CTRelationship) edgeValue(e.getLongAt(s.offset))
            else throw new AssertionError("This is clearly not right")
            k -> value
          case (k, s) => k -> e.getRefAt(s.offset)
        }.toMap
    }

    res
  }

  test("should handle references") {
    // Given
    val lhsInfo = PipelineInformation.empty.newReference("x", nullable = false, CTAny)
    val rhsInfo = PipelineInformation.empty.newReference("x", nullable = false, CTAny)
    val out = PipelineInformation.empty.newReference("x", nullable = false, CTAny)
    val lhsData = List(Map("x" -> 42))
    val rhsData = List(Map("x" -> 43))

    // When
    val result = union(lhsInfo, rhsInfo, out, lhsData, rhsData)

    // Then
    result should equal(
      List(Map("x" -> longValue(42)), Map("x" -> longValue(43))))
  }

  test("should handle mixed longslot and refslot") {
    // Given
    val lhsInfo = PipelineInformation.empty.newLong("x", nullable = false, CTNode)
    val rhsInfo = PipelineInformation.empty.newReference("x", nullable = false, CTAny)
    val out = PipelineInformation.empty.newReference("x", nullable = false, CTAny)
    val lhsData = List(Map("x" -> 42))
    val rhsData = List(Map("x" -> 43))

    // When
    val result = union(lhsInfo, rhsInfo, out, lhsData, rhsData)

    // Then
    result should equal(
      List(Map("x" -> nodeValue(42)), Map("x" -> longValue(43))))
  }

  test("should handle two node longslots") {
    // Given
    val lhsInfo = PipelineInformation.empty.newLong("x", nullable = false, CTNode)
    val rhsInfo = PipelineInformation.empty.newLong("x", nullable = false, CTNode)
    val out = PipelineInformation.empty.newLong("x", nullable = false, CTNode)
    val lhsData = List(Map("x" -> 42))
    val rhsData = List(Map("x" -> 43))

    // When
    val result = union(lhsInfo, rhsInfo, out, lhsData, rhsData)
    // Then
    result should equal(
      List(Map("x" -> nodeValue(42)), Map("x" -> nodeValue(43))))
  }

  test("should handle two relationship longslots") {
    // Given
    val lhsInfo = PipelineInformation.empty.newLong("x", nullable = false, CTRelationship)
    val rhsInfo = PipelineInformation.empty.newLong("x", nullable = false, CTRelationship)
    val out = PipelineInformation.empty.newLong("x", nullable = false, CTRelationship)
    val lhsData = List(Map("x" -> 42))
    val rhsData = List(Map("x" -> 43))

    // When
    val result = union(lhsInfo, rhsInfo, out, lhsData, rhsData)

    // Then
    result should equal(
      List(Map("x" -> edgeValue(42)), Map("x" -> edgeValue(43))))
  }

  test("should handle one long slot and one relationship slot") {
    // Given
    val lhsInfo = PipelineInformation.empty.newLong("x", nullable = false, CTNode)
    val rhsInfo = PipelineInformation.empty.newLong("x", nullable = false, CTRelationship)
    val out = PipelineInformation.empty.newReference("x", nullable = false, CTAny)
    val lhsData = List(Map("x" -> 42))
    val rhsData = List(Map("x" -> 43))

    // When
    val result = union(lhsInfo, rhsInfo, out, lhsData, rhsData)

    // Then
    result should equal(
      List(Map("x" -> nodeValue(42)), Map("x" -> edgeValue(43))))
  }

  test("should handle multiple columns") {
    // Given
    val lhsInfo = PipelineInformation.empty
      .newLong("x", nullable = false, CTNode)
      .newLong("y", nullable = false, CTRelationship)
      .newReference("z", nullable = false, CTAny)
    val rhsInfo = PipelineInformation.empty
      .newLong("x", nullable = false, CTRelationship)
      .newLong("y", nullable = false, CTRelationship)
      .newLong("z", nullable = false, CTRelationship)
    val out = PipelineInformation.empty
      .newReference("x", nullable = false, CTAny)
      .newLong("y", nullable = false, CTRelationship)
      .newReference("z", nullable = false, CTAny)
    val lhsData: immutable.Seq[Map[String, Any]] = List(Map("x" -> 42, "y" -> 1337, "z" -> "FOO"))
    val rhsData: immutable.Seq[Map[String, Int]] = List(Map("x" -> 43, "y" -> 44, "z" -> 45))

    // When
    val result = union(lhsInfo, rhsInfo, out, lhsData, rhsData)

    // Then
    result should equal(
      List(
        Map("x" -> nodeValue(42), "y" -> edgeValue(1337),"z" -> stringValue("FOO")),
        Map("x" -> edgeValue(43), "y" -> edgeValue(44),"z" -> edgeValue(45))
      ))
  }

  test("should handle multiple columns in permutated order") {
    // Given
    val lhsInfo = PipelineInformation.empty
      .newLong("x", nullable = false, CTNode)
      .newLong("y", nullable = false, CTRelationship)
      .newReference("z", nullable = false, CTAny)
    val rhsInfo = PipelineInformation.empty
      .newLong("y", nullable = false, CTRelationship)
      .newLong("z", nullable = false, CTRelationship)
      .newLong("x", nullable = false, CTRelationship)
    val out = PipelineInformation.empty
      .newReference("x", nullable = false, CTAny)
      .newLong("y", nullable = false, CTRelationship)
      .newReference("z", nullable = false, CTAny)
    val lhsData: immutable.Seq[Map[String, Any]] = List(Map("x" -> 42, "y" -> 1337, "z" -> "FOO"))
    val rhsData: immutable.Seq[Map[String, Int]] = List(Map("x" -> 43, "y" -> 44, "z" -> 45))

    // When
    val result = union(lhsInfo, rhsInfo, out, lhsData, rhsData)

    // Then
    result should equal(
      List(
        Map("x" -> nodeValue(42), "y" -> edgeValue(1337),"z" -> stringValue("FOO")),
        Map("x" -> edgeValue(43), "y" -> edgeValue(44),"z" -> edgeValue(45))
      ))
  }

  private def newMockedNode(id: Long) = {
    val node = mock[Node]
    when(node.getId).thenReturn(id)
    node
  }

  private def newMockedRelationship(id: Long) = {
    val rel = mock[Relationship]
    when(rel.getId).thenReturn(id)
    rel
  }

  private def nodeValue(id: Long) = VirtualValues.nodeValue(id, stringArray("L"), EMPTY_MAP)
  private def edgeValue(id: Long) = VirtualValues.edgeValue(id, nodeValue(id - 1), nodeValue(id + 1), stringValue("L"), EMPTY_MAP)
}
