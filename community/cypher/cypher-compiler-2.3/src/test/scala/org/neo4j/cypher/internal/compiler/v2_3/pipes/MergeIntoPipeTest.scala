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

import org.mockito.Matchers.{any => mockAny, eq => mockEq}
import org.mockito.Mockito
import org.mockito.Mockito.{mock => jmock, _}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Identifier, Property, Expression, Literal, Collection}
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.{KeyToken, TokenType}
import org.neo4j.cypher.internal.compiler.v2_3.mutation.{PropertySetAction, SetAction}
import org.neo4j.cypher.internal.compiler.v2_3.spi.{Operations, QueryContext}
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection.{INCOMING, OUTGOING}
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.DynamicRelationshipType.withName
import org.neo4j.graphdb.{Node, Relationship}

class MergeIntoPipeTest extends CypherFunSuite {

  implicit val monitor = mock[PipeMonitor]
  val node_a = newMockedNode(1)
  val node_b = newMockedNode(2)
  val node_c = newMockedNode(3)
  val node_d = newMockedNode(4)
  val rel_a_A_b = newMockedRelationship(1, node_a, node_b, "A")
  val rel_b_A_a = newMockedRelationship(1, node_b, node_a, "A")
  val rel_a_A_b2 = newMockedRelationship(2, node_a, node_b, "A")
  val rel_b_A_a2 = newMockedRelationship(2, node_a, node_b, "A")
  val rel_a_A_c = newMockedRelationship(3, node_a, node_c, "A")
  val rel_a_B_c = newMockedRelationship(4, node_a, node_c, "B")
  val rel_a_C_d = newMockedRelationship(5, node_a, node_d, "C")
  val rel_a_D_a = newMockedRelationship(6, node_a, node_a, "D")

  setupMockingInQueryContext()

  test("should create a relationship when no-one exists between two nodes") {
    // given
    implicit val query = setupMockingInQueryContext()

    setUpNodesWithoutRelationships(node_a, node_b)
    setUpCreationOfRelationship(node_a, node_b, rel_a_A_b)

    val left = newMockedPipe("a",
      row("a" -> node_a, "b" -> node_b))

    // when
    val result = createPipeAndRun(query, left, OUTGOING, "A", Map.empty)

    // then
    val (single :: Nil) = result
    single.m("a") should equal(node_a)
    single.m("b") should equal(node_b)
    single.m("r") should equal(rel_a_A_b)
  }

  test("should create a relationship when no-one exists between two nodes (reverse direction)") {
    // given
    implicit val query = setupMockingInQueryContext()

    setUpNodesWithoutRelationships(node_a, node_b)
    setUpCreationOfRelationship(node_b, node_a, rel_b_A_a)

    val left = newMockedPipe("a",
      row("a" -> node_a, "b" -> node_b))

    // when
    val result = createPipeAndRun(query, left, INCOMING, "A", Map.empty)

    // then
    val (single :: Nil) = result
    single.m("a") should equal(node_a)
    single.m("b") should equal(node_b)
    single.m("r") should equal(rel_b_A_a)
  }

  test("should find a matching relationship between two nodes - neither dense") {
    // given
    implicit val query = setupMockingInQueryContext()

    markAsNotDense(node_a)
    markAsNotDense(node_b)
    setupRelationshipFromNode(node_a, OUTGOING, rel_a_A_b, rel_a_A_c)
    setupRelationshipFromNode(node_a, OUTGOING, rel_a_B_c)
    val left = newMockedPipe("a",
      row("a" -> node_a, "b" -> node_b))

    // when
    val result = createPipeAndRun(query, left, OUTGOING, "A", Map.empty)

    // then
    val (single :: Nil) = result
    single.m should equal(Map("a" -> node_a, "r" -> rel_a_A_b, "b" -> node_b))
  }

  test("should find a matching relationship between two nodes - node_a is dense") {
    // given
    implicit val query = setupMockingInQueryContext()

    markAsDense(node_a, 2)
    markAsNotDense(node_b)
    setupRelationshipFromNode(node_b, INCOMING, rel_a_A_b)
    val left = newMockedPipe("a",
      row("a" -> node_a, "b" -> node_b))

    // when
    val result = createPipeAndRun(query, left, OUTGOING, "A", Map.empty)

    // then
    val (single :: Nil) = result
    single.m should equal(Map("a" -> node_a, "r" -> rel_a_A_b, "b" -> node_b))
  }

  test("should find a matching relationship between two nodes - node_b is dense") {
    // given
    implicit val query = setupMockingInQueryContext()

    markAsNotDense(node_a)
    markAsDense(node_b, 2)
    setupRelationshipFromNode(node_a, OUTGOING, rel_a_A_b)
    val left = newMockedPipe("a",
      row("a" -> node_a, "b" -> node_b))

    // when
    val result = createPipeAndRun(query, left, OUTGOING, "A", Map.empty)

    // then
    val (single :: Nil) = result
    single.m should equal(Map("a" -> node_a, "r" -> rel_a_A_b, "b" -> node_b))
  }

  test("should find a matching relationship between two nodes - both are dense node_a has lower degree") {
    // given
    implicit val query = setupMockingInQueryContext()

    markAsDense(node_a, 1)
    markAsDense(node_b, 2)
    setupRelationshipFromNode(node_a, OUTGOING, rel_a_A_b)
    val left = newMockedPipe("a",
      row("a" -> node_a, "b" -> node_b))

    // when
    val result = createPipeAndRun(query, left, OUTGOING, "A", Map.empty)

    // then
    val (single :: Nil) = result
    single.m should equal(Map("a" -> node_a, "r" -> rel_a_A_b, "b" -> node_b))
  }

  test("should find a matching relationship between two nodes - both are dense node_b has lower degree") {
    // given
    implicit val query = setupMockingInQueryContext()

    markAsDense(node_a, 2)
    markAsDense(node_b, 1)
    setupRelationshipFromNode(node_b, INCOMING, rel_a_A_b)
    val left = newMockedPipe("a",
      row("a" -> node_a, "b" -> node_b))

    // when
    val result = createPipeAndRun(query, left, OUTGOING, "A", Map.empty)

    // then
    val (single :: Nil) = result
    single.m should equal(Map("a" -> node_a, "r" -> rel_a_A_b, "b" -> node_b))
  }

  test("should not find a matching relationship between two nodes - both are dense node_a has no matching rel degree zero") {
    // given
    implicit val query = setupMockingInQueryContext()

    markAsDense(node_a, 0)
    markAsDense(node_b, 1)
    val left = newMockedPipe("a",
      row("a" -> node_a, "b" -> node_b))
    setUpCreationOfRelationship(node_a, node_b, rel_a_A_b)

    // when
    val result = createPipeAndRun(query, left, OUTGOING, "A", Map.empty)

    // then
    val (single :: Nil) = result
    single.m should equal(Map("a" -> node_a, "r" -> rel_a_A_b, "b" -> node_b))
  }

  test("should not find a matching relationship between two nodes - both are dense node_a has no matching rel degree zero (reverse direction)") {
    // given
    implicit val query = setupMockingInQueryContext()

    markAsDense(node_a, 0)
    markAsDense(node_b, 1)
    val left = newMockedPipe("a",
      row("a" -> node_a, "b" -> node_b))
    setUpCreationOfRelationship(node_b, node_a, rel_b_A_a)

    // when
    val result = createPipeAndRun(query, left, INCOMING, "A", Map.empty)

    // then
    val (single :: Nil) = result
    single.m should equal(Map("a" -> node_a, "r" -> rel_b_A_a, "b" -> node_b))
  }

  test("should find a matching relationship between two nodes with no matching properties") {
    // given
    implicit val query = setupMockingInQueryContext()
    markAsNotDense(node_a)
    markAsNotDense(node_b)

    // when
    setupRelationshipFromNode(node_a, INCOMING, rel_a_A_b)
    when(query.relationshipOps.getProperty(1, "key".hashCode())).thenReturn(null, Seq.empty: _*)
    when(query.relationshipOps.getProperty(1, "foo".hashCode())).thenReturn(null, Seq.empty: _*)
    setUpCreationOfRelationship(node_b, node_a, rel_b_A_a2)

    val left = newMockedPipe("a", row("a" -> node_a, "b" -> node_b))
    val result = createPipeAndRun(query, left, INCOMING, "A", Map("key" -> Literal(42),
      "foo" -> Literal("bar")))

    // then
    val (single :: Nil) = result
    single.m should equal(Map("a" -> node_a, "r" -> rel_b_A_a2, "b" -> node_b))
    verify(query.relationshipOps).setProperty(2, "key".hashCode(), 42)
    verify(query.relationshipOps).setProperty(2, "foo".hashCode(), "bar")
  }

  test("should find a matching relationship between two nodes with no matching properties (reverse direction)") {
    // given
    implicit val query = setupMockingInQueryContext()
    markAsNotDense(node_a)
    markAsNotDense(node_b)

    // when
    setupRelationshipFromNode(node_a, OUTGOING, rel_a_A_b)
    when(query.relationshipOps.getProperty(1, "key".hashCode())).thenReturn(null, Seq.empty: _*)
    when(query.relationshipOps.getProperty(1, "foo".hashCode())).thenReturn(null, Seq.empty: _*)
    setUpCreationOfRelationship(node_a, node_b, rel_a_A_b2)

    val left = newMockedPipe("a", row("a" -> node_a, "b" -> node_b))
    val result = createPipeAndRun(query, left, OUTGOING, "A", Map("key" -> Literal(42),
      "foo" -> Literal("bar")))

    // then
    val (single :: Nil) = result
    single.m should equal(Map("a" -> node_a, "r" -> rel_a_A_b2, "b" -> node_b))
    verify(query.relationshipOps).setProperty(2, "key".hashCode(), 42)
    verify(query.relationshipOps).setProperty(2, "foo".hashCode(), "bar")
  }

  test("should find a matching relationship between two nodes with matching properties") {
    // given
    implicit val query = setupMockingInQueryContext()
    markAsNotDense(node_a)
    markAsNotDense(node_b)

    // when
    setupRelationshipFromNode(node_a, INCOMING, rel_a_A_b)
    when(query.relationshipOps.getProperty(1, "key".hashCode())).thenReturn(42, Seq.empty: _*)
    when(query.relationshipOps.getProperty(1, "foo".hashCode())).thenReturn("bar", Seq.empty: _*)

    val left = newMockedPipe("a", row("a" -> node_a, "b" -> node_b))
    val result = createPipeAndRun(query, left, INCOMING, "A", Map("key" -> Literal(42),
      "foo" -> Literal("bar")))

    // then
    val (single :: Nil) = result
    single.m should equal(Map("a" -> node_a, "r" -> rel_a_A_b, "b" -> node_b))
    verify(query.relationshipOps).getProperty(1, "key".hashCode())
    verify(query.relationshipOps).getProperty(1, "foo".hashCode())
    verifyNoMoreInteractions(query.relationshipOps)
  }

  test("should find a matching relationship between two nodes with matching array properties") {
    // given
    implicit val query = setupMockingInQueryContext()
    markAsNotDense(node_a)
    markAsNotDense(node_b)

    // when
    setupRelationshipFromNode(node_a, INCOMING, rel_a_A_b)
    when(query.relationshipOps.getProperty(1, "key".hashCode())).thenReturn(Array(42, 43), Seq.empty: _*)
    when(query.relationshipOps.getProperty(1, "foo".hashCode())).thenReturn(Array("foo", "bar"), Seq.empty: _*)

    val left = newMockedPipe("a", row("a" -> node_a, "b" -> node_b))
    val result = createPipeAndRun(query, left, INCOMING, "A", Map("key" -> Collection(Literal(42), Literal(43)),
      "foo" -> Collection(Literal("foo"), Literal("bar"))))

    // then
    val (single :: Nil) = result
    single.m should equal(Map("a" -> node_a, "r" -> rel_a_A_b, "b" -> node_b))
    verify(query.relationshipOps).getProperty(1, "key".hashCode())
    verify(query.relationshipOps).getProperty(1, "foo".hashCode())
    verifyNoMoreInteractions(query.relationshipOps)
  }

  test("should set properties on create") {
    // given
    implicit val query = setupMockingInQueryContext()
    markAsNotDense(node_a)
    markAsNotDense(node_b)
    setUpNodesWithoutRelationships(node_a, node_b)
    setUpCreationOfRelationship(node_b, node_a, rel_b_A_a)

    // when
    val left = newMockedPipe("a", row("a" -> node_a, "b" -> node_b))
    val propertySetAction = Seq(
      PropertySetAction(Property(Identifier("r"), resolve("key")), Literal(42)),
      PropertySetAction(Property(Identifier("r"), resolve("foo")), Literal("bar")))
    val result = createPipeAndRun(query, left, INCOMING, "A", relProperties = Map.empty,
      onCreateProperties = propertySetAction)

    // then
    val (single :: Nil) = result
    single.m should equal(Map("a" -> node_a, "r" -> rel_b_A_a, "b" -> node_b))
    verify(query.relationshipOps).setProperty(rel_b_A_a.getId, "key".hashCode(), 42)
    verify(query.relationshipOps).setProperty(rel_b_A_a.getId, "foo".hashCode(), "bar")
  }

  test("should set properties on create (reverse direction)") {
    // given
    implicit val query = setupMockingInQueryContext()
    markAsNotDense(node_a)
    markAsNotDense(node_b)
    setUpNodesWithoutRelationships(node_a, node_b)
    setUpCreationOfRelationship(node_a, node_b, rel_a_A_b)

    // when
    val left = newMockedPipe("a", row("a" -> node_a, "b" -> node_b))
    val propertySetAction = Seq(
      PropertySetAction(Property(Identifier("r"), resolve("key")), Literal(42)),
      PropertySetAction(Property(Identifier("r"), resolve("foo")), Literal("bar")))
    val result = createPipeAndRun(query, left, OUTGOING, "A", relProperties = Map.empty,
      onCreateProperties = propertySetAction)

    // then
    val (single :: Nil) = result
    single.m should equal(Map("a" -> node_a, "r" -> rel_a_A_b, "b" -> node_b))
    verify(query.relationshipOps).setProperty(rel_a_A_b.getId, "key".hashCode(), 42)
    verify(query.relationshipOps).setProperty(rel_a_A_b.getId, "foo".hashCode(), "bar")
  }

  test("should set properties on match") {
    // given
    implicit val query = setupMockingInQueryContext()
    markAsNotDense(node_a)
    markAsNotDense(node_b)
    setupRelationshipFromNode(node_a, INCOMING, rel_a_A_b)

    // when
    val left = newMockedPipe("a", row("a" -> node_a, "b" -> node_b))
    val propertySetAction = Seq(
      PropertySetAction(Property(Identifier("r"), resolve("key")), Literal(42)),
      PropertySetAction(Property(Identifier("r"), resolve("foo")), Literal("bar")))
    val result = createPipeAndRun(query, left, INCOMING, "A", relProperties = Map.empty,
      onCreateProperties = Seq.empty, onMatchProperties = propertySetAction)

    // then
    val (single :: Nil) = result
    single.m should equal(Map("a" -> node_a, "r" -> rel_a_A_b, "b" -> node_b))
    verify(query.relationshipOps).setProperty(rel_a_A_b.getId, "key".hashCode(), 42)
    verify(query.relationshipOps).setProperty(rel_a_A_b.getId, "foo".hashCode(), "bar")
  }

  private def createPipeAndRun(query: QueryContext, left: Pipe, dir: SemanticDirection = OUTGOING, relType: String,
                               relProperties: Map[String, Expression], onCreateProperties: Seq[SetAction] = Seq.empty, onMatchProperties: Seq[SetAction] = Seq.empty): List[ExecutionContext] = {
    val f: PartialFunction[(String, Expression), (KeyToken, Expression)] = {
      case (k, v) => resolve(k) -> v
    }
    MergeIntoPipe(left, "a", "r", "b", dir, relType, relProperties.map(f), onCreateProperties, onMatchProperties)().createResults(QueryStateHelper.emptyWith(query)).toList
  }

  private def resolve(key: String): KeyToken = KeyToken.Resolved(key, key.hashCode, TokenType.PropertyKey)


  private def setupRelationshipFromNode(startNode: Node, dir: SemanticDirection, rels: Relationship*)(implicit query: QueryContext) {
    val typeId = rels.head.getType.name().hashCode
    when(query.getRelationshipsForIds(startNode, dir, Some(Seq(typeId)))).thenReturn(rels.toIterator)
  }

  private def markAsNotDense(node: Node)(implicit query: QueryContext) {
    when(query.nodeIsDense(mockEq(node.getId))).thenReturn(false)
  }

  private def markAsDense(node: Node, degree: Int)(implicit query: QueryContext) {
    when(query.nodeIsDense(mockEq(node.getId))).thenReturn(true)
    when(query.nodeGetDegree(mockEq(node.getId), mockAny(), mockAny())).thenReturn(degree)
  }

  private def row(values: (String, Any)*) = ExecutionContext.from(values: _*)

  private def setUpNodesWithoutRelationships(nodes: Node*)(implicit query: QueryContext) {
    nodes.foreach { node =>
      when(query.getRelationshipsForIds(mockEq(node), mockAny(), mockAny())).thenReturn(Iterator.empty)
    }
  }

  private def setUpCreationOfRelationship(from: Node, to: Node, rel: Relationship)(implicit query: QueryContext) {
    when(query.createRelationship(from.getId, to.getId, rel.getType.name().hashCode)).thenReturn(rel)
  }

  private def newMockedNode(id: Int) = {
    val node = mock[Node]
    when(node.toString).thenReturn(id.toString)
    when(node.getId).thenReturn(id)
    node
  }

  private def setupMockingInQueryContext(): QueryContext = {
    val query = jmock(classOf[QueryContext], Mockito.RETURNS_SMART_NULLS)
    when(query.getOrCreateRelTypeId(mockAny(classOf[String]))).thenAnswer(new Answer[Int] {
      override def answer(invocationOnMock: InvocationOnMock): Int = {
        val arguments = invocationOnMock.getArguments
        val head = arguments.head
        head.asInstanceOf[String].hashCode
      }
    })

    val mockOperationsRelationship = mock[Operations[Relationship]]
    when(query.relationshipOps).thenReturn(mockOperationsRelationship)

    query
  }

  private def newMockedRelationship(id: Int, startNode: Node, endNode: Node, typ: String): Relationship = {
    val relationship = mock[Relationship]
    when(relationship.getId).thenReturn(id)
    when(relationship.getStartNode).thenReturn(startNode)
    when(relationship.getEndNode).thenReturn(endNode)
    when(relationship.getOtherNode(startNode)).thenReturn(endNode)
    when(relationship.getOtherNode(endNode)).thenReturn(startNode)
    when(relationship.getType).thenReturn(withName(typ))
    relationship
  }

  private def newMockedPipe(node: String, rows: ExecutionContext*): Pipe = {
    newMockedPipe(Map(node -> CTNode), rows: _*)
  }

  private def newMockedPipe(symbols: Map[String, CypherType], rows: ExecutionContext*): Pipe = {
    val pipe = mock[Pipe]

    when(pipe.sources).thenReturn(Seq.empty)
    when(pipe.symbols).thenReturn(SymbolTable(symbols))
    when(pipe.createResults(mockAny())).thenAnswer(new Answer[Iterator[ExecutionContext]] {
      def answer(invocation: InvocationOnMock) = rows.iterator
    })

    pipe
  }
}
