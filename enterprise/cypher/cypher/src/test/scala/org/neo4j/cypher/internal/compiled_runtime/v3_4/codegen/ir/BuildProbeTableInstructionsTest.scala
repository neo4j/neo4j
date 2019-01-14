/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.compiled_runtime.v3_4.codegen.ir

import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.BiConsumer

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.collection.primitive.PrimitiveLongIterator
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions.{CodeGenType, NodeProjection}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.{CodeGenContext, JoinTableMethod, Variable}
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.TransactionalContextWrapper
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Node
import org.neo4j.internal.kernel.api._
import org.neo4j.internal.kernel.api.helpers.StubNodeCursor
import org.neo4j.kernel.impl.core.{EmbeddedProxySPI, NodeProxy}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable._
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP
import org.neo4j.values.virtual.{ListValue, MapValue, NodeValue}

import scala.collection.{JavaConverters, mutable}

class BuildProbeTableInstructionsTest extends CypherFunSuite with CodeGenSugar {

  private val tableVarName = "probeTable"
  private val buildTableMethodName = "buildProbeTable"
  private val resultRowKey = "resultKey"

  private val entityAccessor = mock[EmbeddedProxySPI]
  private val queryContext = mock[QueryContext]
  private val transactionalContext = mock[TransactionalContextWrapper]
  private val dataRead = mock[Read]
  private val cursors = mock[CursorFactory]
  private def nodeCursor = {
    val cursor = new StubNodeCursor
    val it = allNodeIdsIterator()
    while(it.hasNext) cursor.withNode(it.next())
    cursor
  }

  private val allNodeIds = mutable.ArrayBuffer[Long]()

  // used by instructions that generate probe tables
  private implicit val codeGenContext = new CodeGenContext(SemanticTable(), Map.empty)
  when(queryContext.transactionalContext).thenReturn(transactionalContext)
  when(cursors.allocateNodeCursor()).thenAnswer(new Answer[NodeCursor] {
    override def answer(invocation: InvocationOnMock): NodeCursor = nodeCursor
  })
  when(transactionalContext.dataRead).thenReturn(dataRead)
  when(transactionalContext.cursors).thenReturn(cursors)
  when(queryContext.entityAccessor).thenReturn(entityAccessor)


  override protected def beforeEach() = allNodeIds.clear()

  test("should generate correct code for simple counting probe table") {
    // Given
    setUpNodeMocks(1, 2, 42)
    val nodeVar = "node"
    val nodes = Set(Variable(nodeVar, CodeGenType.primitiveNode))

    val buildInstruction = BuildCountingProbeTable(id = "countingTable",
                                                   name = tableVarName,
                                                   nodes = nodes)

    // When
    val results = runTest(buildInstruction, nodes)

    // Then
    results should have size 3
    val (a :: b :: c :: Nil) = results
    checkNodeResult(1, a)
    checkNodeResult(2, b)
    checkNodeResult(42, c)
  }

  test("should generate correct code for a counting probe table on multiple keys") {
    // Given
    setUpNodeMocks(1, 2)
    val nodes = Set(Variable("node1",CodeGenType.primitiveNode), Variable("node2", CodeGenType.primitiveNode))
    val buildInstruction = BuildCountingProbeTable(id = "countingTable",
                                                   name = tableVarName,
                                                   nodes = nodes)

    // When
    val results = runTest(buildInstruction, nodes)

    // Then
    results should have size 4
    val (a :: b :: c :: d :: Nil) = results
    checkNodeResult(1, a)
    checkNodeResult(1, b)
    checkNodeResult(2, c)
    checkNodeResult(2, d)
  }

  test("should generate correct code for simple recording probe table") {
    // Given
    setUpNodeMocks(42, 4242)
    val nodeVar = "node"
    val nodes = Set(Variable(nodeVar,CodeGenType.primitiveNode))
    val buildInstruction = BuildRecordingProbeTable(id = "recordingTable",
                                                    name = tableVarName,
                                                    nodes = nodes,
                                                    valueSymbols = Map(nodeVar -> Variable(nodeVar, CodeGenType.primitiveNode)))

    // When
    val results = runTest(buildInstruction, nodes)

    // Then
    results should have size 2
    val (a :: b :: Nil) = results
    checkNodeResult(42, a)
    checkNodeResult(4242, b)
  }

  test("should generate correct code for recording probe table on multiple keys") {
    // Given
    setUpNodeMocks(42, 4242)
    val joinNodes = Set(Variable("node1", CodeGenType.primitiveNode), Variable("node2", CodeGenType.primitiveNode))
    val buildInstruction = BuildRecordingProbeTable(id = "recordingTable",
      name = tableVarName,
      nodes = joinNodes,
      valueSymbols = Map("node1" -> Variable("node1", CodeGenType.primitiveNode)))

    // When
    val results = runTest(buildInstruction, joinNodes)

    // Then
    results should have size 4
    val (a :: b :: c:: d :: Nil) = results
    checkNodeResult(42, a)
    checkNodeResult(42, b)
    checkNodeResult(4242, c)
    checkNodeResult(4242, d)
  }

  private def setUpNodeMocks(ids: Long*): Unit = {
    ids.foreach { id =>
      val nodeMock = mock[NodeProxy]
      when(nodeMock.getId).thenReturn(id)
      when(entityAccessor.newNodeProxy(id)).thenReturn(nodeMock)
      allNodeIds += id
    }
  }

  when(queryContext.asObject(any())).thenAnswer(new Answer[AnyRef] {
    override def answer(invocationOnMock: InvocationOnMock): AnyRef =
      toObjectConverter(invocationOnMock.getArguments()(0))
  })

  import JavaConverters._
  private def toObjectConverter(a: AnyRef): AnyRef = a match {
    case Values.NO_VALUE => null
    case n: NodeValue =>
      val proxy = mock[NodeProxy]
      when(proxy.getId).thenReturn(n.id())
      proxy

    case s: TextValue => s.stringValue()
    case b: BooleanValue => Boolean.box(b.booleanValue())
    case f: FloatingPointValue => Double.box(f.doubleValue())
    case i: IntegralValue => Long.box(i.longValue())
    case l: ListValue =>
      val list = new util.ArrayList[AnyRef]
      l.iterator().asScala.foreach(a => list.add(toObjectConverter(a)))
      list
    case m: MapValue =>
      val map = new util.HashMap[String, AnyRef]()
      m.foreach(new BiConsumer[String, AnyValue] {
        override def accept(t: String, u: AnyValue): Unit = map.put(t, toObjectConverter(u))
      })
      map
  }


  private def checkNodeResult(id: Long, res: Map[String, Object]): Unit = {
    res.size shouldEqual 1
    val node = res(resultRowKey).asInstanceOf[Node]
    node.getId shouldEqual id
  }

  private def allNodeIdsIterator() = new PrimitiveLongIterator {
    val inner = allNodeIds.iterator

    override def hasNext = inner.hasNext

    override def next() = inner.next()
  }

  private def runTest(buildInstruction: BuildProbeTable, nodes: Set[Variable]): List[Map[String, Object]] = {
    val instructions = buildProbeTableWithTwoAllNodeScans(buildInstruction, nodes)
    val ids: Map[String, Id] = instructions.flatMap(_.allOperatorIds.map(id => id -> Id.INVALID_ID)).toMap
    evaluate(instructions, queryContext, Seq(resultRowKey), EMPTY_MAP, ids)
  }

  private def buildProbeTableWithTwoAllNodeScans(buildInstruction: BuildProbeTable, nodes: Set[Variable]): Seq[Instruction] = {
    val counter = new AtomicInteger(0)
    val buildWhileLoop = nodes.foldRight[Instruction](buildInstruction){
      case (variable, instruction) => WhileLoop(variable, ScanAllNodes("scanOp" + counter.incrementAndGet()), instruction)
    }

    val buildProbeTableMethod = MethodInvocation(operatorId = Set.empty,
                                                 symbol = JoinTableMethod(tableVarName, buildInstruction.tableType),
                                                 methodName = buildTableMethodName,
                                                 statements = Seq(buildWhileLoop))

    val probeVars = nodes.map(n => n.copy(name = s"probe" + n.name))
    //just put one node in the actual result
    val resultVar = probeVars.head

    val acceptVisitor = AcceptVisitor("visitorOp", Map(resultRowKey -> NodeProjection(resultVar)))

    val probeTheTable = GetMatchesFromProbeTable(keys = probeVars,
                                                 code = buildInstruction.joinData,
                                                 action = acceptVisitor)

    val probeTheTableWhileLoop = probeVars.foldRight[Instruction](probeTheTable){
      case (variable, instruction) => WhileLoop(variable, ScanAllNodes("scanOp" + counter.incrementAndGet()), instruction)
    }
    Seq(buildProbeTableMethod, probeTheTableWhileLoop)
  }

}
